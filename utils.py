import re
import os
import httpx
import isodate
import asyncio
from pathlib import Path
from rapidfuzz import fuzz
from dotenv import load_dotenv
from datetime import date, datetime
from urllib.parse import urlparse, parse_qs

load_dotenv()

API_KEY = os.getenv("GOOGLE_API_KEY")
YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3"


def file_update_template(file_path: Path, yt_info: list[str]):
    file_text = f"\nFILE: {file_path.as_posix()}\n"
    for yt_data in yt_info:
        name, url = yt_data["name"], yt_data["url"]
        file_text += f"Name: {name}\n[OLD] {url}\n[NEW] https://www.youtube.com/watch?v=xyz9876\n\n"
    return file_text


async def get_file_yt_info(file_path: Path):
    yt_info = []
    yt_url_pattern = re.compile(
        r"\[([^\]]+)\]\((https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtube\.com/playlist\?list=|youtu\.be/)[^)]+)\)"
    )

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            match = yt_url_pattern.search(line)
            if match:
                name = match.group(1).strip()
                url = match.group(2).strip()
                if "playlist?" in url:
                    type = "playlists"
                else:
                    type = "videos"
                yt_info.append({"name": name, "type": type, "url": url})

    return yt_info


def create_log_file(markdown_files: list[Path], is_dry_run=False):
    current_date = date.today()
    log_file_path = "dry_run" if is_dry_run else "update_log"
    file_mode = "DRY RUN" if is_dry_run else "ACTUAL UPDATE"

    with open(f"{log_file_path}_{current_date}.log", "w") as log_file:
        log_file.writelines(
            [
                "=== YouTube Markdown Link Refresher Log ===\n",
                f"Date: {current_date}\n",
                f"Mode: {file_mode}\n",
            ]
        )

    # log file update template for each md
    for markdown_file in markdown_files:
        yt_info = get_file_yt_info(markdown_file)
        file_text = file_update_template(markdown_file, yt_info)
        with open(f"{log_file_path}_{current_date}.log", "a") as log_file:
            log_file.write(file_text)


async def get_markdown_files(folder_path: Path):
    """
    Recursively finds all .md (Markdown) files in the given folder path.

    Args:
        folder_path (Path): The root directory to search from.

    Returns:
        List[Path]: A list of Path objects representing all markdown files found.
    """
    return list(folder_path.rglob("*.md"))


async def is_yt_url_outdated(yt_id: str, type="videos"):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/{type}?key={API_KEY}&id={yt_id}&part=snippet"
        )

    current_year = datetime.now().year
    data = response.json()
    published_year = int(data["items"][0]["snippet"]["publishedAt"][:4])

    return current_year - 3 >= published_year


async def get_new_yt_data(video_data):
    yt_id = video_data["id"]
    status = "Updated successfully!"
    yt_title = video_data["snippet"]["title"]
    yt_url = f"https://www.youtube.com/watch?{yt_id}"
    duration = video_data["contentDetails"]["duration"]

    return {
        "new_title": yt_title,
        "new_url": yt_url,
        "status": status,
        "duration": duration,
    }


async def outdated_md_info(markdown_files: list[Path]):
    outdated_md_data = []
    for markdown_file in markdown_files:
        outdated_yt_info = []
        yt_info = await get_file_yt_info(markdown_file)
        for yt_data in yt_info:
            yt_url, yt_name, yt_type = yt_data["url"], yt_data["name"], yt_data["type"]

            parsed_yt_url = urlparse(yt_url)
            query_params = parse_qs(parsed_yt_url.query)

            if yt_data["type"] == "videos":
                yt_url_id = query_params.get("v")[0]
                is_vd_outdated = await is_yt_url_outdated(yt_url_id)
            else:
                yt_url_id = query_params.get("list")[0]
                is_vd_outdated = await is_yt_url_outdated(yt_url_id, yt_type)

            if is_vd_outdated:
                data = await fetch_youtube_data(yt_name, yt_type)
                extra_data = (
                    {"status": data}
                    if isinstance(data, str)
                    else await get_new_yt_data(data)
                )

                outdated_yt_info.append(
                    {
                        "old_title": yt_data["name"],
                        "type": yt_data["type"],
                        "old_url": yt_data["url"],
                        **extra_data,
                    }
                )
        outdated_md_data.append({markdown_file.name: outdated_yt_info})
    return outdated_md_data


async def extract_keywords(title: str):
    words = re.findall(r"[a-zA-Z0-9#+]+", title.lower())
    stop_words = {"for", "in", "is", "what", "the", "a", "an", "to", "of", "and", "on"}
    keywords = [word for word in words if word not in stop_words]
    return keywords


async def is_relevant(title: str, keywords: list[str]):
    title_lower = title.lower()

    if any(x in title_lower for x in ["😂", "🤣", "#shorts", "#meme"]):
        return False

    # Count how many keywords have at least a 30% fuzzy match score
    matched_keywords = sum(
        1 for keyword in keywords if fuzz.partial_ratio(keyword, title_lower) >= 90
    )

    threshold = max(1, int(len(keywords) * 0.7))
    return matched_keywords >= threshold


async def get_best_item(results):
    def score(video):
        stats = video.get("statistics", {})
        views = int(stats.get("viewCount", 0))
        likes = int(stats.get("likeCount", 0))
        return views + likes

    best = max(results, key=score, default=None)
    return best


async def is_long_enough(duration_str: str, min_minutes=5):
    duration = isodate.parse_duration(duration_str)
    return duration.total_seconds() >= min_minutes * 60


async def get_vid_duration(video_ids: list[str], type="videos"):
    joined_vid_ids = ",".join(video_ids)
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/{type}?key={API_KEY}&id={joined_vid_ids}&part=snippet,contentDetails,statistics&order=viewCount"
        )
    data = response.json()
    return data["items"]


async def fetch_youtube_data(title: str, type="videos"):
    min_upload_year = datetime.now().year - 3
    published_after = f"{min_upload_year}-12-31T00:00:00Z"

    # Step 1: Get search results
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/search?key={API_KEY}&part=snippet&q={title}&publishedAfter={published_after}&order=relevance&type={type}&maxResults=50"
        )

    data = response.json()
    items = data.get("items", [])
    print(items)

    if not items:
        return f"No search result for '{title}'"

    # Step 2: Filter by keyword relevance
    keywords = await extract_keywords(title)
    check_relevant_items = await asyncio.gather(
        *[is_relevant(item["snippet"]["title"], keywords) for item in items]
    )

    relevant_items = [
        item for item, passed in zip(items, check_relevant_items) if passed
    ]

    if not relevant_items:
        return f"No relevant search for '{title}'"

    for item in relevant_items:
        if not isinstance(item.get("id"), dict) or "videoId" not in item["id"]:
            print(f"Skipping item with unexpected ID format: {item['id']}")

    # Step 3: Get video IDs
    video_ids = [
        item["id"]["videoId"]
        for item in relevant_items
        if isinstance(item.get("id"), dict) and "videoId" in item["id"]
    ]
    vids_info = await get_vid_duration(video_ids, type)

    # Step 4: Filter by duration
    check_long_enough_items = await asyncio.gather(
        *[is_long_enough(item["contentDetails"]["duration"]) for item in vids_info]
    )

    long_enough_items = [
        item for item, passed in zip(vids_info, check_long_enough_items) if passed
    ]
    if not long_enough_items:
        return "No relevant content that is >= 5 mins"

    # Step 5: Find best match by title similarity
    best_item = await get_best_item(long_enough_items)
    return best_item
