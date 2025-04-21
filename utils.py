import re
import os
import math
import httpx
import isodate
import asyncio
from pathlib import Path
from rapidfuzz import fuzz
from dotenv import load_dotenv
from datetime import date, datetime
from urllib.parse import urlparse, parse_qs

load_dotenv()

yt_outdated_cache = {}
API_KEY = os.getenv("GOOGLE_API_KEY")
YOUTUBE_API_URL = os.getenv("YOUTUBE_API_URL")


async def file_update_template(file_path: Path, yt_info: list[str]):
    file_text = f"\nFILE: {file_path.as_posix()}\n"

    for yt_data in yt_info:
        status, old_url, new_url, duration, old_title, new_title = (
            yt_data["status"],
            yt_data["old_url"],
            yt_data["new_url"],
            yt_data["duration"],
            yt_data["new_title"],
            yt_data["new_title"],
        )

        duration_in_word = await convert_duration(duration)

        file_text += f"NAME: {old_title}\n[OLD] {old_url}\n"

        if new_url and new_title:
            file_text += (
                f"[NEW] {new_url}\nNAME: {new_title}\nTIME: {duration_in_word}\n"
            )

        file_text += f"STAT: {status}\n\n"
    return file_text


async def convert_duration(duration):
    # Regex to extract minutes and seconds
    match = re.match(r"PT(\d+)M(\d+)S", duration)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))

        # Apply Math.ceil logic for seconds (round up if seconds > 0)
        if seconds > 0:
            minutes = math.ceil(minutes + 1)

        # If minutes are greater than or equal to 60, convert to hours
        if minutes >= 60:
            hours = minutes / 60
            return f"{hours:.2f} hours"
        else:
            return f"{minutes} minute(s)"
    return "Invalid duration format"


async def get_file_yt_info(file_path: Path):
    """
    Extracts YouTube video/playlist links from a markdown file.

    Args:
        file_path (Path): Path to a markdown file.

    Returns:
        List[dict]: A list of dictionaries with 'name', 'type', and 'url' for each YouTube link.
    """
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


async def create_log_file(markdown_files: list[Path], is_dry_run=False):
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
        yt_info = await get_file_yt_info(markdown_file)
        file_text = await file_update_template(markdown_file, yt_info)
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


async def get_new_yt_data(data, type: str):
    print(f"get_new_yt_data: {data}")
    sub_url = "watch?v=" if type == "videos" else "playlist?list="
    sub_url += data["id"]

    return {
        "new_title": data["snippet"]["title"],
        "new_url": f"https://www.youtube.com/{sub_url}",
        "status": "Updated successfully!",
        "duration": data["contentDetails"]["duration"],
    }


async def is_yt_url_outdated(yt_id: str, type="videos"):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/{type}?key={API_KEY}&id={yt_id}&part=snippet"
        )

    data = response.json()

    if data["error"]["code"] == 403:
        print(data["error"]["message"])
        exit(1)

    current_year = datetime.now().year
    published_year = int(data["items"][0]["snippet"]["publishedAt"][:4])

    return current_year - 3 >= published_year


async def check_and_update_yt(yt_data):
    yt_url, yt_name, yt_type = yt_data["url"], yt_data["name"], yt_data["type"]

    parsed_yt_url = urlparse(yt_url)
    query_params = parse_qs(parsed_yt_url.query)

    yt_url_id = (
        query_params.get("v")[0] if yt_type == "videos" else query_params.get("list")[0]
    )

    if yt_url_id in yt_outdated_cache:
        is_outdated = yt_outdated_cache[yt_url_id]
    else:
        is_outdated = await is_yt_url_outdated(yt_url_id, yt_type)
        yt_outdated_cache[yt_url_id] = is_outdated

    if not is_outdated:
        return None

    data = await fetch_youtube_data(yt_name, yt_type)
    extra_data = (
        {"status": data}
        if isinstance(data, str)
        else await get_new_yt_data(data, yt_type)
    )

    return {
        "old_title": yt_name,
        "type": yt_type,
        "old_url": yt_url,
        **extra_data,
    }


async def outdated_md_info(markdown_files: list[Path]) -> dict[str, dict[str, list]]:
    """
    Scan markdown files for outdated YouTube videos/playlists (3+ years old)
    and fetch updated replacements.

    Args:
        markdown_files (list[Path]): List of markdown file paths.

    Returns:
        dict: {
            parent_folder: {
                filename.md: [
                    {
                        "old_title": ...,
                        "type": ...,
                        "old_url": ...,
                        "new_title": ...,
                        "new_url": ...,
                        "status": ...,
                        "duration": ...
                    },
                    ...
                ],
                ...
            },
            ...
        }
    """
    outdated_md_data: dict[str, dict[str, list]] = {}

    for markdown_file in markdown_files:
        outdated_yt_info: list[dict] = []
        yt_info = await get_file_yt_info(markdown_file)

        # Gather all `check_and_update_yt()` tasks concurrently
        update_tasks = [check_and_update_yt(yt_data) for yt_data in yt_info]
        results = await asyncio.gather(*update_tasks)

        # Filter out None results (non-outdated)
        outdated_yt_info = [res for res in results if res]

        # Only add entry if there were outdated links
        if outdated_yt_info:
            parent_folder = "/".join(markdown_file.parts[:-1]) or "/"
            outdated_md_data.setdefault(parent_folder, {})[
                markdown_file.name
            ] = outdated_yt_info

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
        1 for keyword in keywords if fuzz.partial_ratio(keyword, title_lower) >= 70
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


async def get_item_info(item_ids: list[str], type="videos"):
    joined_item_ids = ",".join(item_ids)
    part_query = "snippet"

    if type == "videos":
        part_query += ",contentDetails,statistics&order=viewCount"

    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/{type}?key={API_KEY}&id={joined_item_ids}&part={part_query}"
        )

    data = response.json()
    return data["items"]


async def fetch_youtube_data(title: str, type="videos"):
    min_upload_year = datetime.now().year - 3
    published_after = f"{min_upload_year}-12-31T00:00:00Z"

    # Step 1: Get search results
    async with httpx.AsyncClient() as client:
        title_normalized = title.lower().replace("c#", "c sharp").replace("c++", "cpp")
        response = await client.get(
            f"{YOUTUBE_API_URL}/search?key={API_KEY}&part=snippet&q={title_normalized}&publishedAfter={published_after}&order=relevance&type={type}&maxResults=50"
        )

    data = response.json()
    items = data.get("items", [])

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
        print(f"No relevant search for '{title}'")
        return f"No relevant search for '{title}'"

    # Step 3: Handle video or playlist based on type
    if type == "videos":
        video_ids = [
            item["id"]["videoId"] for item in relevant_items if "videoId" in item["id"]
        ]
        video_items = await get_item_info(video_ids)

        # Step 4: Filter by duration
        check_long_video_items = await asyncio.gather(
            *[
                is_long_enough(item["contentDetails"]["duration"])
                for item in video_items
            ]
        )
        long_video_items = [
            item for item, passed in zip(video_items, check_long_video_items) if passed
        ]

        if not long_video_items:
            print(f"'{title}' content not >= 5 mins")
            return "No relevant video content that is >= 5 mins"

        best_item = await get_best_item(long_video_items)
        return best_item

    elif type == "playlists":
        playlist_ids = [
            item["id"]["playlistId"]
            for item in relevant_items
            if "playlistId" in item["id"]
        ]

        playlist_items = await get_item_info(playlist_ids, "playlists")
        best_item = await get_best_item(playlist_items)
        return best_item
