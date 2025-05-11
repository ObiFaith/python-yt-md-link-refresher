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
        type = yt_data.get("type", "")
        status = yt_data.get("status", "")
        old_url = yt_data.get("old_url", "")
        new_url = yt_data.get("new_url", "")
        duration = yt_data.get("duration", "")
        old_title = yt_data.get("old_title", "")
        new_title = yt_data.get("new_title", "")

        file_text += f"NAME: {old_title}\n[OLD] {old_url}\n"

        if new_url and new_title:
            if type == "videos":
                duration_in_word = await convert_duration(duration)
                file_text += (
                    f"NAME: {new_title}\nTIME: {duration_in_word}\n[NEW] {new_url}\n"
                )
            else:
                file_text += f"NAME: {new_title}\n[NEW] {new_url}\n"

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


async def create_log_file(is_dry_run=False):
    current_date = date.today()
    log_file_path = "dry_run" if is_dry_run else "update_log"
    file_mode = "DRY RUN" if is_dry_run else "ACTUAL UPDATE"

    with open(f"{log_file_path}_{current_date}.log", "w", encoding="utf-8") as log_file:
        log_file.writelines(
            [
                "=== YouTube Markdown Link Refresher Log ===\n",
                f"Date: {current_date}\n",
                f"Mode: {file_mode}\n",
            ]
        )


from pathlib import Path


async def get_markdown_files(folder_path: Path):
    """
    Recursively finds all .md (Markdown) files in the given folder path,
    excluding files in 'projects', 'project', 'assignment', 'assignments' directories.

    Args:
        folder_path (Path): The root directory to search from.

    Returns:
        List[Path]: A list of Path objects representing all markdown files found,
                     excluding those in the specified directories.
    """
    # List of folder names to exclude
    exclude_folders = ["projects", "project", "assignment", "assignments"]

    # Use rglob to find all .md files and filter out the ones in excluded directories
    return [
        file
        for file in folder_path.rglob("*.md")
        if not any(exclude_folder in file.parts for exclude_folder in exclude_folders)
    ]


async def get_new_yt_data(data, type: str):
    sub_url = "watch?v=" if type == "videos" else "playlist?list="
    sub_url += data["id"] or data["items"]["id"]
    duration = data["contentDetails"]["duration"] if type == "videos" else ""

    return {
        "new_title": data["snippet"]["title"],
        "new_url": f"https://www.youtube.com/{sub_url}",
        "status": "Updated successfully!",
        "duration": duration,
    }


async def is_yt_url_outdated(yt_id: str, type="videos"):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{YOUTUBE_API_URL}/{type}?key={API_KEY}&id={yt_id}&part=snippet"
            )
        except httpx.ConnectTimeout:
            print(f"Timeout while accessing {type} with id: {yt_id}")

    data = response.json()

    if "error" in data and data["error"].get("code") == 403:
        print(data["error"]["message"])
        exit(1)

    current_year = datetime.now().year
    published_year = int(data["items"][0]["snippet"]["publishedAt"][:4])

    return current_year - 3 >= published_year


async def check_and_update_yt(yt_data):
    yt_url, yt_name, yt_type = yt_data["url"], yt_data["name"], yt_data["type"]

    parsed_yt_url = urlparse(yt_url)
    query_params = parse_qs(parsed_yt_url.query)

    if "youtu.be" in parsed_yt_url.netloc:
        yt_url_id = parsed_yt_url.path.strip("/")
    else:
        yt_url_id = (
            query_params.get("v")[0]
            if yt_type == "videos"
            else query_params.get("list")[0]
        )

    if yt_url_id in yt_outdated_cache:
        is_outdated = yt_outdated_cache[yt_url_id]
    else:
        is_outdated = await is_yt_url_outdated(yt_url_id, yt_type)
        yt_outdated_cache[yt_url_id] = is_outdated

    if not is_outdated:
        return None

    data = await fetch_youtube_data(yt_name, yt_type)

    if not data:
        return {
            "old_title": yt_name,
            "type": yt_type,
            "old_url": yt_url,
            "status": "Failed to fetch YouTube data.",
        }

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


async def outdated_md_info(
    markdown_files: list[Path], is_dry_run=False
) -> dict[str, dict[str, list]]:
    """
    Scan markdown files for outdated YouTube videos/playlists (3+ years old)
    and fetch updated replacements.

    Args:
        markdown_files (list[Path]): List of markdown file paths.
    """

    current_date = date.today()
    log_file_path = "dry_run" if is_dry_run else "update_log"

    for markdown_file in markdown_files:
        outdated_yt_info: list[dict] = []
        yt_info = await get_file_yt_info(markdown_file)

        # Gather all `check_and_update_yt()` tasks concurrently
        update_tasks = [check_and_update_yt(yt_data) for yt_data in yt_info]
        results = await asyncio.gather(*update_tasks)
        print(f"results: {results}\n")

        # Filter out None results (non-outdated)
        outdated_yt_info = [res for res in results if res]

        if outdated_yt_info:
            file_text = await file_update_template(markdown_file, outdated_yt_info)

            with open(
                f"{log_file_path}_{current_date}.log", "a", encoding="utf-8"
            ) as log_file:
                log_file.write(file_text)


async def extract_keywords(title: str):
    words = re.findall(r"[a-zA-Z0-9#+]+", title.lower())
    stop_words = {"for", "in", "is", "what", "the", "a", "an", "to", "of", "and", "on"}
    keywords = [word for word in words if word not in stop_words]
    return keywords


async def is_relevant(title: str, keywords: list[str]):
    title_lower = title.lower()

    if any(x in title_lower for x in ["😂", "🤣", "#shorts", "#meme"]):
        return False

    # Count how many keywords have at least a 70% fuzzy match score
    matched_keywords = sum(
        1 for keyword in keywords if fuzz.partial_ratio(keyword, title_lower) >= 70
    )

    threshold = max(1, int(len(keywords) * 0.7))
    return matched_keywords >= threshold


async def get_best_video(results):
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


async def get_video_info(video_ids: list[str]):
    joined_video_ids = ",".join(video_ids)

    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/videos?key={API_KEY}&id={joined_video_ids}&part=snippet,contentDetails,statistics&order=viewCount"
        )

    data = response.json()
    return data["items"]


async def fetch_youtube_data(title: str, type="videos"):
    min_upload_year = datetime.now().year - 3
    published_after = f"{min_upload_year}-12-31T00:00:00Z"

    # Step 1: Get search results
    async with httpx.AsyncClient() as client:
        title_normalized = title.lower().replace("c#", "c sharp")
        response = await client.get(
            f"{YOUTUBE_API_URL}/search?key={API_KEY}&part=snippet&q={title_normalized}&publishedAfter={published_after}&order=relevance&type={type}&maxResults=50"
        )

    data = response.json()
    print(f"fetched data: {data}\n")
    items = data.get("items", [])
    print(f"get items: {items}\n")

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
        video_items = await get_video_info(video_ids)

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

        best_video = await get_best_video(long_video_items)
        return best_video

    elif type == "playlists":
        playlist_items = [item for item in relevant_items if "playlistId" in item["id"]]
        return playlist_items[0]
