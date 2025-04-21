import re
import os
import httpx
import isodate
import asyncio
from pathlib import Path
from rapidfuzz import fuzz
from dotenv import load_dotenv
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

load_dotenv()

API_KEY = os.getenv("GOOGLE_API_KEY")
YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3"


async def file_update_template(file_path: Path, yt_info: list[str]):
    file_text = f"\nFILE: {file_path.as_posix()}\n"
    for yt_data in yt_info:
        status, old_url, new_url, old_title, new_title = (
            yt_data["status"],
            yt_data["old_url"],
            yt_data["new_url"],
            yt_data["old_title"],
            yt_data["new_title"],
        )

        file_text += f"NAME: {old_title}\n[OLD] {old_url}\n"

        if new_url and new_title:
            file_text += f"[NEW] {new_url}\nNAME: {new_title}\n"

        file_text += f"STAT: {status}\n\n"
    return file_text


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


async def is_yt_url_outdated(video_ids: list[str], type="videos") -> dict[str, bool]:
    if not video_ids:
        return {}

    joined_ids = ",".join(video_ids)

    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/{type}?key={API_KEY}&id={joined_ids}&part=snippet"
        )
        data = response.json()

    if data["error"]["code"] == 403:
        print(f"Quota exceeded for YouTube API: {data['error']['message']}")
        return

    items = data.get("items", [])
    threshold_date = datetime.now(timezone.utc) - timedelta(days=365 * 3)  # 3 years ago

    # Map video_id => is_outdated
    result = {}
    for item in items:
        vid_id = item.get("id")
        published_at = item["snippet"]["publishedAt"]  # e.g. "2019-03-05T15:23:45Z"
        published_date = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
        result[vid_id] = published_date <= threshold_date

    # Handle missing videos (e.g., private or deleted ones)
    for vid in video_ids:
        if vid not in result:
            result[vid] = False

    return result


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

        # Collect all IDs and map back
        video_ids: list[str] = []
        id_to_data: dict[str, dict] = {}

        for yt_data in yt_info:
            parsed = urlparse(yt_data["url"])
            params = parse_qs(parsed.query)

            if yt_data["type"] == "videos":
                vid_id = params.get("v", [None])[0]
            else:
                vid_id = params.get("list", [None])[0]

            if vid_id:
                video_ids.append(vid_id)
                id_to_data[vid_id] = yt_data

        # Skip if no IDs found
        if not video_ids:
            continue

        # Batch check which IDs are outdated
        outdated_status_map = await is_yt_url_outdated(video_ids)
        if outdated_status_map is None:
            return

        # For each outdated ID, fetch replacement data
        for vid_id, is_outdated in outdated_status_map.items():
            if not is_outdated:
                continue

            yt_data = id_to_data[vid_id]
            print(f"Outdated: {yt_data['name']} ({vid_id})")

            fetched = await fetch_youtube_data(yt_data["name"], yt_data["type"])
            print(f"Fetched data for {yt_data['name']}: {fetched}")

            if isinstance(fetched, str):
                extra = {"status": fetched}
            else:
                extra = await get_new_yt_data(fetched)

            outdated_yt_info.append(
                {
                    "old_title": yt_data["name"],
                    "type": yt_data["type"],
                    "old_url": yt_data["url"],
                    **extra,
                }
            )

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
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/{type}?key={API_KEY}&id={joined_item_ids}&part=snippet,contentDetails,statistics&order=viewCount"
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
        video_ids = [item["id"]["videoId"] for item in relevant_items]
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

    elif type == "playlists":
        playlist_ids = [item["id"]["playlistId"] for item in relevant_items]

        playlist_items = await get_item_info(playlist_ids, "playlists")
        best_item = await get_best_item(playlist_items)

    return best_item
