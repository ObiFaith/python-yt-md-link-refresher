import re
import os
import httpx
from pathlib import Path
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


def get_file_yt_info(file_path: Path):
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


def create_log_file(markdown_files, is_dry_run=False):
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


def get_markdown_files(folder_path: Path):
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


async def outdated_markdown_files(folder_path: Path):
    outdated_md_files = []
    markdown_files = get_markdown_files(folder_path)
    for markdown_file in markdown_files:
        outdated_yt_info = []
        yt_info = get_file_yt_info(markdown_file)
        for yt_data in yt_info:
            yt_url = yt_data["url"]
            parsed_yt_url = urlparse(yt_url)
            query_params = parse_qs(parsed_yt_url.query)

            if yt_data["type"] == "videos":
                yt_url_id = query_params.get("v")[0]
            else:
                yt_url_id = query_params.get("list")[0]

            is_vd_outdated = await is_yt_url_outdated(yt_url_id)
            if is_vd_outdated:
                outdated_yt_info.append(
                    {
                        "old_title": yt_data["name"],
                        "type": yt_data["type"],
                        "old_url": yt_data["url"],
                        "new_title": "",
                        "new_url": "",
                        "status": "",
                    }
                )
        outdated_md_files.append({markdown_file.name: outdated_yt_info})
    return outdated_md_files


async def fetch_youtube_data(title: str, type="videos"):
    min_upload_year = datetime.now().year - 3
    publishedAfter = f"{min_upload_year}-01-01T00:00:00Z"
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{YOUTUBE_API_URL}/search?key={API_KEY}&part=snippet&q={title}?&publishedAfter={publishedAfter}&order=relevance&type={type}&maxResults={30}"
        )
    data = response.json()
    return [snippets["snippet"] for snippets in data["items"]]
