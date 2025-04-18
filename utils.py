import re
import datetime
from pathlib import Path


def yt_urls_file_log(file_path: Path, yt_urls: list[str]):
    file_text = f"File: {file_path.as_posix()}\n"
    for yt_url in yt_urls:
        file_text += f"[OLD] {yt_url} (Uploaded: 2019-07-10)\n[NEW] https://www.youtube.com/watch?v=xyz9876 (Uploaded: 2024-01-12)\n\n"
    return file_text


def get_file_yt_urls(file_path: Path):
    yt_urls = []
    yt_url_pattern = re.compile(
        r"\((https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtube.com/playlist?|youtu\.be/)[^)]+)\)"
    )

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            match = yt_url_pattern.search(line)
            if match:
                url = re.sub(r"&index=\d+", "", match.group(1))
                yt_urls.append(url)

    return yt_urls


def create_log_file(markdown_file, yt_urls, is_dry_run=False):
    current_date = datetime.date.today()
    file_text = yt_urls_file_log(markdown_file, yt_urls)
    log_file_path = "dry_run" if is_dry_run else "update_log"
    file_mode = "DRY RUN" if is_dry_run else "ACTUAL UPDATE"

    with open(f"{log_file_path}_{current_date}.log", "w") as log_file:
        log_file.writelines(
            [
                "=== YouTube Markdown Link Refresher Log ===\n",
                f"Date: {current_date}\n",
                f"Mode: {file_mode}\n\n",
            ]
        )

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
