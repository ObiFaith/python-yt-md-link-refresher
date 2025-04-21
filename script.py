import os
import json
import asyncio
import argparse
from utils import *
from pathlib import Path

# Create argument parser
parser = argparse.ArgumentParser(
    description="Update old YouTube links in markdown files."
)

# Define the path argument
parser.add_argument(
    "path",
    nargs="?",  # makes it optional
    default=os.getcwd(),  # fallback to current directory
    help="Path to the root folder to scan (default: current directory)",
)

# Optional flags
parser.add_argument(
    "--dry-run", action="store_true", help="Simulate changes without modifying files"
)
parser.add_argument(
    "--backup", action="store_true", help="Backup the original .md file"
)
parser.add_argument("--no-log", action="store_true", help="Disable logging")

# Parse the arguments
args = parser.parse_args()

# Access values
folder_path = Path(args.path)  # Converts to a Path object
is_backup = args.backup  # True if --backup was passed
is_dry_run = args.dry_run  # True if --dry-run was passed
logging_enabled = not args.no_log  # Default is True unless --no-log is passed


# Validate that path exists and is a directory.
if not (os.path.exists(folder_path) and os.path.isdir(folder_path)):
    print("Error: No such directory")
    exit(1)


async def main():
    markdown_files = await get_markdown_files(folder_path)
    outdated_md_data = await outdated_md_info(markdown_files)

    with open("dump.json", "w", encoding="utf-8") as json_file:
        json.dump(outdated_md_data, json_file)


asyncio.run(main())

""" # Log file updates
if is_dry_run:
    # create dry-run log file
    create_log_file(markdown_files, is_dry_run=True)
elif not is_dry_run and logging_enabled:
    # Create update log file
    create_log_file(markdown_files) """
