import os
import re
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



# Now use folder_path to walk through the folder and its subfolders
markdown_files = get_markdown_files(folder_path)
markdown_file = markdown_files[1]

yt_urls = get_file_yt_urls(markdown_file)

if is_dry_run:
    # create dry-run log file
    create_log_file(markdown_file, yt_urls, is_dry_run=True)
elif not is_dry_run and logging_enabled:
    # Create update log file
    create_log_file(markdown_file, yt_urls)
