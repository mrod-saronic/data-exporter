import os
import sys
import shutil
import subprocess
from pathlib import Path

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 organize_and_unzip.py <ASSET_ID> <DATE1> [<DATE2> ...]")
        print("Example: python3 organize_and_unzip.py cr12 2025-01-01 2025-01-02")
        sys.exit(1)

    asset = sys.argv[1]
    dates = sys.argv[2:]

    mount_base = "/run/media/coco_mops"
    source_dirs = [d for d in Path(mount_base).iterdir() if d.is_dir()]
    if not source_dirs:
        print("No source directories found in the mount base.")
        sys.exit(1)
    source_dir = source_dirs[0]

    dest_dir = Path("/home/coco_mops/RPG/data_extract/raw_json")
    asset_dir = dest_dir / asset
    print(f"Creating asset directory: {asset_dir}")
    asset_dir.mkdir(parents=True, exist_ok=True)

    snarfd_backup_dir = None
    for possible_dir in ["snarfd", "snarfd_backup"]:
        candidate_dir = source_dir / possible_dir
        if candidate_dir.exists():
            snarfd_backup_dir = candidate_dir
            break

    if snarfd_backup_dir is None:
        print("Neither 'snarfd' nor 'snarfd_backup' directory found")
        sys.exit(1)

    print(f"Source Dir: {snarfd_backup_dir}")

    print("Starting file processing...")
    for date in dates:
        print(f"Processing files for date: {date}")
        files = list(snarfd_backup_dir.glob(f"snarf*{date}*.log.jsonl.zst"))
        if not files:
            print(f"No files found for date {date}")
            continue

        for file in files:
            print(f"Processing {file}")
            date_part = extract_date_part(file.name)
            if not date_part:
                print(f"Failed to extract date from {file}")
                continue

            target_dir = asset_dir / f"{date_part}_{asset}"
            print(f"Target directory: {target_dir}")
            target_dir.mkdir(parents=True, exist_ok=True)

            print("Copying file...")
            copied_file = target_dir / file.name
            shutil.copy(file, copied_file)

            print(f"Decompressing: {copied_file}")
            result = subprocess.run(["unzstd", "--rm", str(copied_file)], capture_output=True)
            if result.returncode == 0:
                print("Decompression successful!")
            else:
                print(f"Failed to decompress {file}")
                print(result.stderr.decode())

    print(f"{asset} -> All files organized under {asset_dir}")

def extract_date_part(filename):
    import re
    match = re.search(r"snarfd?\.([0-9]{4})-([0-9]{2})-([0-9]{2})T", filename)
    if match:
        return f"{match.group(1)}_{match.group(2)}_{match.group(3)}"
    return None

if __name__ == "__main__":
    main()