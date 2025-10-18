import zipfile
import io
import json
import subprocess
import requests
import os
import shutil

from datetime import datetime
from zoneinfo import ZoneInfo


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/114.0.0.0 Safari/537.36"
}

def set_metadata(file, date, lat, lon):
    lat_float = float(lat)
    lon_float = float(lon)
    lat_ref = 'N' if lat_float >= 0 else 'S'
    lon_ref = 'E' if lon_float >= 0 else 'W'
    lat_abs = abs(lat_float)
    lon_abs = abs(lon_float)

    # Parse UTC date string
    dt_utc = datetime.strptime(date, '%Y:%m:%d %H:%M:%S').replace(tzinfo=ZoneInfo('UTC'))
    
    # Determine local timezone from GPS coords - example for Toronto area
    # For accurate global timezone from lat/lon, consider third-party services or timezonefinder package
    local_tz = ZoneInfo('America/Toronto')
    dt_local = dt_utc.astimezone(local_tz)
    
    local_date_str = dt_local.strftime('%Y:%m:%d %H:%M:%S')
    
    # Calculate offset string
    offset_seconds = dt_local.utcoffset().total_seconds()
    offset_hours = int(offset_seconds // 3600)
    offset_minutes = int((offset_seconds % 3600) // 60)
    offset_str = f"{offset_hours:+03d}:{offset_minutes:02d}"

    exif_command = [
        'exiftool',
        f'-DateTimeOriginal={local_date_str}',
        f'-CreateDate={local_date_str}',
        f'-GPSLatitude={lat_abs}',
        f'-GPSLongitude={lon_abs}',
        f'-GPSLatitudeRef={lat_ref}',
        f'-GPSLongitudeRef={lon_ref}',
        f'-OffsetTimeOriginal={offset_str}',
        f'-OffsetTime={offset_str}',
        '-overwrite_original',
        file
    ]
    subprocess.run(exif_command)

# Load the JSON metadata from file
with open('memories_history.json', 'r') as f:
    data = json.load(f)

download_folder = 'downloaded_media'
os.makedirs(download_folder, exist_ok=True)

for item in data["Saved Media"]:
    # Convert date format from "YYYY-MM-DD HH:MM:SS UTC" to "YYYY:MM:DD HH:MM:SS"
    date = item["Date"].replace('-', ':').replace(' UTC', '')

    # Parse latitude and longitude values from the "Location" string
    loc_str = item.get("Location", "")
    if loc_str:
        coords = loc_str.split(':')[1].strip().split(',')
        lat = coords[0].strip()
        lon = coords[1].strip()
    else:
        lat, lon = None, None

    url = item["Media Download Url"]
    media_type = item["Media Type"].lower()
    # Define file extension based on media type (default mp4 for video, jpg for photo)
    extension = 'mp4' if media_type == 'video' else 'jpg'

    # Create a unique filename
    safe_date = date.replace(':', '').replace(' ', '_')
    filename = f"{media_type}_{safe_date}.{extension}"
    file_path = os.path.join(download_folder, filename)

    if not os.path.exists(file_path):
        print(f"Downloading {url} as {filename}...")
        response = requests.get(url, headers=headers)
        print("Status code:", response.status_code)
        content_type = response.headers.get('Content-Type', '').lower()
        print("Content-Type:", content_type)

        if response.status_code == 200:
            # Handle ZIP files
            if 'zip' in content_type:
                # Save ZIP first
                zip_path = os.path.join(download_folder, filename.replace(f".{extension}", ".zip"))
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded ZIP archive as {zip_path}")

                # Extract all files inside the ZIP
                extract_folder = os.path.join(download_folder, f"{media_type}_{safe_date}_extracted")
                os.makedirs(extract_folder, exist_ok=True)
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    z.extractall(extract_folder)
                    print(f"Extracted ZIP contents to {extract_folder}")

                # Set metadata on extracted files
                for extracted_file in os.listdir(extract_folder):
                    # Only process .jpg, .jpeg, .mp4 files — skip .png and others
                    if extracted_file.lower().endswith(('.jpg', '.jpeg', '.mp4')):
                        extracted_path = os.path.join(extract_folder, extracted_file)
                        if lat and lon:
                            set_metadata(extracted_path, date, lat, lon)
                            print(f"Metadata set for {extracted_file}")
                        else:
                            print(f"No location data for {extracted_file}, skipping metadata update.")
                    else:
                        # Skip .png and unexpected files silently (or print if you want)
                        print(f"Ignoring file {extracted_file} (skipped, not processed).")

                for extracted_file in os.listdir(extract_folder):
                    if extracted_file.lower().endswith(('.jpg', '.jpeg', '.mp4')):
                        extracted_path = os.path.join(extract_folder, extracted_file)
                        dest_path = os.path.join(download_folder, extracted_file)
                        shutil.move(extracted_path, dest_path)
                        print(f"Moved {extracted_file} to {download_folder}")

                shutil.rmtree(extract_folder)
                print(f"Deleted extracted folder {extract_folder}")

                os.remove(zip_path)
                print(f"Deleted ZIP file {zip_path}")


            # Handle normal media files
            elif 'video' in content_type or 'image' in content_type:
                with open(file_path, 'wb') as file:
                    file.write(response.content)
            else:
                print(f"Skipped download, unexpected content type for {url}")
                print(response.text[:500])  # Show beginning of the response for debug
                continue
        else:
            print(f"Failed to download {url}, status code {response.status_code}")
            continue