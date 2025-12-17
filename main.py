import zipfile
import io
import json
import requests
import os
import shutil
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import exiftool
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/114.0.0.0 Safari/537.36"
}


# Define supported media types (includes PNG files now)
SUPPORTED_TYPES = (
    '.jpg', '.jpeg', '.heic', '.mp4',
    '.mov', '.png', '.avi', '.wmv',
    '.webp', '.mkv', '.m4v'
)
VIDEO_EXTENSIONS = ('.mp4', '.mov', '.avi', '.wmv', '.mkv', '.m4v')


def is_supported_file(filename):
    filename_lower = filename.lower()
    for ext in SUPPORTED_TYPES:
        if filename_lower.endswith(ext):
            return True
    return False


def is_video_file(filename):
    return filename.lower().endswith(VIDEO_EXTENSIONS)


def set_metadata(file, date, lat, lon, media_type, et):
    # Skip setting metadata on PNG files
    if file.lower().endswith('.png'):
        print(f"⚠ Skipping metadata set for PNG: {os.path.basename(file)} (no GPS/time needed)")
        return True  # Consider processed without metadata

    try:
        lat_float = float(lat)
        lon_float = float(lon)
        lat_ref = 'N' if lat_float >= 0 else 'S'
        lon_ref = 'E' if lon_float >= 0 else 'W'
        lat_abs = abs(lat_float)
        lon_abs = abs(lon_float)

        # Convert UTC to local timezone
        dt_utc = datetime.strptime(date, '%Y:%m:%d %H:%M:%S').replace(tzinfo=ZoneInfo('UTC'))
        local_tz = ZoneInfo('America/Toronto')
        dt_local = dt_utc.astimezone(local_tz)
        local_date_str = dt_local.strftime('%Y:%m:%d %H:%M:%S')
        
        # Calculate timezone offset
        offset_seconds = dt_local.utcoffset().total_seconds()
        offset_hours = int(offset_seconds // 3600)
        offset_minutes = int((offset_seconds % 3600) // 60)
        offset_str = f"{offset_hours:+03d}:{offset_minutes:02d}"

        if media_type.lower() == 'video' or file.lower().endswith(VIDEO_EXTENSIONS):
            # Video metadata (MP4, MOV, AVI, etc.)
            tags = {
                'QuickTime:GPSCoordinates': f"{lat_abs:.6f}, {lon_abs:.6f}",
                'QuickTime:GPSLatitudeRef': lat_ref,
                'QuickTime:GPSLongitudeRef': lon_ref,
                'Keys:GPSCoordinates': f"{lat_float:.6f} {lon_float:.6f} 0.000000",
                'XMP:GPSLatitude': lat_float,
                'XMP:GPSLongitude': lon_float,
                'XMP:GPSDateTime': f"{date.replace(' ', 'T')}Z",
                'CreateDate': local_date_str,
                'TrackCreateDate': local_date_str,
                'TrackModifyDate': local_date_str,
                'MediaCreateDate': local_date_str,
                'MediaModifyDate': local_date_str
            }
        else:
            # Image metadata (JPEG/HEIC)
            tags = {
                'DateTimeOriginal': local_date_str,
                'CreateDate': local_date_str,
                'GPSLatitude': lat_abs,
                'GPSLongitude': lon_abs,
                'GPSLatitudeRef': lat_ref,
                'GPSLongitudeRef': lon_ref,
                'OffsetTimeOriginal': offset_str,
                'OffsetTime': offset_str
            }

        # Set metadata using PyExifTool
        et.set_tags(
            [file],
            tags=tags,
            params=["-P", "-overwrite_original"]
        )
        
        print(f"✓ Metadata set for {os.path.basename(file)}")
        return True

    except Exception as e:
        print(f"✗ Metadata error for {os.path.basename(file)}: {e}")
        return False


def verify_metadata(file, date, lat, lon, media_type, et):
    """Verify metadata was written correctly"""
    try:
        metadata = et.get_metadata([file])
        
        if not metadata:
            print(f"✗ No metadata found for {os.path.basename(file)}")
            return False
        
        file_meta = metadata[0]
        expected_lat = abs(float(lat))
        expected_lon = abs(float(lon))
        
        if media_type.lower() == 'video' or file.lower().endswith(VIDEO_EXTENSIONS):
            gps_found = False
            for key in ['QuickTime:GPSCoordinates', 'Keys:GPSCoordinates', 'XMP:GPSLatitude']:
                if key in file_meta:
                    gps_found = True
                    break
            
            if gps_found:
                print(f"✓ Metadata verified for {os.path.basename(file)}")
                return True
            else:
                print(f"✗ GPS not found in {os.path.basename(file)}")
                return False
        else:
            lat_meta = None
            lon_meta = None
            
            for lat_key in ['EXIF:GPSLatitude', 'Composite:GPSLatitude']:
                if lat_key in file_meta:
                    lat_meta = file_meta[lat_key]
                    break
            
            for lon_key in ['EXIF:GPSLongitude', 'Composite:GPSLongitude']:
                if lon_key in file_meta:
                    lon_meta = file_meta[lon_key]
                    break
            
            if lat_meta is not None and lon_meta is not None:
                try:
                    found_lat = abs(float(lat_meta))
                    found_lon = abs(float(lon_meta))
                    
                    lat_match = abs(found_lat - expected_lat) < 0.001
                    lon_match = abs(found_lon - expected_lon) < 0.001
                    
                    if lat_match and lon_match:
                        print(f"✓ Metadata verified for {os.path.basename(file)}")
                        return True
                    else:
                        print(f"⚠ GPS differs for {os.path.basename(file)} (tolerance)")
                        return True
                except (ValueError, TypeError) as e:
                    print(f"✗ GPS parsing error: {e}")
                    return False
            else:
                print(f"✗ GPS not found in {os.path.basename(file)}")
                return False

    except Exception as e:
        print(f"✗ Verification error for {os.path.basename(file)}: {e}")
        return False


def download_with_retries(url, headers, max_retries=3, backoff=2):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response
        except (requests.exceptions.HTTPError, requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"❗ Download failed on attempt {attempt}: {e}")
            if attempt == max_retries:
                raise
            else:
                time.sleep(backoff ** attempt)
    return None


def process_zip_file(zip_content, date, lat, lon, download_folder, safe_date, et):
    """
    Extract and process supported files from ZIP
    Tag metadata for videos and images except PNG
    Save files to download_folder
    Returns: Number of files successfully processed
    """
    processed_count = 0
    
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            file_list = z.namelist()
            
            print(f"   ZIP contains {len(file_list)} files")
            
            for file_name in file_list:
                if file_name.endswith('/'):
                    continue
                
                if not is_supported_file(file_name):
                    print(f"   ⊘ Skipping unsupported: {file_name}")
                    continue
                
                # Extract to temp folder first
                extract_folder = os.path.join(download_folder, f"temp_{safe_date}")
                os.makedirs(extract_folder, exist_ok=True)
                
                z.extract(file_name, extract_folder)
                extracted_path = os.path.join(extract_folder, file_name)
                
                print(f"   → Processing: {file_name}")
                
                # Infer media type per file by extension
                file_media_type = 'video' if is_video_file(file_name) else 'image'
                
                # Set metadata only if not PNG and GPS present
                if not file_name.lower().endswith('.png') and lat and lon:
                    if set_metadata(extracted_path, date, lat, lon, file_media_type, et):
                        verify_metadata(extracted_path, date, lat, lon, file_media_type, et)
                    else:
                        print(f"   ✗ Metadata set failed for {file_name}")
                
                # Move extracted file to download_folder root to keep permanently
                final_path = os.path.join(download_folder, os.path.basename(file_name))
                shutil.move(extracted_path, final_path)
                print(f"   → Saved to: {final_path}")
                
                processed_count += 1
            
            # Cleanup temp folder if empty
            if os.path.exists(extract_folder) and not os.listdir(extract_folder):
                os.rmdir(extract_folder)
            
            return processed_count
        
    except zipfile.BadZipFile:
        print(f"✗ Invalid ZIP file")
        return 0
    except Exception as e:
        print(f"✗ ZIP processing error: {e}")
        return 0


def main():
    try:
        with open('memories_history.json', 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("✗ memories_history.json not found!")
        return
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON file: {e}")
        return

    download_folder = 'downloaded_media'
    os.makedirs(download_folder, exist_ok=True)

    saved_media = data.get("Saved Media", [])
    if not saved_media:
        print("✗ No 'Saved Media' found in JSON")
        return

    print(f"Found {len(saved_media)} items to process\n")
    print("="*60)

    with exiftool.ExifToolHelper() as et:
        print("✓ ExifTool initialized\n")

        total_processed = 0
        total_skipped = 0
        total_errors = 0

        for idx, item in enumerate(saved_media, 1):
            print(f"\n[{idx}/{len(saved_media)}] Processing item...")

            date = item.get("Date", "").replace('-', ':').replace(' UTC', '')
            if not date:
                print(f"✗ Missing date, skipping")
                total_errors += 1
                continue

            loc_str = item.get("Location", "")
            if not loc_str:
                print(f"⚠ Missing location")
                lat, lon = None, None
            else:
                try:
                    coords = loc_str.split(':')[1].strip().split(',')
                    lat = coords[0].strip()
                    lon = coords[1].strip()
                    print(f"   Location: {lat}, {lon}")
                except Exception as e:
                    print(f"✗ Location parse error: {e}")
                    lat, lon = None, None

            url = item.get("Media Download Url")
            if not url:
                print(f"✗ Missing download URL, skipping")
                total_errors += 1
                continue

            media_type = item.get("Media Type", "").lower()
            extension = 'mp4' if media_type == 'video' else 'jpg'  # Default extension
            url_lower = url.lower()
            for ext in SUPPORTED_TYPES:
                if url_lower.endswith(ext):
                    extension = ext.lstrip('.')
                    break

            safe_date = date.replace(':', '').replace(' ', '_')
            filename = f"{media_type}_{safe_date}.{extension}"
            file_path = os.path.join(download_folder, filename)

            if os.path.exists(file_path):
                print(f"⚠ {filename} exists, skipping")
                total_skipped += 1
                continue

            print(f"⬇ Downloading from Snapchat...")
            try:
                response = download_with_retries(url, headers)
            except Exception as e:
                print(f"✗ Download error: {e}")
                total_errors += 1
                continue

            content_type = response.headers.get('Content-Type', '').lower()
            print(f"   Content-Type: {content_type}")

            if 'zip' in content_type or 'application/zip' in content_type:
                print(f"   📦 ZIP file detected")
                processed = process_zip_file(
                    response.content, date, lat, lon,
                    download_folder, safe_date, et
                )
                if processed > 0:
                    total_processed += processed
                else:
                    total_errors += 1

            elif 'video' in content_type or 'image' in content_type:
                print(f"   💾 Direct media file")
                with open(file_path, 'wb') as f:
                    f.write(response.content)

                # Infer single file media type for direct files
                file_media_type = 'video' if is_video_file(file_path) else 'image'

                if lat and lon:
                    if set_metadata(file_path, date, lat, lon, file_media_type, et):
                        verify_metadata(file_path, date, lat, lon, file_media_type, et)
                        total_processed += 1
                    else:
                        total_errors += 1
                else:
                    print(f"⚠ No GPS data for {filename}")
                    total_processed += 1
            else:
                print(f"✗ Unsupported content-type: {content_type}")
                total_errors += 1

    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"✓ Successfully processed: {total_processed}")
    print(f"⚠ Skipped (already exists): {total_skipped}")
    print(f"✗ Errors: {total_errors}")
    print(f"Total items: {len(saved_media)}")
    print("="*60)


if __name__ == "__main__":
    main()
