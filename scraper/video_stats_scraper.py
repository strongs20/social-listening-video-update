import httpx
import json
import re

# regex to find the specific script tag with video data
STATE_JSON_REGEX = re.compile(r'<script id="__FRONTITY_CONNECT_STATE__"[^>]*>([\s\S]*?)<\/script>')

async def get_video_stats(video_id: str):
    embed_url = f"https://www.tiktok.com/embed/v2/{video_id}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        # only get the last 150kb of the file to avoid unnecessary CSS and HTML
        "Range": "bytes=-153600"
    }

    async with httpx.AsyncClient(headers=headers, timeout=20.0, follow_redirects=True) as client:
        try:
            response = await client.get(embed_url)
            
            if response.status_code not in [200, 206]:
                print(f"Error for video {video_id}: Received status code {response.status_code}")
                return None

            match = STATE_JSON_REGEX.search(response.text)
            if not match:
                print(f"Error for video {video_id}: Could not find '__FRONTITY_CONNECT_STATE__' script tag via regex.")
                return None

            raw_json = match.group(1)
            data = json.loads(raw_json)

            video_data = data.get("source", {}).get("data", {}).get(f"/embed/v2/{video_id}", {}).get("videoData", {})
            
            if not video_data:
                print(f"Error for video {video_id}: 'videoData' key not found in JSON path.")
                return None
            
            item_infos = video_data.get("itemInfos", {})
            
            if not item_infos:
                print(f"Error for video {video_id}: 'itemInfos' key not found in 'videoData'.")
                return None

            return {
                "views": item_infos.get("playCount", 0),
                "likes": item_infos.get("diggCount", 0),
                "comments": item_infos.get("commentCount", 0),
                "shares": item_infos.get("shareCount", 0)
            }

        except httpx.RequestError as e:
            print(f"HTTP Request Error for video {video_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error for video {video_id}: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred for video {video_id}: {e}")
            return None

