"""Quick test to verify YouTube API key works."""
import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('YOUTUBE_API_KEY')

if not api_key:
    raise ValueError("YOUTUBE_API_KEY not found in .env")

url = 'https://www.googleapis.com/youtube/v3/search'
params = {
    'part': 'snippet',
    'q': 'trader joes haul',
    'type': 'video',
    'maxResults': 5,
    'key': api_key,
}

response = requests.get(url, params=params)
data = response.json()

if 'error' in data:
    print(f"API Error: {data['error']['message']}")
else:
    print(f"Found {len(data.get('items', []))} videos:\n")
    for item in data.get('items', []):
        title = item['snippet']['title']
        channel = item['snippet']['channelTitle']
        video_id = item['id']['videoId']
        print(f"- {title}")
        print(f"  Channel: {channel}")
        print(f"  URL: https://youtube.com/watch?v={video_id}\n")