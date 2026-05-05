"""YouTube comment scraper for grocery chain sentiment analysis."""
import os
import time
import csv
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')

SEARCHES = {
    'trader_joes': 'trader joes haul',
    'walmart': 'walmart grocery haul',
    'costco': 'costco haul',
    'aldi': 'aldi haul',
    'publix': 'publix haul',
}

MAX_VIDEOS = 15
MAX_COMMENTS = 100


def search_videos(query, max_results=MAX_VIDEOS):
    url = 'https://www.googleapis.com/youtube/v3/search'
    params = {
        'part': 'snippet',
        'q': query,
        'type': 'video',
        'maxResults': max_results,
        'key': API_KEY,
    }
    r = requests.get(url, params=params).json()
    videos = []
    for item in r.get('items', []):
        videos.append({
            'video_id': item['id']['videoId'],
            'title': item['snippet']['title'],
            'channel': item['snippet']['channelTitle'],
        })
    return videos


def get_comments(video_id, max_comments=MAX_COMMENTS):
    url = 'https://www.googleapis.com/youtube/v3/commentThreads'
    params = {
        'part': 'snippet',
        'videoId': video_id,
        'maxResults': 100,
        'order': 'relevance',
        'key': API_KEY,
    }
    comments = []
    while len(comments) < max_comments:
        r = requests.get(url, params=params).json()
        if 'error' in r:
            break
        for item in r.get('items', []):
            text = item['snippet']['topLevelComment']['snippet']['textDisplay']
            likes = item['snippet']['topLevelComment']['snippet']['likeCount']
            comments.append({'text': text, 'likes': likes})
            if len(comments) >= max_comments:
                break
        next_page = r.get('nextPageToken')
        if not next_page:
            break
        params['pageToken'] = next_page
    return comments


def main():
    os.makedirs('data/raw', exist_ok=True)
    output_file = 'data/raw/youtube_comments.csv'

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['chain', 'video_id', 'title', 'channel', 'comment', 'likes'])
        writer.writeheader()

        for chain, query in SEARCHES.items():
            print(f"\nScraping: {chain}")
            videos = search_videos(query)
            print(f"  Found {len(videos)} videos")

            for v in videos:
                print(f"  Fetching comments: {v['title'][:50]}")
                comments = get_comments(v['video_id'])
                print(f"    Got {len(comments)} comments")

                for c in comments:
                    writer.writerow({
                        'chain': chain,
                        'video_id': v['video_id'],
                        'title': v['title'],
                        'channel': v['channel'],
                        'comment': c['text'],
                        'likes': c['likes'],
                    })
                time.sleep(0.5)  # avoid rate limits

    print(f"\nDone. Saved to {output_file}")


if __name__ == '__main__':
    main()