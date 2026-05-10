"""Scrape H-E-B YouTube comments only — append to existing CSV."""
import os
import csv
import time
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')

QUERY = 'HEB grocery haul'
CHAIN = 'heb'
MAX_VIDEOS = 15
MAX_COMMENTS = 100


def search_videos(query, max_results=MAX_VIDEOS):
    url = 'https://www.googleapis.com/youtube/v3/search'
    params = {'part': 'snippet', 'q': query, 'type': 'video',
              'maxResults': max_results, 'key': API_KEY}
    r = requests.get(url, params=params).json()
    return [{'video_id': i['id']['videoId'], 'title': i['snippet']['title'],
             'channel': i['snippet']['channelTitle']} for i in r.get('items', [])]


def get_comments(video_id, max_comments=MAX_COMMENTS):
    url = 'https://www.googleapis.com/youtube/v3/commentThreads'
    params = {'part': 'snippet', 'videoId': video_id, 'maxResults': 100,
              'order': 'relevance', 'key': API_KEY}
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
    output_file = 'data/raw/youtube_comments.csv'
    
    print(f"Scraping: {CHAIN}")
    videos = search_videos(QUERY)
    print(f"  Found {len(videos)} videos")
    
    rows = []
    for v in videos:
        print(f"  {v['title'][:50]}")
        comments = get_comments(v['video_id'])
        print(f"    {len(comments)} comments")
        for c in comments:
            rows.append({
                'chain': CHAIN, 'video_id': v['video_id'],
                'title': v['title'], 'channel': v['channel'],
                'comment': c['text'], 'likes': c['likes'],
            })
        time.sleep(0.5)
    
    file_exists = os.path.exists(output_file)
    with open(output_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['chain', 'video_id', 'title',
                                                 'channel', 'comment', 'likes'])
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    
    print(f"\nAdded {len(rows)} H-E-B comments to {output_file}")


if __name__ == '__main__':
    main()