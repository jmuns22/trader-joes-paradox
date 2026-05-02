"""
Reddit Scraper — Trader Joe's Paradox Project
Phase 2: Sentiment Teardown

Pulls top posts + top-level comments from grocery subreddits over the last 12 months.
Saves raw data to data/raw/reddit_posts.csv and data/raw/reddit_comments.csv.

Usage:
    python scripts/reddit_scraper.py

Requires .env with:
    REDDIT_CLIENT_ID
    REDDIT_CLIENT_SECRET
    REDDIT_USER_AGENT
"""

import os
import time
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import praw
import pandas as pd
from dotenv import load_dotenv

# ---------- Configuration ----------

SUBREDDITS = [
    'traderjoes',
    'walmart',
    'Costco',
    'Publix',
    'aldi',
]

POSTS_PER_SUB = 1000          # Reddit API hard cap on listings is ~1000
COMMENTS_PER_POST = 50        # Top-level comments to grab per post
TIME_FILTER = 'year'          # 'all', 'year', 'month', 'week', 'day'
SORT_BY = 'top'               # 'top', 'hot', 'new'

OUTPUT_DIR = Path('data/raw')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

POSTS_FILE = OUTPUT_DIR / 'reddit_posts.csv'
COMMENTS_FILE = OUTPUT_DIR / 'reddit_comments.csv'
LOG_FILE = OUTPUT_DIR / 'scraper_log.txt'

# ---------- Logging Setup ----------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


# ---------- Reddit Client ----------

def get_reddit_client():
    """Authenticate with Reddit via PRAW using credentials from .env."""
    load_dotenv()
    
    client_id = os.getenv('REDDIT_CLIENT_ID')
    client_secret = os.getenv('REDDIT_CLIENT_SECRET')
    user_agent = os.getenv('REDDIT_USER_AGENT')
    
    if not all([client_id, client_secret, user_agent]):
        raise ValueError(
            "Missing Reddit credentials. Check .env file has "
            "REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT."
        )
    
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )
    
    log.info(f"Authenticated as read-only client. User agent: {user_agent}")
    return reddit


# ---------- Scraping Functions ----------

def scrape_subreddit(reddit, sub_name, posts_data, comments_data):
    """Pull top posts + top-level comments from one subreddit."""
    log.info(f"Scraping r/{sub_name}...")
    subreddit = reddit.subreddit(sub_name)
    
    post_count = 0
    comment_count = 0
    
    try:
        for post in subreddit.top(time_filter=TIME_FILTER, limit=POSTS_PER_SUB):
            # Skip stickied/moderator posts
            if post.stickied:
                continue
            
            posts_data.append({
                'subreddit': sub_name,
                'post_id': post.id,
                'title': post.title,
                'body': post.selftext,
                'score': post.score,
                'num_comments': post.num_comments,
                'upvote_ratio': post.upvote_ratio,
                'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                'author': str(post.author) if post.author else '[deleted]',
                'url': post.url,
                'permalink': f"https://reddit.com{post.permalink}",
                'flair': post.link_flair_text,
            })
            post_count += 1
            
            # Pull top-level comments
            try:
                post.comments.replace_more(limit=0)  # skip "load more" stubs
                top_comments = post.comments[:COMMENTS_PER_POST]
                
                for comment in top_comments:
                    if not hasattr(comment, 'body'):
                        continue
                    if comment.body in ('[deleted]', '[removed]'):
                        continue
                    
                    comments_data.append({
                        'subreddit': sub_name,
                        'post_id': post.id,
                        'comment_id': comment.id,
                        'body': comment.body,
                        'score': comment.score,
                        'created_utc': datetime.fromtimestamp(comment.created_utc).isoformat(),
                        'author': str(comment.author) if comment.author else '[deleted]',
                        'is_submitter': comment.is_submitter,
                    })
                    comment_count += 1
            
            except Exception as e:
                log.warning(f"Comment fetch failed for {post.id}: {e}")
            
            # Progress checkpoint every 100 posts
            if post_count % 100 == 0:
                log.info(f"  r/{sub_name}: {post_count} posts, {comment_count} comments so far")
            
            # Light rate limiting (PRAW handles most of it, this is belt-and-suspenders)
            time.sleep(0.1)
    
    except Exception as e:
        log.error(f"Subreddit scrape failed for r/{sub_name}: {e}")
    
    log.info(f"Done r/{sub_name}: {post_count} posts, {comment_count} comments")
    return post_count, comment_count


# ---------- Main ----------

def main():
    log.info("=" * 60)
    log.info("Reddit scraper started")
    log.info(f"Targets: {SUBREDDITS}")
    log.info(f"Posts/sub cap: {POSTS_PER_SUB} | Comments/post cap: {COMMENTS_PER_POST}")
    log.info("=" * 60)
    
    reddit = get_reddit_client()
    
    posts_data = []
    comments_data = []
    
    summary = {}
    
    for sub_name in SUBREDDITS:
        p_count, c_count = scrape_subreddit(reddit, sub_name, posts_data, comments_data)
        summary[sub_name] = {'posts': p_count, 'comments': c_count}
        time.sleep(2)  # courtesy pause between subs
    
    # Save to CSV
    posts_df = pd.DataFrame(posts_data)
    comments_df = pd.DataFrame(comments_data)
    
    posts_df.to_csv(POSTS_FILE, index=False, encoding='utf-8')
    comments_df.to_csv(COMMENTS_FILE, index=False, encoding='utf-8')
    
    log.info("=" * 60)
    log.info("Scrape complete")
    log.info(f"Posts saved to: {POSTS_FILE} ({len(posts_df)} rows)")
    log.info(f"Comments saved to: {COMMENTS_FILE} ({len(comments_df)} rows)")
    log.info("Summary:")
    for sub, counts in summary.items():
        log.info(f"  r/{sub}: {counts['posts']} posts, {counts['comments']} comments")
    log.info("=" * 60)


if __name__ == '__main__':
    main()
