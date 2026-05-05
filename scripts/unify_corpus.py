"""Unify Reddit + YouTube data into single text corpus."""
import pandas as pd
import os

os.makedirs('data/clean', exist_ok=True)

# Load Reddit
reddit = pd.read_csv('data/raw/reddit_final.csv')

# Load YouTube
youtube = pd.read_csv('data/raw/youtube_comments.csv')

# Normalize Reddit
chain_map_reddit = {
    'traderjoes': 'trader_joes',
    'walmart': 'walmart',
    'Costco': 'costco',
    'publix': 'publix',
    'aldi': 'aldi',
}

reddit['chain'] = reddit['parsedCommunityName'].str.lower().map({
    'traderjoes': 'trader_joes',
    'walmart': 'walmart',
    'costco': 'costco',
    'publix': 'publix',
    'aldi': 'aldi',
})

# Forward-fill chain so comments inherit parent post's chain
reddit['chain'] = reddit['chain'].ffill()

# Combine title + body for posts
reddit['text'] = reddit['title'].fillna('') + ' ' + reddit['body'].fillna('')
reddit['text'] = reddit['text'].str.strip()

reddit_clean = reddit[['chain', 'text', 'upVotes', 'createdAt']].copy()
reddit_clean.columns = ['chain', 'text', 'engagement', 'date']
reddit_clean['source'] = 'reddit'

# Normalize YouTube
youtube_clean = youtube[['chain', 'comment', 'likes']].copy()
youtube_clean.columns = ['chain', 'text', 'engagement']
youtube_clean['date'] = None
youtube_clean['source'] = 'youtube'

# Combine
corpus = pd.concat([reddit_clean, youtube_clean], ignore_index=True)

# Clean: drop empty text, deleted/removed
corpus = corpus[corpus['text'].notna()]
corpus = corpus[corpus['text'].str.len() > 5]
corpus = corpus[~corpus['text'].isin(['[deleted]', '[removed]'])]
corpus = corpus[corpus['chain'].notna()]

corpus.to_csv('data/clean/corpus.csv', index=False)

print(f"Total documents: {len(corpus)}")
print(f"\nBy source:\n{corpus['source'].value_counts()}")
print(f"\nBy chain:\n{corpus['chain'].value_counts()}")
print(f"\nSample:\n{corpus.head(3)}")