"""LDA topic modeling per chain — improved cleaning."""
import pandas as pd
import re
import html
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

corpus = pd.read_csv('data/clean/corpus_scored.csv')

EXTRA_STOPS = {
    'walmart', 'costco', 'publix', 'aldi', 'trader', 'joes', 'joe', 'tj', 'tjs',
    'amp', 'http', 'https', 'www', 'com', 'org', 'gt', 'lt', 'br', 'quot', 'href',
    'store', 'stores', 'shopping', 'grocery', 'groceries',
    'video', 'videos', 'haul', 'hauls', 'channel', 'subscribe', 'thanks', 'thank',
    'love', 'loved', 'loves', 'great', 'good', 'best', 'awesome', 'nice',
    'people', 'really', 'just', 'like', 'know', 'think', 'going', 'want',
    've', 'don', 'didn', 'doesn', 'isn', 'wasn', 'won', 'haven', 'hadn',
    'll', 'ye', 'ya', 'lol', 'omg', 'oh', 'yeah', 'yes', 'yep', 'nope',
    'today', 'tomorrow', 'yesterday', 'week', 'month', 'year', 'time',
    'subreddit', 'post', 'posts', 'comment', 'descriptive', 'message', 'questions',
    'titles', 'mods', 'moderator', 'rule', 'rules', 'flair',
    'happy', 'birthday', 'gerry', 'kayla', 'bobby', 'calvin',
    'star', 'fetch', 'sharing',
    'make', 'makes', 'made', 'use', 'using', 'used', 'try', 'trying', 'tried',
    'buy', 'bought', 'buying', 'get', 'got', 'getting', 'go', 'goes', 'going',
    'day', 'days', 'said', 'say', 'says', 'looks', 'look', 'looking',
    'better', 'home', 'work', 'actually', 'feel', 'hope', 'wish', 'soon',
    'soon', 'pay', 'paid', 'paying',
    'red', 'new', 'old', 'big', 'little',
}

def clean_text(t):
    if not isinstance(t, str):
        return ''
    t = html.unescape(t)
    t = re.sub(r'<[^>]+>', ' ', t)
    t = re.sub(r'http\S+', '', t)
    t = re.sub(r'&\w+;', ' ', t)
    t = re.sub(r'[^a-zA-Z\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t)
    return t.lower().strip()

def get_topics(texts, n_topics=5, n_words=8):
    cleaned = [clean_text(t) for t in texts]
    cleaned = [t for t in cleaned if len(t.split()) > 3]
    
    if len(cleaned) < 20:
        return []
    
    vectorizer = CountVectorizer(
        max_df=0.7, min_df=8, stop_words='english',
        ngram_range=(1, 2), max_features=2000
    )
    try:
        dtm = vectorizer.fit_transform(cleaned)
    except ValueError:
        return []
    
    feature_names = vectorizer.get_feature_names_out()
    keep_idx = [i for i, w in enumerate(feature_names) 
                if not any(s in EXTRA_STOPS for s in w.split())]
    
    if not keep_idx:
        return []
    
    lda = LatentDirichletAllocation(
        n_components=n_topics, random_state=42, 
        max_iter=30, learning_method='batch'
    )
    lda.fit(dtm)
    
    topics = []
    for comp in lda.components_:
        valid = [(comp[j], feature_names[j]) for j in keep_idx]
        valid.sort(reverse=True)
        top_words = [w for _, w in valid[:n_words]]
        topics.append(top_words)
    return topics

results = {}
for chain in corpus['chain'].unique():
    print(f"\n{'='*60}")
    print(f"CHAIN: {chain.upper()}")
    print(f"{'='*60}")
    texts = corpus[corpus['chain'] == chain]['text'].tolist()
    print(f"Documents: {len(texts)}\n")
    topics = get_topics(texts, n_topics=5)
    results[chain] = topics
    for i, words in enumerate(topics):
        print(f"Topic {i+1}: {', '.join(words)}")

# Save topics to file
import json
with open('data/clean/topics.json', 'w') as f:
    json.dump(results, f, indent=2)

print("\nSaved to data/clean/topics.json")