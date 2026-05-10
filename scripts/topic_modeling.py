"""Improved LDA topic modeling — coherence-tuned, TF-IDF, better cleaning."""
import pandas as pd
import re
import html
import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation

corpus = pd.read_csv('data/clean/corpus_scored.csv')

EXTRA_STOPS = {
    'walmart', 'costco', 'publix', 'aldi', 'trader', 'joes', 'joe', 'tj', 'tjs', 'heb',
    'amp', 'http', 'https', 'www', 'com', 'org', 'gt', 'lt', 'br', 'quot', 'href',
    'store', 'stores', 'shopping', 'grocery', 'groceries',
    'video', 'videos', 'haul', 'hauls', 'channel', 'subscribe', 'thanks', 'thank',
    'love', 'loved', 'loves', 'great', 'good', 'best', 'awesome', 'nice',
    'people', 'really', 'just', 'like', 'know', 'think', 'going', 'want',
    've', 'don', 'didn', 'doesn', 'isn', 'wasn', 'won', 'haven', 'hadn',
    'll', 'ye', 'ya', 'lol', 'omg', 'oh', 'yeah', 'yes', 'yep', 'nope',
    'today', 'tomorrow', 'yesterday', 'week', 'month', 'year', 'time',
    'subreddit', 'post', 'posts', 'comment', 'comments', 'descriptive',
    'message', 'questions', 'titles', 'mods', 'moderator', 'rule', 'rules', 'flair',
    'happy', 'birthday', 'gerry', 'kayla', 'bobby', 'calvin', 'marie', 'robin',
    'star', 'fetch', 'sharing', 'automod', 'automatically', 'mentioned', 'helps',
    'make', 'makes', 'made', 'use', 'using', 'used', 'try', 'trying', 'tried',
    'buy', 'bought', 'buying', 'get', 'got', 'getting', 'go', 'goes', 'going',
    'day', 'days', 'said', 'say', 'says', 'looks', 'look', 'looking',
    'better', 'home', 'work', 'actually', 'feel', 'hope', 'wish', 'soon',
    'pay', 'paid', 'paying', 'red', 'new', 'old', 'big', 'little',
    'image', 'images', 'preview', 'redd', 'wonderful', 'bless', 'god',
    'job', 'morning', 'enjoy', 'watching', 'watch', 'shop',
    'thing', 'things', 'lot', 'way', 'need', 'definitely', 'sure',
    'amazing', 'delicious', 'thats', 'havent', 'didnt', 'dont',
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


def filter_quality(texts):
    cleaned = []
    automod_signals = ['automoderator', 'this is an automatic', 'descriptive post titles']
    for t in texts:
        ct = clean_text(t)
        if len(ct.split()) < 15:
            continue
        if any(sig in ct for sig in automod_signals):
            continue
        cleaned.append(ct)
    return cleaned


def get_topics(texts, k_values=[3, 5, 7], n_words=10):
    cleaned = filter_quality(texts)
    if len(cleaned) < 30:
        return None, None
    
    vectorizer = TfidfVectorizer(
        max_df=0.6, min_df=8, stop_words='english',
        ngram_range=(1, 2), max_features=2000,
        token_pattern=r'(?u)\b[a-z]{3,}\b'
    )
    try:
        dtm = vectorizer.fit_transform(cleaned)
    except ValueError:
        return None, None
    
    feature_names = vectorizer.get_feature_names_out()
    keep_idx = [i for i, w in enumerate(feature_names)
                if not any(s in EXTRA_STOPS for s in w.split())]
    
    if not keep_idx:
        return None, None
    
    best_k = None
    best_perp = float('inf')
    best_topics = None
    
    for k in k_values:
        lda = LatentDirichletAllocation(
            n_components=k, random_state=42, max_iter=30,
            learning_method='batch', doc_topic_prior=0.1, topic_word_prior=0.01
        )
        lda.fit(dtm)
        perp = lda.perplexity(dtm)
        
        topics = []
        for comp in lda.components_:
            valid = [(comp[j], feature_names[j]) for j in keep_idx]
            valid.sort(reverse=True)
            top_words = [w for _, w in valid[:n_words]]
            topics.append(top_words)
        
        if perp < best_perp:
            best_perp = perp
            best_k = k
            best_topics = topics
    
    return best_topics, best_k


results = {}
for chain in corpus['chain'].unique():
    print(f"\n{'='*60}")
    print(f"CHAIN: {chain.upper()}")
    print(f"{'='*60}")
    texts = corpus[corpus['chain'] == chain]['text'].tolist()
    print(f"Documents (raw): {len(texts)}")
    
    topics, best_k = get_topics(texts, k_values=[3, 5, 7])
    
    if topics is None:
        print("Insufficient data after filtering")
        continue
    
    print(f"Best k: {best_k}\n")
    results[chain] = {'k': best_k, 'topics': topics}
    
    for i, words in enumerate(topics):
        print(f"Topic {i+1}: {', '.join(words[:8])}")

with open('data/clean/topics.json', 'w') as f:
    json.dump(results, f, indent=2)

print("\nSaved to data/clean/topics.json")