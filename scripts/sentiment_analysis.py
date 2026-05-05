"""VADER sentiment scoring on unified corpus."""
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

corpus = pd.read_csv('data/clean/corpus.csv')
analyzer = SentimentIntensityAnalyzer()

def score_text(text):
    if not isinstance(text, str):
        return None
    return analyzer.polarity_scores(text)['compound']

corpus['sentiment'] = corpus['text'].apply(score_text)

# Categorize
def label(score):
    if score is None:
        return 'unknown'
    if score >= 0.05:
        return 'positive'
    if score <= -0.05:
        return 'negative'
    return 'neutral'

corpus['sentiment_label'] = corpus['sentiment'].apply(label)

corpus.to_csv('data/clean/corpus_scored.csv', index=False)

# Aggregate per chain
print("Mean sentiment per chain:")
print(corpus.groupby('chain')['sentiment'].agg(['mean', 'median', 'count']).round(3))

print("\nSentiment distribution per chain (%):")
dist = corpus.groupby('chain')['sentiment_label'].value_counts(normalize=True).unstack(fill_value=0) * 100
print(dist.round(1))

print("\nWeighted by engagement (likes/upvotes):")
def weighted(g):
    if g['engagement'].sum() > 0:
        return (g['sentiment'] * g['engagement']).sum() / g['engagement'].sum()
    return g['sentiment'].mean()

print(corpus.groupby('chain').apply(weighted, include_groups=False).round(3))