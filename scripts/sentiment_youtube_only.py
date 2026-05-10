import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from scipy import stats

corpus = pd.read_csv('data/clean/corpus_scored.csv')
yt = corpus[corpus['source'] == 'youtube'].copy()

print("YouTube-only sentiment per chain:")
print(yt.groupby('chain')['sentiment'].agg(['mean', 'median', 'count']).round(3))

print("\nNegative % per chain (YouTube only):")
neg = yt.groupby('chain').apply(
    lambda x: (x['sentiment_label'] == 'negative').sum() / len(x) * 100,
    include_groups=False
).round(1)
print(neg)

print("\nT-tests: TJ vs each chain (YouTube only):")
tj = yt[yt['chain'] == 'trader_joes']['sentiment'].dropna()
for chain in yt['chain'].unique():
    if chain == 'trader_joes':
        continue
    other = yt[yt['chain'] == chain]['sentiment'].dropna()
    t, p = stats.ttest_ind(tj, other, equal_var=False)
    sig = "✓" if p < 0.05 else "✗"
    print(f"  TJ ({tj.mean():.3f}) vs {chain} ({other.mean():.3f}): t={t:.2f}, p={p:.4f} {sig}")