"""Extract location mentions from Reddit + YouTube data to find TJ demand signals."""
import pandas as pd
import re
from collections import Counter

corpus = pd.read_csv('data/clean/corpus_scored.csv')
tj_corpus = corpus[corpus['chain'] == 'trader_joes']

# State + city names to scan for
us_states = [
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
    'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
    'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
    'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
    'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new hampshire', 'new jersey', 'new mexico', 'new york',
    'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon',
    'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
    'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
    'west virginia', 'wisconsin', 'wyoming'
]

# Major cities (subset)
us_cities = [
    'austin', 'dallas', 'houston', 'san antonio',  # TX
    'miami', 'orlando', 'tampa', 'jacksonville',   # FL
    'atlanta', 'savannah',                          # GA
    'nashville', 'memphis',                         # TN
    'charlotte', 'raleigh',                         # NC
    'cleveland', 'columbus', 'cincinnati',          # OH
    'detroit', 'grand rapids',                      # MI
    'indianapolis',                                 # IN
    'milwaukee', 'madison',                         # WI
    'kansas city', 'st louis',                      # KS/MO
    'oklahoma city', 'tulsa',                       # OK
    'salt lake city',                               # UT
    'new orleans', 'baton rouge',                   # LA
    'little rock',                                  # AR
    'birmingham', 'huntsville',                     # AL
]

# Demand-signal phrases
demand_phrases = [
    'wish', 'need', 'want', 'come to', 'open in', 'no tj in',
    'nearest tj', 'closest tj', 'drive to', 'hour to', 'miles to',
    'theres no', "there's no", 'we dont have', "we don't have"
]

state_mentions = Counter()
city_mentions = Counter()
demand_mentions = Counter()

for text in tj_corpus['text'].dropna():
    t = text.lower()
    
    for state in us_states:
        if re.search(rf'\b{re.escape(state)}\b', t):
            state_mentions[state.title()] += 1
    
    for city in us_cities:
        if re.search(rf'\b{re.escape(city)}\b', t):
            city_mentions[city.title()] += 1
    
    # Demand signals
    for phrase in demand_phrases:
        if phrase in t:
            # Find which state/city is nearby
            for state in us_states:
                if state in t:
                    demand_mentions[state.title()] += 1
                    break

print("Top 20 states mentioned in TJ corpus:")
for state, count in state_mentions.most_common(20):
    print(f"  {state}: {count}")

print("\nTop 15 cities mentioned (non-California focus):")
for city, count in city_mentions.most_common(15):
    print(f"  {city}: {count}")

print("\nTop demand-signal states (mentions with 'wish', 'need', 'no TJ', etc):")
for state, count in demand_mentions.most_common(15):
    print(f"  {state}: {count}")