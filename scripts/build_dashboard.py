"""Build interactive HTML dashboard combining all phase results."""
import pandas as pd
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

# Load all data
kpi = pd.read_csv('data/kpi_raw.csv')
corpus = pd.read_csv('data/clean/corpus_scored.csv')
prices = pd.read_csv('data/pricing_raw.csv')
ops = pd.read_csv('data/operational_metrics.csv')
expansion = pd.read_csv('data/expansion_data.csv')
trends = pd.read_csv('data/google_trends_state.csv')

with open('data/clean/topics.json') as f:
    topics = json.load(f)

# Merge expansion + trends
expansion = expansion.merge(trends, on='state', how='left')
expansion['trends_score'] = expansion['trends_score'].fillna(20)
expansion['demo_propensity'] = (
    (expansion['median_income_k'] / expansion['median_income_k'].max()) * 40 +
    (expansion['bachelor_pct'] / expansion['bachelor_pct'].max()) * 40 +
    (expansion['urban_pct'] / expansion['urban_pct'].max()) * 20
)
expansion['refined_propensity'] = expansion['demo_propensity'] * 0.6 + expansion['trends_score'] * 0.4
expansion['stores_per_m'] = expansion['tj_stores'] / expansion['population_m']
expansion['expected_stores_per_m'] = expansion['refined_propensity'] / expansion['refined_propensity'].max() * expansion['stores_per_m'].max()
expansion['additional_stores'] = ((expansion['expected_stores_per_m'] - expansion['stores_per_m']) * expansion['population_m']).round().clip(lower=0).astype(int)

# Helper: TJ-highlight color scheme
def get_colors(items, highlight='trader_joes', high_color='#B22222', other_color='#808080'):
    return [high_color if str(item).lower().replace(' ', '_').replace("'", '').replace("'", '') == highlight else other_color for item in items]

# ===== CHART 1: Sales per Sq Ft =====
kpi_sorted = kpi.sort_values('sales_per_sqft_usd', ascending=True)
fig1 = go.Figure(go.Bar(
    x=kpi_sorted['sales_per_sqft_usd'],
    y=kpi_sorted['chain'],
    orientation='h',
    marker=dict(color=['#B22222' if c == 'Trader Joes' else '#808080' for c in kpi_sorted['chain']]),
    text=[f'${v:,.0f}' for v in kpi_sorted['sales_per_sqft_usd']],
    textposition='outside',
))
fig1.update_layout(
    title="<b>Sales per Square Foot</b> — Trader Joe's Dominates",
    xaxis_title="USD per sqft",
    height=400, template='plotly_white'
)

# ===== CHART 2: Sentiment per chain (FULL CORPUS — Reddit + YouTube + TikTok) =====
sent = corpus.groupby('chain')['sentiment'].mean().sort_values()
fig2 = go.Figure(go.Bar(
    x=sent.values,
    y=sent.index,
    orientation='h',
    marker=dict(color=['#B22222' if c == 'trader_joes' else '#808080' for c in sent.index]),
    text=[f'{v:.3f}' for v in sent.values],
    textposition='outside',
))
fig2.update_layout(
    title="<b>Mean Sentiment Across Platforms</b> — TJ Mid-Pack on Volume, #1 on ACSI",
    xaxis_title="VADER compound score",
    height=400, template='plotly_white'
)

# ===== CHART 3: Pricing bifurcation =====
chains_p = ['trader_joes', 'walmart', 'aldi', 'publix', 'costco']
for chain in chains_p:
    prices[f'{chain}_idx'] = (prices[chain] / prices['walmart']) * 100
cat_idx = prices.groupby('category')[[f'{c}_idx' for c in chains_p]].mean().sort_values('trader_joes_idx')

fig3 = go.Figure(go.Bar(
    x=cat_idx.index,
    y=cat_idx['trader_joes_idx'],
    marker=dict(color=['#2E8B57' if v < 100 else '#B22222' for v in cat_idx['trader_joes_idx']]),
    text=[f'{v:.0f}' for v in cat_idx['trader_joes_idx']],
    textposition='outside',
))
fig3.add_hline(y=100, line_dash='dash', line_color='black', annotation_text='Walmart baseline')
fig3.update_layout(
    title="<b>TJ's Strategic Pricing</b> — Cheap on Curated, Premium on Commodities",
    xaxis_title="Category",
    yaxis_title="Price Index (Walmart=100)",
    height=400, template='plotly_white'
)

# ===== CHART 4: Inventory turns vs margin =====
fig4 = go.Figure(go.Scatter(
    x=ops['inv_turns'],
    y=ops['op_margin_pct'],
    mode='markers+text',
    marker=dict(
        size=ops['private_label_pct'] * 1.5,
        color=['#B22222' if c == 'trader_joes' else '#4A90E2' for c in ops['chain']],
        line=dict(width=1, color='black')
    ),
    text=ops['chain'].str.replace('_', ' ').str.title(),
    textposition='top center',
))
fig4.update_layout(
    title="<b>Inventory Turns vs Margin</b> (bubble size = private label %)",
    xaxis_title="Inventory Turns per Year",
    yaxis_title="Operating Margin (%)",
    height=400, template='plotly_white'
)

# ===== CHART 5: Expansion opportunity =====
top_exp = expansion.nlargest(15, 'additional_stores').sort_values('additional_stores')
fig5 = go.Figure(go.Bar(
    x=top_exp['additional_stores'],
    y=top_exp['state'],
    orientation='h',
    marker=dict(color='#B22222'),
    text=[f'+{int(v)}' for v in top_exp['additional_stores']],
    textposition='outside',
))
fig5.update_layout(
    title="<b>Top Expansion Targets</b> — Where Demographics & Search Demand Both Align",
    xaxis_title="Additional Store Potential",
    height=500, template='plotly_white'
)

# ===== CHART 6: Total revenue gap =====
fig6 = go.Figure()
fig6.add_trace(go.Bar(
    x=kpi['chain'],
    y=kpi['revenue_usd_b'],
    name='Total Revenue',
    marker_color='lightblue',
))
fig6.add_trace(go.Bar(
    x=kpi['chain'],
    y=kpi['grocery_revenue_usd_b'],
    name='Grocery Revenue',
    marker_color='#B22222',
))
fig6.update_layout(
    title="<b>Revenue Reality</b> — Rivals Earn from Non-Grocery Streams TJ Refuses",
    yaxis_title="USD (Billions)",
    barmode='overlay',
    height=400, template='plotly_white'
)

# ===== Build HTML =====
html_parts = [
    "<!DOCTYPE html>",
    "<html><head><title>The Trader Joe's Paradox</title>",
    "<style>",
    "@font-face { font-family: 'TraderJoes'; src: url('fonts/TraderJoes-JYrx.otf') format('opentype'); font-weight: normal; font-style: normal; }",
    "body { font-family: Georgia, 'Times New Roman', serif; max-width: 1200px; margin: 40px auto; padding: 24px 32px; background-color: #F5EFE0; background-image: url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='300' height='300'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3CfeColorMatrix type='saturate' values='0'/%3E%3C/filter%3E%3Crect width='300' height='300' filter='url(%23n)' opacity='0.04'/%3E%3C/svg%3E\"); color: #2C1810; }",
    "h1 { font-family: 'TraderJoes', Georgia, serif; font-size: 2.8em; color: #B22222; border-bottom: 2px solid #D4C5A9; padding-bottom: 12px; margin-bottom: 4px; }",
    "h2 { font-family: Georgia, 'Times New Roman', serif; font-size: 1.55rem; font-weight: bold; color: #2C1810; margin-top: 68px; margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid #D4C5A9; }",
    ".thesis { background: #FDF3EC; padding: 20px 24px; border-left: 5px solid #B22222; margin: 24px 0; font-size: 1.05em; font-family: Georgia, serif; color: #2C1810; line-height: 1.7; border-radius: 0 4px 4px 0; }",
    ".thesis b { font-family: 'TraderJoes', Georgia, serif; font-weight: normal; font-size: 1.05em; }",
    ".finding { background: #FDFAF4; padding: 16px 20px; margin: 14px 0; border-radius: 4px; border-left: 4px solid #4A7C59; color: #2C1810; font-family: Georgia, serif; font-size: 0.97em; line-height: 1.6; box-shadow: 1px 2px 6px rgba(44,24,16,0.05); }",
    ".chart-container { background: #FDFAF4; padding: 20px 20px 8px 20px; margin: 24px 0; border-radius: 4px; border: 1px solid #D4C5A9; box-shadow: 2px 3px 10px rgba(44,24,16,0.07); }",
    ".kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin: 28px 0; }",
    ".kpi-card { background: #FDFAF4; padding: 24px 16px; border-radius: 4px; text-align: center; border: 1px solid #D4C5A9; border-top: 4px solid #B22222; box-shadow: 2px 3px 8px rgba(44,24,16,0.08); }",
    ".kpi-value { font-family: Georgia, 'Times New Roman', serif; font-size: 2.2em; font-weight: bold; color: #B22222; }",
    ".kpi-label { color: #6B5D52; font-size: 0.82em; margin-top: 8px; font-family: Georgia, serif; line-height: 1.4; }",
    "footer { margin-top: 60px; padding: 20px; color: #6B5D52; border-top: 1px solid #D4C5A9; font-size: 0.88em; font-family: Georgia, serif; }",
    "</style></head><body>",
    "<h1>The Trader Joe's Paradox</h1>",
    "<p><i>Why does TJ win with fewer stores, fewer SKUs, and near-zero ad spend?</i></p>",
    "<div class='thesis'><b>Thesis:</b> Trader Joe's wins not on price or scale, but on cultural distinctness. Fewer SKUs → faster inventory turns → product-focused community → consistent satisfaction. Their 'flaws' (no online, no loyalty program, no ads) function as filters that attract aligned customers and repel everyone else.</div>",
    
    "<div class='kpi-grid'>",
    "<div class='kpi-card'><div class='kpi-value'>$1,900</div><div class='kpi-label'>TJ Sales per Sqft (vs $400 Walmart)</div></div>",
    "<div class='kpi-card'><div class='kpi-value'>4,000</div><div class='kpi-label'>TJ SKUs (vs 120,000 Walmart)</div></div>",
    "<div class='kpi-card'><div class='kpi-value'>80%</div><div class='kpi-label'>TJ Private Label Share</div></div>",
    "<div class='kpi-card'><div class='kpi-value'>14x/yr</div><div class='kpi-label'>TJ Inventory Turns (industry-leading)</div></div>",
    "</div>",
    
    "<h2>Phase 1: The Paradox in Numbers</h2>",
    "<div class='finding'>TJ generates $1,900/sqft — 4.7x Walmart's $400. With stores 1/15 the size, TJ punches above its weight.</div>",
    "<div class='chart-container'>",
    fig1.to_html(include_plotlyjs='cdn', full_html=False, div_id='chart1'),
    "</div>",
    "<div class='chart-container'>",
    fig6.to_html(include_plotlyjs=False, full_html=False, div_id='chart6'),
    "</div>",
    
   "<h2>Phase 2: Community Voice</h2>",
    "<div class='finding'>Across 8,321 documents from Reddit + YouTube + TikTok, TJ ranks 5th of 6 chains on raw sentiment — HEB leads. But TJ wins ACSI #1 customer satisfaction. The disconnect IS the insight: TJ's loyalty is quiet, not loud. Topic modeling shows why: TJ's community uniquely centers on the products themselves, while rivals center on deals (Publix), bulk (Costco), or regional pride (HEB).</div>", "<div class='chart-container'>",
    fig2.to_html(include_plotlyjs=False, full_html=False, div_id='chart2'),
    "</div>",
    
    "<h2>Phase 3: Strategic Pricing</h2>",
    "<div class='finding'>TJ is 5% MORE expensive than Walmart on average. But strategically: cheaper on signature items (snacks, pantry), premium on commodities (produce, dairy). Pricing matches what their community cares about.</div>",
    "<div class='chart-container'>",
    fig3.to_html(include_plotlyjs=False, full_html=False, div_id='chart3'),
    "</div>",
    
    "<h2>Phase 4: Operational Moat</h2>",
    "<div class='finding'>Curation (fewer SKUs) → 14 inventory turns/year → less waste → sustainable margins. TJ matches Costco on SKU count but in 12x smaller stores.</div>",
    "<div class='chart-container'>",
    fig4.to_html(include_plotlyjs=False, full_html=False, div_id='chart4'),
    "</div>",
    
    "<h2>Phase 5: Expansion Opportunity</h2>",
    "<div class='finding'>Refined model (60% demographics + 40% Google search demand) identifies ~700 additional store opportunity. Texas demographics suggest huge expansion potential, but low search demand suggests latent demand or competitive overlap with HEB.</div>",
    "<div class='chart-container'>",
    fig5.to_html(include_plotlyjs=False, full_html=False, div_id='chart5'),
    "</div>",
    
   "<h2>Synthesis: Quiet Loyalty, Not Loud Advocacy</h2>",
    "<div class='finding'><b>Topic Modeling:</b> TJ community talks about products. Publix talks deals. Costco talks bulk. Aldi talks budget. HEB talks Texas. Each chain has a distinct cultural identity.</div>",
    "<div class='finding'><b>Pricing:</b> TJ is strategically priced — cheap on items the community discusses, premium on commodities they don't.</div>",
    "<div class='finding'><b>Operations:</b> Curation (4,000 SKUs, 80% private label) enables 14 inventory turns/year — industry-leading efficiency.</div>",
    "<div class='finding'><b>Sentiment vs Satisfaction:</b> TJ scores middle on raw sentiment (5th of 6) but #1 on ACSI. These measure different things. TJ has loyal-but-quiet customers; others have louder but less loyal ones.</div>",
    "<div class='thesis'><b>Conclusion:</b> The moat is operational + cultural, not sentiment. TJ wins long-term satisfaction without dominating short-term enthusiasm. Curation + cultural distinctness + strategic pricing + operational efficiency = sustainable competitive advantage that's structurally hard for rivals to replicate.</div>",
    "<footer>Built by jmuns22 · 8-day analysis project · Data: SEC 10-Ks, Reddit, YouTube, Google Trends, ACSI, Consumer Reports · <a href='https://github.com/jmuns22/trader-joes-paradox'>GitHub repo</a></footer>",
    "</body></html>"
]

with open('output/dashboard.html', 'w', encoding='utf-8') as f:
    f.write('\n'.join(html_parts))

print("Dashboard built: output/dashboard.html")
print("Open in browser to view.")