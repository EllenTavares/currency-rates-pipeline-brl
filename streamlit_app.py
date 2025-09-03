import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import unicodedata, re, json
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title="FX — Gold (BRL)", layout="wide")

GOLD_DIR = Path("data/gold")
PATTERN = "exchange_rates_brl_base_{}.parquet"

@st.cache_data
def list_dates():
    files = sorted(GOLD_DIR.glob(PATTERN.format("*")))
    return [p.stem.replace("exchange_rates_brl_base_", "") for p in files]

@st.cache_data
def load_gold(day: str) -> pd.DataFrame:
    p = GOLD_DIR / PATTERN.format(day)
    df = pd.read_parquet(p)[["currency", "rate_brl_base", "last_update_utc"]].drop_duplicates(subset=["currency"])
    return df.reset_index(drop=True)

def prev_day(days, current):
    if current not in days:
        return None
    i = days.index(current)
    return days[i-1] if i > 0 else None

def fmt_brl(x: float) -> str:
    return f"R$ {x:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")

def sanitize_md(text: str) -> str:
    t = unicodedata.normalize("NFKC", text)
    t = t.replace("\u200b","").replace("\u200c","").replace("\u200d","").replace("\ufeff","").replace("\u2060","").replace("\xa0"," ")
    t = re.sub(r"(?<=\w)_(?=\w)", " ", t)
    t = re.sub(r",\s+(\d)", r",\1", t)
    t = re.sub(r"R\s+\$", "R$ ", t)
    parts = re.split(r"\n\s*\n", t.strip())
    parts = [" ".join(p.strip().split()) for p in parts]
    return "\n\n".join(parts)

@st.cache_data
def history_for(currencies, days, last_n=15):
    days = days[-last_n:]
    rows = []
    for d in days:
        df = load_gold(d)
        for c in currencies:
            s = df[df["currency"] == c]
            if not s.empty:
                rows.append({"date": d, "currency": c, "value": float(s.iloc[0]["rate_brl_base"])})
    return pd.DataFrame(rows)

def sparkline(df: pd.DataFrame, ccy: str):
    d = df[df["currency"] == ccy].copy()
    if d.empty:
        return None
    d["ds"] = pd.to_datetime(d["date"])
    chart = alt.Chart(d).mark_area(opacity=0.35).encode(
        x=alt.X("ds:T", axis=None),
        y=alt.Y("value:Q", axis=None),
        tooltip=[alt.Tooltip("ds:T", title="Dia"), alt.Tooltip("value:Q", format=".4f", title="BRL")]
    ).properties(height=36)
    line = alt.Chart(d).mark_line().encode(x="ds:T", y="value:Q")
    return (chart + line).configure_area(color="#60a5fa").configure_line(color="#2563eb")

st.markdown("""
<style>
h1, h2, h3 { letter-spacing: .2px }
.hero { padding: 10px 14px; border-radius: 14px; background: linear-gradient(135deg,#0b1220 0%,#121a2b 60%,#1b2a4b 100%); border: 1px solid rgba(255,255,255,.06); margin-bottom: 18px }
.hero .title { font-size: 32px; font-weight: 800; margin: 0 }
.hero .sub { opacity: .8; margin-top: 2px }
.kpi { background: rgba(255,255,255,.03); padding: 14px; border-radius: 14px; border: 1px solid rgba(255,255,255,.06) }
.kpi .ccy { font-size: 14px; opacity:.8 }
.kpi .val { font-size: 28px; font-weight: 800; margin-top: 4px }
.badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; margin-top:6px }
.badge.up { background:rgba(34,197,94,.15); color:#22c55e }
.badge.down { background:rgba(239,68,68,.15); color:#ef4444 }
.section { margin-top: 10px }
.report-view .markdown-text-container p { text-align: justify; line-height: 1.5 }
</style>
""", unsafe_allow_html=True)

days = list_dates()
if not days:
    st.error("Nenhum arquivo em data/gold. Rode `python -m src.cli all`.")
    st.stop()

with st.container():
    colh1, colh2 = st.columns([3,2])
    with colh1:
        st.markdown(f'<div class="hero"><div class="title">FX — Gold (BRL)</div><div class="sub">Cotações diárias baseadas no Real (1 unidade de cada moeda em BRL)</div></div>', unsafe_allow_html=True)
    with colh2:
        day = st.selectbox("Dia", days, index=len(days)-1)

df = load_gold(day)
default_pick = [c for c in ["USD","EUR","BRL","GBP","JPY"] if c in set(df["currency"])]
pick = st.multiselect("Moedas", sorted(df["currency"].unique()), default_pick)

df_view = df[df["currency"].isin(pick)].drop_duplicates(subset=["currency"]).sort_values("currency").reset_index(drop=True)
if df_view.empty:
    st.warning("Selecione ao menos uma moeda.")
    st.stop()

pday = prev_day(days, day)
hist = history_for(pick, days, last_n=15)

kcols = st.columns(len(df_view) or 1)
if pday:
    prev = load_gold(pday)[["currency","rate_brl_base"]].rename(columns={"rate_brl_base":"prev"})
    kdf = df_view.merge(prev, on="currency", how="left")
else:
    kdf = df_view.copy()
    kdf["prev"] = np.nan

for c, r in zip(kcols, kdf.itertuples()):
    v = float(r.rate_brl_base)
    dlt = None if pd.isna(r.prev) or float(r.prev) == 0 else (v/float(r.prev)-1)*100
    html = f'<div class="kpi"><div class="ccy">{r.currency}</div><div class="val">{fmt_brl(v)}</div>'
    if dlt is not None:
        klass = "up" if dlt>=0 else "down"
        html += f'<div class="badge {klass}">{dlt:+.2f}%</div>'
    html += "</div>"
    c.markdown(html, unsafe_allow_html=True)
    sp = sparkline(hist, r.currency)
    if sp is not None:
        c.altair_chart(sp, use_container_width=True)

tab_overview, tab_detail, tab_summary = st.tabs(["Visão Geral", "Tabela/Download", "Resumo LLM"])

with tab_overview:
    left, right = st.columns([3,2])
    with left:
        compare_prev = pday is not None and st.toggle(f"Comparar com dia anterior ({pday})", value=True)
    with right:
        len_df = len(df_view)
        if len_df <= 3:
            topn = len_df
            st.caption("Poucas moedas selecionadas — exibindo todas no gráfico.")
        else:
            topn = st.slider(
                "Top N por valor (aplicado ao gráfico)",
                min_value=3,
                max_value=len_df,
                value=min(8, len_df)
            )
        logscale = st.toggle("Escala logarítmica", value=False)

    if compare_prev and pday:
        cmp = kdf.copy()
        cmp = cmp.nlargest(topn, "rate_brl_base")
        order = list(cmp.sort_values("rate_brl_base")["currency"])
        long = cmp.melt("currency", value_vars=["rate_brl_base","prev"], var_name="Dia", value_name="BRL").replace({"rate_brl_base": day, "prev": pday})
        enc_y = alt.Y("currency:N", sort=order, title="")
        enc_x = alt.X("BRL:Q", title="Valor (BRL)", scale=alt.Scale(type="log") if logscale else alt.Scale())
        base = alt.Chart(long).encode(
            y=enc_y,
            x=enc_x,
            color=alt.Color("Dia:N", sort=[pday, day], legend=alt.Legend(orient="bottom")),
            tooltip=["currency:N","Dia:N",alt.Tooltip("BRL:Q",format=".4f")]
        )
        bars = base.mark_bar(size=22)
        chart = bars.properties(height=320)
    else:
        dfo = df_view.copy().nlargest(topn, "rate_brl_base")
        dfo = dfo.sort_values("rate_brl_base")
        enc_y = alt.Y("currency:N", sort=list(dfo["currency"]), title="")
        enc_x = alt.X("rate_brl_base:Q", title="Valor (BRL)", scale=alt.Scale(type="log") if logscale else alt.Scale())
        bars = alt.Chart(dfo).mark_bar(size=22).encode(
            y=enc_y, x=enc_x, color=alt.value("#60a5fa"),
            tooltip=["currency:N",alt.Tooltip("rate_brl_base:Q",format=".4f")]
        )
        labels = alt.Chart(dfo.assign(label=dfo["rate_brl_base"].map(fmt_brl))).mark_text(align="left", dx=6).encode(
            y=enc_y, x="rate_brl_base:Q", text="label:N"
        )
        chart = (bars + labels).properties(height=320)

    st.altair_chart(
        chart.configure_view(strokeOpacity=0).configure_axis(grid=True, gridOpacity=0.08),
        use_container_width=True
    )

with tab_detail:
    st.dataframe(df_view, use_container_width=True)
    st.download_button("Baixar CSV (recorte atual)", data=df_view.to_csv(index=False).encode("utf-8"), file_name=f"gold_{day}.csv", mime="text/csv")
    st.download_button("Baixar Parquet (recorte atual)", data=df_view.to_parquet(index=False), file_name=f"gold_{day}.parquet", mime="application/octet-stream")

with tab_summary:
    json_path = GOLD_DIR / f"daily_summary_{day}.json"
    md_path = GOLD_DIR / f"daily_summary_{day}.md"
    if json_path.exists():
        data = json.loads(json_path.read_text(encoding="utf-8"))
        st.markdown(f"### {data.get('title','Resumo Cambial')}")
        for p in data.get("paragraphs", []):
            st.markdown(p)
    elif md_path.exists():
        st.markdown("### Resumo Cambial")
        st.markdown(sanitize_md(md_path.read_text(encoding="utf-8")))
    else:
        st.info("Resumo não encontrado. Rode: `python -m src.cli enrich`")

st.caption(f"Atualizado para {datetime.strptime(day,'%Y-%m-%d').strftime('%d/%m/%Y')} — Base BRL")
