import os
import argparse
import json
import re
import unicodedata
import pandas as pd
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
load_dotenv()

def _fmt_brl(x):
    return f"R$ {x:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _clean_text(t):
    t = unicodedata.normalize("NFKC", t)
    t = t.replace("\u200b","").replace("\u200c","").replace("\u200d","").replace("\ufeff","").replace("\u2060","")
    t = t.replace("\xa0"," ")
    t = re.sub(r"(?<=\w)_(?=\w)", " ", t)
    t = re.sub(r",\s+(\d)", r",\1", t)
    t = re.sub(r"R\s+\$", "R$ ", t)
    parts = re.split(r"\n\s*\n", t.strip())
    parts = [" ".join(p.strip().split()) for p in parts]
    return "\n\n".join(parts)

def _extract_json(s):
    s = s.strip()
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        try:
            fixed = re.sub(r"(\n|\r)", " ", m.group(0))
            return json.loads(fixed)
        except Exception:
            return None

def _generate_for_date(date_str):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logging.error("OPENAI_API_KEY ausente no .env")
        return False
    client = OpenAI(api_key=api_key)
    gold_file_path = os.path.join("data", "gold", f"exchange_rates_brl_base_{date_str}.parquet")
    if not os.path.exists(gold_file_path):
        logging.warning(f"Gold ausente para {date_str}: {gold_file_path}")
        return False
    df = pd.read_parquet(gold_file_path)
    key = ["USD", "EUR", "GBP", "JPY", "ARS"]
    df_f = df[df["currency"].isin(key)].copy()
    if df_f.empty:
        df_f = df.sort_values("rate_brl_base", ascending=False).head(5).copy()
    lines = [f"1 {r.currency} = {_fmt_brl(float(r.rate_brl_base))}" for r in df_f.itertuples()]
    data_for_prompt = "\n".join(lines)
    d = datetime.strptime(date_str, "%Y-%m-%d")
    date_br = d.strftime("%d/%m/%Y")
    prompt = f"""
Você é um analista financeiro sênior no Brasil. Gere um resumo executivo do câmbio do dia a partir dos dados abaixo.
Dados de {date_br}, valores em BRL para 1 unidade de cada moeda:
{data_for_prompt}

Responda SOMENTE em JSON válido, sem Markdown, no formato:
{{
  "title": "Resumo Cambial - {date_br}",
  "paragraphs": [
    "Parágrafo 1 curto, objetivo, em PT-BR.",
    "Parágrafo 2 curto, objetivo, em PT-BR.",
    "Parágrafo 3 curto, objetivo, em PT-BR."
  ]
}}
Regras: frases curtas; destaque USD e EUR; cite moedas sul-americanas se relevante; sem itálico/negrito; sem caracteres invisíveis; sem emojis; sem quebras de palavra. Não inclua explicações fora do JSON.
""".strip()
    try:
        logging.info(f"Gerando resumo LLM (JSON) para {date_str}")
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Você é um analista financeiro útil e conciso."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
            max_tokens=300
        )
        content = resp.choices[0].message.content or ""
        data = _extract_json(content)
        if not data or "title" not in data or "paragraphs" not in data:
            logging.warning("Resposta sem JSON válido. Salvando .md limpo como fallback.")
            insight = _clean_text(content)
            md_path = os.path.join("data", "gold", f"daily_summary_{date_str}.md")
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(insight)
            return True
        title = str(data.get("title", f"Resumo Cambial - {date_br}")).strip()
        paragraphs = [p for p in data.get("paragraphs", []) if isinstance(p, str) and p.strip()]
        paragraphs = [_clean_text(p) for p in paragraphs][:3]
        out_json = {"title": title, "paragraphs": paragraphs}
        json_path = os.path.join("data", "gold", f"daily_summary_{date_str}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(out_json, f, ensure_ascii=False, indent=2)
        md_text = title + "\n\n" + "\n\n".join(paragraphs)
        md_path = os.path.join("data", "gold", f"daily_summary_{date_str}.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_text)
        logging.info(f"Resumo salvo: {json_path} e {md_path}")
        return True
    except Exception as e:
        logging.error(f"Falha na geração para {date_str}: {e}")
        return False

def main(date=None, start=None, end=None):
    if start and end:
        d0 = datetime.strptime(start, "%Y-%m-%d")
        d1 = datetime.strptime(end, "%Y-%m-%d")
        cur = d0
        ok = False
        while cur <= d1:
            ok = _generate_for_date(cur.strftime("%Y-%m-%d")) or ok
            cur += timedelta(days=1)
        return ok
    target = date or datetime.now().strftime("%Y-%m-%d")
    return _generate_for_date(target)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date")
    p.add_argument("--start")
    p.add_argument("--end")
    a = p.parse_args()
    main(a.date, a.start, a.end)
