import argparse
from pathlib import Path
import pandas as pd
from src.ingest import main as ingest_main
from src.transform import main as transform_main
from src.load import main as load_main
from src.enrich import main as enrich_main

GOLD_DIR = Path("data/gold")
SILVER_DIR = Path("data/silver")

def _pick_file(dirpath: Path, pattern: str, date_str: str | None) -> Path | None:
    if date_str:
        p = dirpath / pattern.format(date=date_str)
        return p if p.exists() else None
    files = sorted(dirpath.glob(pattern.format(date="*")))
    return files[-1] if files else None

def _fmt_decimal(x: float, places: int = 6) -> str:
    return f"{x:,.{places}f}".replace(",", "X").replace(".", ",").replace("X", ".")

def view_gold(date: str | None, currencies: list[str] | None, top: int | None) -> int:
    p = _pick_file(GOLD_DIR, "exchange_rates_brl_base_{date}.parquet", date)
    if not p:
        print("Nenhum arquivo encontrado em data/gold para a data solicitada.")
        return 1
    df = pd.read_parquet(p)[["currency", "rate_brl_base", "last_update_utc"]].copy()
    if currencies:
        wanted = [c.upper() for c in currencies]
        found = df["currency"].str.upper().isin(wanted)
        missing = [c for c in wanted if c not in set(df["currency"].str.upper())]
        if missing:
            print(f"Moedas não encontradas no arquivo {p.name}: {', '.join(missing)}")
        df = df[found]
    if top:
        df = df.sort_values("rate_brl_base", ascending=False).head(top)
    df = df.sort_values("currency").reset_index(drop=True)
    df["rate_brl_base"] = df["rate_brl_base"].map(_fmt_decimal)
    print(f"\n[GOLD/BRL] Arquivo: {p}")
    print(df.to_string(index=False))
    print()
    return 0

def view_silver(date: str | None, currencies: list[str] | None, top: int | None) -> int:
    p = _pick_file(SILVER_DIR, "{date}.parquet", date)
    if not p:
        print("Nenhum arquivo encontrado em data/silver para a data solicitada.")
        return 1
    df = pd.read_parquet(p)[["base_currency", "target_currency", "rate", "last_update_utc"]].copy()
    if currencies:
        wanted = [c.upper() for c in currencies]
        df = df[df["target_currency"].str.upper().isin(wanted)]
    if top:
        df = df.sort_values("rate", ascending=False).head(top)
    df = df.rename(columns={"target_currency": "currency"}).sort_values("currency").reset_index(drop=True)
    df["rate"] = df["rate"].map(_fmt_decimal)
    base = df["base_currency"].iloc[0] if not df.empty else "USD"
    print(f"\n[SILVER/base={base}] Arquivo: {p} (rate = {base}->currency)")
    print(df[["currency", "rate", "last_update_utc"]].to_string(index=False))
    print()
    return 0

def _load_layer_row(layer: str, path: Path) -> pd.DataFrame:
    if layer == "gold":
        df = pd.read_parquet(path)[["currency", "rate_brl_base"]].copy()
        df = df.rename(columns={"rate_brl_base": "value"})
    else:
        df = pd.read_parquet(path)[["target_currency", "rate"]].copy()
        df = df.rename(columns={"target_currency": "currency", "rate": "value"})
    df["currency"] = df["currency"].astype(str)
    return df

def compare_dates(date1: str, date2: str, layer: str, currencies: list[str] | None, top: int | None) -> int:
    pattern = "exchange_rates_brl_base_{date}.parquet" if layer == "gold" else "{date}.parquet"
    dirpath = GOLD_DIR if layer == "gold" else SILVER_DIR
    p1 = _pick_file(dirpath, pattern, date1)
    p2 = _pick_file(dirpath, pattern, date2)
    if not p1 or not p2:
        print("Arquivo(s) não encontrado(s) para as datas informadas.")
        return 1
    d1 = _load_layer_row(layer, p1)
    d2 = _load_layer_row(layer, p2)
    df = d1.merge(d2, on="currency", how="inner", suffixes=(f"_{date1}", f"_{date2}"))
    df["delta"] = df[f"value_{date2}"] - df[f"value_{date1}"]
    df["pct"] = (df["delta"] / df[f"value_{date1}"]) * 100.0
    if currencies:
        wanted = [c.upper() for c in currencies]
        df = df[df["currency"].str.upper().isin(wanted)]
    df = df.sort_values("pct", key=lambda s: s.abs(), ascending=False)
    if top:
        df = df.head(top)
    df_form = df.copy()
    df_form[f"value_{date1}"] = df_form[f"value_{date1}"].map(lambda x: _fmt_decimal(x, 6))
    df_form[f"value_{date2}"] = df_form[f"value_{date2}"].map(lambda x: _fmt_decimal(x, 6))
    df_form["delta"] = df_form["delta"].map(lambda x: _fmt_decimal(x, 6))
    df_form["pct"] = df_form["pct"].map(lambda x: f"{x:+.2f}%".replace(".", ","))
    header = "GOLD (BRL)" if layer == "gold" else "SILVER (base USD)"
    print(f"\nComparação {header}\n  {p1.name}  →  {p2.name}")
    cols = ["currency", f"value_{date1}", f"value_{date2}", "delta", "pct"]
    print(df_form[cols].to_string(index=False))
    print()
    return 0

def main():
    parser = argparse.ArgumentParser("FX Pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ingest")
    sub.add_parser("transform")
    sub.add_parser("load")

    p_enrich = sub.add_parser("enrich")
    p_enrich.add_argument("--date")
    p_enrich.add_argument("--start")
    p_enrich.add_argument("--end")

    sub.add_parser("all")

    p_view = sub.add_parser("view")
    p_view.add_argument("--date", default=None)
    p_view.add_argument("--curr", nargs="*")
    p_view.add_argument("--top", type=int)

    p_view_s = sub.add_parser("view-silver")
    p_view_s.add_argument("--date", default=None)
    p_view_s.add_argument("--curr", nargs="*")
    p_view_s.add_argument("--top", type=int)

    p_cmp = sub.add_parser("compare")
    p_cmp.add_argument("date1")
    p_cmp.add_argument("date2")
    p_cmp.add_argument("--layer", choices=["gold", "silver"], default="gold")
    p_cmp.add_argument("--curr", nargs="*")
    p_cmp.add_argument("--top", type=int)

    args = parser.parse_args()

    if args.cmd == "ingest":
        ingest_main()
    elif args.cmd == "transform":
        transform_main()
    elif args.cmd == "load":
        load_main()
    elif args.cmd == "enrich":
        enrich_main(args.date, args.start, args.end)
    elif args.cmd == "all":
        ingest_main(); transform_main(); load_main(); enrich_main(None, None, None)
    elif args.cmd == "view":
        if args.curr is None and args.top is None:
            args.curr = ["USD", "EUR", "BRL", "GBP", "JPY"]
        raise SystemExit(view_gold(args.date, args.curr, args.top))
    elif args.cmd == "view-silver":
        if args.curr is None and args.top is None:
            args.curr = ["USD", "EUR", "BRL", "GBP", "JPY"]
        raise SystemExit(view_silver(args.date, args.curr, args.top))
    elif args.cmd == "compare":
        if args.curr is None and args.top is None:
            args.top = 10
        raise SystemExit(compare_dates(args.date1, args.date2, args.layer, args.curr, args.top))

if __name__ == "__main__":
    main()
