import os
import pandas as pd
import json
from datetime import datetime, timezone
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

RAW_DATA_PATH = os.path.join('data', 'raw')
SILVER_DATA_PATH = os.path.join('data', 'silver')


def to_silver_df(data: dict) -> pd.DataFrame:
    """
    Converte o JSON bruto da API em um DataFrame normalizado (silver).
    Aplica qualidade: remove nulos/<=0.
    """
    base_currency = data.get("base_code")
    rates = data.get("conversion_rates", {})
    last_update_unix = data.get("time_last_update_unix")
    last_update_utc = datetime.fromtimestamp(last_update_unix, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    rows = [
        {"base_currency": base_currency, "target_currency": cur, "rate": rate, "last_update_utc": last_update_utc}
        for cur, rate in rates.items()
    ]
    df = pd.DataFrame(rows)

    df['rate'] = pd.to_numeric(df['rate'], errors='coerce')
    initial_rows = len(df)
    df = df.dropna(subset=['rate'])
    df = df[df['rate'] > 0]
    removed = initial_rows - len(df)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por qualidade (nulos/<=0).")

    return df


def main():
    """
    Transforma os dados brutos da raw em silver (parquet).
    """
    logging.info("Iniciando o processo de transformação de dados.")

    today_str = datetime.now().strftime('%Y-%m-%d')
    raw_file_path = os.path.join(RAW_DATA_PATH, f"{today_str}.json")

    if not os.path.exists(raw_file_path):
        logging.error(f"Arquivo de dados brutos não encontrado para hoje: {raw_file_path}")
        return

    logging.info(f"Carregando dados de {raw_file_path}")
    with open(raw_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)


    df = to_silver_df(data)
    logging.info(f"Dados transformados em DataFrame com {len(df)} registros.")

    os.makedirs(SILVER_DATA_PATH, exist_ok=True)
    silver_file_path = os.path.join(SILVER_DATA_PATH, f"{today_str}.parquet")
    df.to_parquet(silver_file_path, index=False)
    logging.info(f"Dados transformados e salvos com sucesso em: {silver_file_path}")


if __name__ == "__main__":
    main()
