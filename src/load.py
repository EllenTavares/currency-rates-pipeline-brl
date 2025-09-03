import os
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

SILVER_DATA_PATH = os.path.join('data', 'silver')
GOLD_DATA_PATH = os.path.join('data', 'gold')


def to_gold_brl_df(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Converte a tabela silver (base USD) para BRL:
    1 moeda X em BRL = (USD->BRL) / (USD->X)
    Garante BRL = 1.0.
    """
    
    brl_vs_usd = df_silver.loc[df_silver['target_currency'] == 'BRL', 'rate'].iloc[0]

    out = df_silver.copy()
    out['rate_brl_base'] = brl_vs_usd / out['rate']  

    df_gold = (
        out[['target_currency', 'rate_brl_base', 'last_update_utc']]
        .rename(columns={'target_currency': 'currency'})
        .reset_index(drop=True)
    )

    
    if (df_gold['currency'] == 'BRL').any():
        df_gold.loc[df_gold['currency'] == 'BRL', 'rate_brl_base'] = 1.0
    else:
        df_gold = pd.concat([df_gold, pd.DataFrame([{
            'currency': 'BRL',
            'rate_brl_base': 1.0,
            'last_update_utc': out['last_update_utc'].iloc[0]
        }])], ignore_index=True)

    return df_gold


def main():
    """
    Lê a camada silver, converte para base BRL e salva na gold.
    """
    logging.info("Iniciando o processo de carga para a camada Gold.")

    
    today_str = datetime.now().strftime('%Y-%m-%d')
    silver_file_path = os.path.join(SILVER_DATA_PATH, f"{today_str}.parquet")

    if not os.path.exists(silver_file_path):
        logging.error(f"Arquivo da camada Silver não encontrado: {silver_file_path}")
        return

    # 2) carrega silver
    logging.info(f"Carregando dados de {silver_file_path}")
    df_silver = pd.read_parquet(silver_file_path)

    try:
        df_gold = to_gold_brl_df(df_silver)

        # 3) salva gold
        os.makedirs(GOLD_DATA_PATH, exist_ok=True)
        gold_file_path = os.path.join(GOLD_DATA_PATH, f"exchange_rates_brl_base_{today_str}.parquet")
        df_gold.to_parquet(gold_file_path, index=False)
        logging.info(f"Dataset Gold salvo com sucesso em: {gold_file_path}")

    except IndexError:
        logging.error("A moeda 'BRL' não foi encontrada nos dados. Não é possível criar o dataset final.")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado durante a transformação para Gold: {e}")


if __name__ == "__main__":
    main()
