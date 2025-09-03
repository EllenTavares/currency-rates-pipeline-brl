import os
import requests
import json
from datetime import datetime
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


load_dotenv()
logging.info("Variáveis de ambiente carregadas.")

def main():
    """
    Função principal para executar o pipeline de ingestão de dados.
    """
    logging.info("Iniciando o processo de ingestão de dados.")

   
    api_key = os.getenv("EXCHANGERATE_API_KEY")
    if not api_key:
        logging.error("A chave da API (EXCHANGERATE_API_KEY) não foi encontrada. Verifique seu arquivo .env.")
        return 

   
    base_currency = "USD"
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}"


    try:
        logging.info(f"Buscando cotações para a moeda base: {base_currency}")
        response = requests.get(url)
        response.raise_for_status()  

        data = response.json()
        logging.info("Dados recebidos da API com sucesso.")

        
        today_str = datetime.now().strftime('%Y-%m-%d')
        file_name = f"{today_str}.json"

        raw_data_path = os.path.join('data', 'raw')
        os.makedirs(raw_data_path, exist_ok=True)
        
        file_path = os.path.join(raw_data_path, file_name)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Dados brutos salvos com sucesso em: {file_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao fazer a requisição à API: {e}")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    main()