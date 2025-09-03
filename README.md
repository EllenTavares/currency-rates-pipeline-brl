# FX Pipeline + LLM — Cotações Cambiais (USD→moedas | base BRL)

**Aluna:** Ellen Cristina Tavares Gabriel — **RA:** 1520194
**Aluno:** Jorge Rodrigues de Moraes Junior - **RA:** 2500751
**Aluna:** Emanuelle Müller Tadaiesky — **RA:** 2502198
**Aluna:** Melany Santos Antoniazzi — **RA:** 2102353
## Guia rápido (passo a passo)

### Windows (PowerShell)
##powershell
# 1) Criar e ativar a venv
python -m venv venv
.\venv\Scripts\Activate.ps1

# 2) Instalar dependências
pip install -r requirements.txt

# 3) Configurar chaves (.env)
copy .env.example .env
notepad .env
# preencha:
# EXCHANGERATE_API_KEY=SEU_TOKEN_EXCHANGERATE_V6_AQUI
# OPENAI_API_KEY=SEU_TOKEN_OPENAI_AQUI

# 4) Rodar o pipeline completo (gera raw/silver/gold + resumo LLM)
python -m src.cli all

# 5) Conferir rápido no terminal (opcional)
python -m src.cli view
python -m src.cli compare 2025-08-01 2025-08-30

# 6) Abrir o dashboard
python -m streamlit run streamlit_app.py --server.runOnSave true
# abra o link mostrado (ex.: http://localhost:8501)
# para parar: Ctrl+C no terminal


## WSL/Linux/macOS

# 1) Criar e ativar a venv
python -m venv venv
source venv/bin/activate

# 2) Instalar dependências
pip install -r requirements.txt

# 3) Configurar chaves (.env)
cp .env.example .env
$EDITOR .env
# preencha:
# EXCHANGERATE_API_KEY=SEU_TOKEN_EXCHANGERATE_V6_AQUI
# OPENAI_API_KEY=SEU_TOKEN_OPENAI_AQUI

# 4) Rodar o pipeline completo (gera raw/silver/gold + resumo LLM)
python -m src.cli all

# 5) Conferir rápido no terminal (opcional)
python -m src.cli view
python -m src.cli compare 2025-08-29 2025-08-30

# 6) Abrir o dashboard
python -m streamlit run streamlit_app.py --server.runOnSave true
# abra o link mostrado (ex.: http://localhost:8501)
# para parar: Ctrl+C

Sumário

Objetivo
Arquitetura e Camad
Estrutura do Repositório
Pré-requisitos
Instalação e Setup
Execução
Comandos de Inspeção
Saídas Esperadas
Testes e Qualidade
Logging e Observabilidade
LLM: Estratégia e Configuração
Troubleshooting
Segurança
Licença

Objetivo

Coletar taxas de câmbio diárias da ExchangeRate API (endpoint /latest/USD).
Normalizar e validar dados (raw → silver → gold).
Gerar resumo executivo via LLM (OpenAI) em Markdown.
Entregar dados em Parquet e fornecer comandos de inspeção/testes.

Arquitetura e Camadas:

Raw: resposta JSON bruta (um arquivo por dia).
Silver (base USD): Parquet normalizado com filtros de qualidade (remove nulos/zero/negativos).
Gold (base BRL): conversão para BRL (1 moeda X em BRL = (USD→BRL) / (USD→X); BRL=1.0).
LLM: resumo executivo diário (Markdown) gerado a partir da Gold.
Dashboard: streamlit_app.py exibe KPIs, tabela, gráfico e o resumo LLM.

Estrutura do Repositório

projeto-pipeline-cambio/
├─ data/
│  ├─ raw/      # JSON (YYYY-MM-DD.json)
│  ├─ silver/   # Parquet (YYYY-MM-DD.parquet)
│  └─ gold/     # Parquet base BRL + resumo .md
├─ src/
│  ├─ __init__.py
│  ├─ ingest.py
│  ├─ transform.py
│  ├─ load.py
│  ├─ enrich.py
│  └─ cli.py
├─ tests/
│  ├─ test_load_conversion.py
│  └─ test_transform_quality.py
├─ streamlit_app.py
├─ .env / .env.example
├─ requirements.txt
├─ pytest.ini
└─ .vscode/tasks.json

Pré-requisitos

Python 3.11+ (testado em 3.12)
Internet para exchangerate-api.com e api.openai.com
Chaves: EXCHANGERATE_API_KEY (ExchangeRate v6) e OPENAI_API_KEY (OpenAI)

Instalação e Setup:

# Windows
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
notepad .env

# WSL/Linux/macOS
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env && $EDITOR .env

Endpoint usado: https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD.
pytest.ini garante from src... sem exportar PYTHONPATH.

Execução:

# pipeline completo (raw → silver → gold + resumo LLM)
python -m src.cli all

# etapas individuais
python -m src.cli ingest
python -m src.cli transform
python -m src.cli load
python -m src.cli enrich

Comandos de Inspeção:

# GOLD (BRL)
python -m src.cli view
python -m src.cli view --curr USD BRL EUR
python -m src.cli view --top 10
python -m src.cli view --date 2025-08-30

# SILVER (USD)
python -m src.cli view-silver
python -m src.cli view-silver --curr USD EUR BRL
python -m src.cli view-silver --top 15

# comparação entre dias
python -m src.cli compare 2025-08-29 2025-08-30
python -m src.cli compare 2025-08-29 2025-08-30 --curr USD EUR BRL
python -m src.cli compare 2025-08-29 2025-08-30 --layer silver --top 10

Comandos de Inspeção:

# GOLD (BRL)
python -m src.cli view
python -m src.cli view --curr USD BRL EUR
python -m src.cli view --top 10
python -m src.cli view --date 2025-08-30

# SILVER (USD)
python -m src.cli view-silver
python -m src.cli view-silver --curr USD EUR BRL
python -m src.cli view-silver --top 15

# comparação entre dias
python -m src.cli compare 2025-08-29 2025-08-30
python -m src.cli compare 2025-08-29 2025-08-30 --curr USD EUR BRL
python -m src.cli compare 2025-08-29 2025-08-30 --layer silver --top 10

Saídas Esperadas:

data/raw/YYYY-MM-DD.json

data/silver/YYYY-MM-DD.parquet
colunas: base_currency, target_currency, rate, last_update_utc

data/gold/exchange_rates_brl_base_YYYY-MM-DD.parquet
colunas: currency, rate_brl_base, last_update_utc

data/gold/daily_summary_YYYY-MM-DD.md (resumo LLM)

Testes e Qualidade:

pytest -q
test_transform_quality.py: filtros de qualidade
test_load_conversion.py: matemática da conversão para BRL
# pytest.ini
[pytest]
pythonpath = .
testpaths = tests

Logging e Observabilidade:

Mensagens INFO em todas as etapas (ingest, transform, load, enrich) e erros claros.
Evolutivo para structlog se necessário.

LLM: Estratégia e Configuração:

Modelo: gpt-4o-mini (configurável em src/enrich.py)
Prompt: português, executivo, objetivo; destaca USD/EUR e moedas regionais
Saída: Markdown salvo em data/gold

Troubleshooting:

ModuleNotFoundError: No module named 'src': confirme src/__init__.py e pytest.ini com pythonpath = .
“Nenhum arquivo em data/...”: rode python -m src.cli all ou use --date
streamlit não encontrado: pip install streamlit e rode python -m streamlit run streamlit_app.py
Página não atualiza: Rerun, --server.runOnSave true ou “⋯ → Clear cache”
Chaves não lidas: verifique .env e venv ativa
OpenAI quota/HTTP: revise credenciais e max_tokens em enrich.py

Segurança:

Não versionar .env (já no .gitignore)
Não expor chaves de API