import pandas as pd
from src.load import to_gold_brl_df

def test_brl_conversion_math():
    df_silver = pd.DataFrame({
        "target_currency": ["USD", "BRL", "EUR"],
        "rate": [1.0, 5.0, 0.5],  # USD->USD, USD->BRL, USD->EUR
        "last_update_utc": ["2099-01-01 00:00:00"]*3
    })
    gold = to_gold_brl_df(df_silver).set_index("currency")

    # Esperado: 1 USD = 5 BRL ; 1 BRL = 1 BRL ; 1 EUR = 10 BRL (5/0.5)
    assert abs(gold.loc["USD","rate_brl_base"] - 5.0) < 1e-9
    assert abs(gold.loc["BRL","rate_brl_base"] - 1.0) < 1e-9
    assert abs(gold.loc["EUR","rate_brl_base"] - 10.0) < 1e-9
