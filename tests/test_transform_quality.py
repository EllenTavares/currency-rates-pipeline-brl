import pandas as pd
from src.transform import to_silver_df

def test_quality_filters():
    raw = {
        "base_code": "USD",
        "time_last_update_unix": 4102444800,  # 2100-01-01
        "conversion_rates": {"USD": 1.0, "BRL": 5.0, "EUR": 0.0, "ARS": -2}
    }
    df = to_silver_df(raw)
    assert set(df['target_currency']) == {"USD", "BRL"}
