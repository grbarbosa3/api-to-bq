
import pandas as pd

def normalize_rows(rows):
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["loaded_at"] = pd.Timestamp.utcnow()
    return df

def run_quality_checks(df: pd.DataFrame):
    # Exemplo de regras simples e explicáveis
    assert df is not None, "no dataframe"
    if df.empty:
        raise ValueError("no data returned from API")
    if "lat" in df and ((df["lat"].abs() > 90).any() or (df["lon"].abs() > 180).any()):
        raise ValueError("invalid coordinates detected")

