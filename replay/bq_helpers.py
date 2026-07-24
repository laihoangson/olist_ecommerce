"""
Shared BigQuery query helper for the Operations Command Center scripts.

BigQuery NUMERIC/BIGNUMERIC columns (commonly used for money columns like
price/freight_value) come back from `.to_dataframe()` as Python
`decimal.Decimal` objects inside an object-dtype pandas column -- NOT
float64. Most of numpy/sklearn (np.log1p, StandardScaler, arithmetic
against a plain float, sklearn's imputers/scalers) don't accept Decimal
and raise a TypeError/AttributeError the first time they touch it (e.g.
"'decimal.Decimal' object has no attribute 'log1p'").

model_v4_improved.ipynb's `q()` helper already guarded against this (see
its Setup cell). This module is the equivalent for the live-scoring
scripts and model/train_and_export.py, factored out into one place so
every BigQuery call site in this project gets the same protection instead
of relying on remembering `.astype(float)` after each individual query.
"""

import decimal


def fix_decimal_columns(df):
    """Casts any object-dtype column holding decimal.Decimal values to
    float64, in place, and returns df."""
    for c in df.columns:
        if df[c].dtype == "object" and len(df) > 0:
            first_non_null = df[c].dropna()
            if len(first_non_null) and isinstance(first_non_null.iloc[0], decimal.Decimal):
                df[c] = df[c].astype(float)
    return df


def q(client, sql):
    """Runs a query and returns a decimal-safe DataFrame. Use this instead
    of `client.query(sql).to_dataframe()` everywhere in this project."""
    df = client.query(sql).to_dataframe()
    return fix_decimal_columns(df)
