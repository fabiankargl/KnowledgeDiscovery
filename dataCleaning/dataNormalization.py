import pandas as pd
import re

df = pd.read_csv("datasets/players_cleaned.csv", sep=None, engine="python")

df.columns = df.columns.str.strip().str.lower()
df = df.loc[:, ~df.columns.duplicated()]

def find_column_containing(df, keyword):
    for col in df.columns:
        if keyword in col:
            return col
    return None

position_col = find_column_containing(df, "position")

def split_position_shoots(value):
    if pd.isna(value):
        return pd.Series({"position clean": None, "shoots": None})
    pattern = r"(.*?)(?:\s+shoots:\s*(\w+))?$"
    match = re.match(pattern, str(value).strip(), flags=re.IGNORECASE)
    return pd.Series({
        "position clean": match.group(1).strip() if match and match.group(1) else None,
        "shoots": match.group(2) if match and match.group(2) else None
    })

if position_col:
    split_cols = df[position_col].apply(split_position_shoots)
    df = pd.concat([df, split_cols], axis=1)

birthday_col = find_column_containing(df, "born")
if birthday_col:
    df["birthday"] = df[birthday_col].apply(
        lambda x: re.search(r"([A-Za-z]+\s+\d{1,2}\s*,\s*\d{4})", str(x)).group(1).strip()
        if pd.notna(x) and re.search(r"([A-Za-z]+\s+\d{1,2}\s*,\s*\d{4})", str(x)) else None
    )

tx_col = find_column_containing(df, "transactions")
if tx_col:
    df["transactions list"] = df[tx_col].apply(
        lambda x: [y.strip() for y in str(x).split(". ") if y.strip()] if pd.notna(x) else []
    )

weight_col = find_column_containing(df, "weight")
if weight_col:
    df["weight"] = (
        df[weight_col].astype(str)
        .str.extract(r"([\d.]+)")
        .astype(float)
        .fillna(0)
        .round(0)
        .astype(int)
    )

age_col = find_column_containing(df, "age")
if age_col:
    df["age"] = pd.to_numeric(df[age_col], errors="coerce").fillna(0).astype(int)

for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].astype(str).str.strip().str.lower()

keep_cols = [
    "player name", "profile url", "position clean", "shoots",
    "birthday", "college", "high school", "draft",
    "weight", "age", "birth city", "birth country",
    "transactions list"
]
existing = [c for c in keep_cols if c in df.columns]
clean_df = df[existing]

clean_df.to_csv("datasets/players_normalized.csv", index=False)
