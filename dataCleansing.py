import csv
import pandas as pd
import re
from datetime import datetime
from tqdm import tqdm

tqdm.pandas()

def clean_text(val):
    if pd.isna(val):
        return None
    val = re.sub(r"[Â\xa0]+", " ", str(val))
    val = re.sub(r"\s+", " ", val).strip()
    return val


def extract_birth_info(born_str):
    if pd.isna(born_str):
        return pd.Series([None, None, None, None],
                         index=["Birthday", "Age", "Birth City", "Birth Country"])
    
    born_str = clean_text(born_str)

    date_match = re.search(r"([A-Za-z]+\s+\d{1,2},\s*\d{4})", born_str)
    birthday = None
    if date_match:
        try:
            birthday = datetime.strptime(date_match.group(1), "%B %d, %Y").date()
        except ValueError:
            birthday = None

    age_match = re.search(r"Age:\s*([0-9\-]+)", born_str)
    age = None
    if age_match:
        age = age_match.group(1).split("-")[0].strip()


    place_match = re.search(r"in\s*([\w\s\.-]+?),\s*([\w\s]+?)(?:\s+([A-Za-z]{2}))?$", born_str)
    birth_city = birth_state = birth_country = None
    if place_match:
        birth_city = place_match.group(1).strip()
        birth_state = place_match.group(2).strip()
        birth_country = place_match.group(3).strip() if place_match.group(3) else None

    return pd.Series([birthday, age, birth_city,birth_state, birth_country],
                     index=["Birthday", "Age", "Birth City", "Birth State", "Birth Country"])


def clean_transactions(text):
    if pd.isna(text):
        return None
    text = re.sub(r"\n+", " ", str(text))
    text = re.sub(r"\s*\.\s*", ". ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def extract_position(meta):
    if pd.isna(meta):
        return None
    match = re.search(r"Position:\s*([^\nâ–ª]+)", meta)
    if match:
        return match.group(1).strip()
    return None


def pounds_to_kg(weight_str):
    if pd.isna(weight_str):
        return None
    match = re.search(r"(\d+)\s*lb", str(weight_str))
    if match:
        lbs = float(match.group(1))
        kg = round(lbs * 0.45359237, 1)
        return f"{kg} kg"
    return weight_str


def clean_player_data(df):
    df = df.copy()
    df = df.applymap(clean_text)

    if "Born" in df.columns:
        born_parts = df["Born"].progress_apply(extract_birth_info)
        df = pd.concat([df, born_parts], axis=1)

    if "TransactionsRaw" in df.columns:
        df["Transactions"] = df["TransactionsRaw"].progress_apply(clean_transactions)

    if "MetaRaw" in df.columns:
        df["Position"] = df["MetaRaw"].progress_apply(extract_position)

    if "Weight" in df.columns:
        df["Weight"] = df["Weight"].apply(pounds_to_kg)

    df.dropna(how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    return df

df_namelink = pd.read_csv(
    "player_links.csv",
    quotechar='"',
    encoding="utf-8",
    engine="python"
)

df_playerinfo = pd.read_csv(
    "players_data3.csv",
    quotechar='"',
    encoding="utf-8",
    engine="python",
    skipinitialspace=True
)

if "MetaRaw" in df_playerinfo.columns:
    df_playerinfo = df_playerinfo.drop(columns=["MetaRaw"])

df_merged = pd.merge(
    df_namelink,
    df_playerinfo,
    on=["Player Name", "Profile URL"],
    how="left"
)

df_clean = clean_player_data(df_merged)

df_clean.to_csv(
    "players_cleaned.csv",
    index=False,
    quotechar='"',
    quoting=csv.QUOTE_ALL,
    encoding="utf-8"
)