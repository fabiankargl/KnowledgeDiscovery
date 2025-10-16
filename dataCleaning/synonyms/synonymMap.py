import pandas as pd
import json
import re

df = pd.read_csv("datasets/players_normalized_stopwords_stemming.csv", sep=None, engine="python")
with open("dataCleaning/synonyms/synonymList.json") as f:
    position_map = json.load(f)

def abbreviate(text):
    if pd.isna(text):
        return None
    for full, abbr in position_map.items():
        text = re.sub(r"\b{}\b".format(re.escape(full)), abbr, text)
    text = re.sub(r"\s*[,/]\s*", " / ", text)
    text = re.sub(r"(?: / )+", " / ", text)
    text = text.strip()
    return text

df["age"] = df["age"].apply(lambda x: "-" if x == 0 else x)
for col in df.columns:
    df[col] = df[col].replace({pd.NA: "-", pd.NaT: "-", None: "-", "": "-"})
    df[col] = df[col].fillna("-")

df["position clean"] = df["position clean"].apply(abbreviate)
df["shoots"] = df["shoots"].map(position_map)

df.to_csv("datasets/players_clean_abbr.csv", index=False)
