"""
Usage when using CLI: python build_index_players.py --input datasets/players_clean_abbr.csv
"""

import pandas as pd
import math
import pickle
import re
import json
from collections import defaultdict, Counter
from typing import Dict, List, Any
from argparse import ArgumentParser

INPUT_CSV = "datasets/players_clean_abbr.csv"
OUT_INDEX = "indexes/index.pkl"
OUT_IDF = "indexes/idf.pkl"
OUT_DOCNORMS = "indexes/doc_norms.pkl"
OUT_DOCMETA = "indexes/doc_meta.pkl"

# Text fields to be tokenized and indexed
FIELDS_TO_INDEX = [
    "player name",
    "position clean",
    "draft",
    "birth city",
    "birth country",
    "transactions list",
    "college",
    "high school"
]

# Numeric fields
FIELDS_NUMERIC = [
    "age", "weight"
]

# Keyword fields
FIELDS_KEYWORD = [
    "profile url"
]

# Field boosts
FIELD_BOOSTS: Dict[str, float] = {
    "player name": 3.0,
    "position clean": 2.0,
    "draft": 1.5,
    "birth city": 1.0,
    "birth country": 1.0,
    "transactions list": 1.0,
    "college": 1.0,
    "high school": 0.8
}

def simple_tokenize(text: str) -> List[str]:
    """
    Tokenize a text string into a list of terms
    """
    if text is None or str(text).lower() == "nan":
        return []

    # Handle text stored as a Python list
    if text.startswith("[") and text.endswith("]"):
        try: 
            parsed = json.loads(text.replace("'", '"'))
            if isinstance(parsed, list):
                text = " ".join(parsed)
        except Exception:
            pass

    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # Split text into tokens
    tokens = [t for t in text.split() if t]
    return tokens

def tf_weight(count: int) -> float:
    """
    Compute the logarithmic TF weighting
    """
    return 1.0 + math.log10(count) if count > 0 else 0.0

def persist(obj: Any, fname: str) -> None:
    """
    Saves a Python object as a pickle file
    """
    with open(fname, "wb") as f:
        pickle.dump(obj, f)
    print(f"[SUCCESS] {fname} saved")

def build_index(df: pd.DataFrame):
    """
    Build a complete inverted index from a pandas DataFrame
    """
    index = {
        "text": {f: defaultdict(dict) for f in FIELDS_TO_INDEX},
        "numeric": {f: {} for f in FIELDS_NUMERIC},
        "keyword": {f: defaultdict(list) for f in FIELDS_KEYWORD}
    }

    doc_meta: Dict[int, Dict[str, Any]] = {}

    number_of_documents = len(df)
    print(f"[INFO] Number of Documents (Players): {number_of_documents}")

    # Iterate through each row
    for i, row in df.iterrows():
        doc_id = i
        doc_meta[doc_id] = row.to_dict() # original row as metadata

        for field in FIELDS_TO_INDEX:
            tokens = simple_tokenize(row.get(field, ""))
            if not tokens:
                continue
            counts = Counter(tokens)
            for term, count in counts.items():
                index["text"][field].setdefault(term, {})[doc_id] = count

        for field in FIELDS_NUMERIC:
            try:
                value = float(row.get(field))
                if not math.isnan(value):
                    index["numeric"][field][doc_id] = value
            except Exception:
                continue

        for field in FIELDS_KEYWORD:
            value = str(row.get(field, "")).strip().lower()
            if value:
                index["keyword"][field][value].append(doc_id)

    return index, number_of_documents, doc_meta
 
def compute_idf_and_norms(index: Dict[str, Dict[str, Dict[int, int]]],
                          number_of_documents: int):
    """
    Compute IDF and document norms
    """
    idf: Dict[str, Dict[str, float]] = {f: {} for f in FIELDS_TO_INDEX}

    doc_squared_weights: Dict[int, float] = defaultdict(float)

    # Compute IDF values
    for field in FIELDS_TO_INDEX:
        for term, postings in index["text"][field].items():
            df_term = len(postings)

            idf_value = math.log10((number_of_documents + 1) / (df_term + 1)) + 1.0
            idf[field][term] = idf_value

    # Compute document norms for cosine similarity
    for field in FIELDS_TO_INDEX:
        boost = FIELD_BOOSTS.get(field, 1.0)
        for term, postings in index["text"][field].items():
            idf_value = idf[field][term]

            for doc_id, count in postings.items():
                tf = tf_weight(count)
                weight = tf * idf_value * boost
                doc_squared_weights[doc_id] += weight * weight

    doc_norms = {doc_id: math.sqrt(squared_sum) for doc_id, squared_sum in doc_squared_weights.items()}
    return idf, doc_norms

def main(input_csv: str = INPUT_CSV):
    print(f"[INFO] Load CSV-File: {input_csv}")
    df = pd.read_csv(input_csv, sep=";")

    print("[INFO] Build invetred Index ...")
    index, number_of_documents, doc_meta = build_index(df=df)

    print("[INFO] Calculate IDF and Document Norms ...")
    idf, doc_norms = compute_idf_and_norms(index=index,
                                           number_of_documents=number_of_documents)
    
    print("[INFO] Saving results ...")
    persist(index, OUT_INDEX)
    persist(idf, OUT_IDF)
    persist(doc_norms, OUT_DOCNORMS)
    persist(doc_meta, OUT_DOCMETA)

    print("[SUCCESS] Inverted Index created successfully")
    print("\n--- Summary ---")
    print("Indexed field types:")
    print(f"Text fields: {list(index['text'].keys())}")
    print(f"Numeric fields: {list(index['numeric'].keys())}")
    print(f"Keyword fields: {list(index['keyword'].keys())}")

    for f in FIELDS_TO_INDEX:
        print(f"  - {f}: {len(index['text'][f])} unique terms")

if __name__ == "__main__":
    parser = ArgumentParser(description="Build field-aware inverted index for player data.")
    parser.add_argument("--input", default=INPUT_CSV, help="Path to cleaned CSV-File")
    args = parser.parse_args()

    main(args.input)