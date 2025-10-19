import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import ast
import re

df = pd.read_csv("datasets/players_normalized.csv", sep=None, engine="python")

nltk.download("stopwords")
stop_words = set(stopwords.words("english"))
stemmer = SnowballStemmer("english")

def clean_transaction(transaction):
    words = re.split(r'[^a-zA-Z0-9]+', transaction)
    return " ".join(stemmer.stem(w) for w in words if w.strip() and w not in stop_words)

def clean_transactions_list(transactions_list):
    if not transactions_list or str(transactions_list).strip() == "":
        return []
    if isinstance(transactions_list, str):
        try:
            transactions_list = ast.literal_eval(transactions_list)
        except:
            transactions_list = [transactions_list]
    if not isinstance(transactions_list, list):
        transactions_list = [transactions_list]
    return [clean_transaction(tx) for tx in transactions_list if tx and str(tx).strip()]

transaction_col = next((c for c in df.columns if "transactions" in c), None)
if transaction_col is not None:
    df["transactions list"] = df[transaction_col].apply(clean_transactions_list)

df.to_csv("datasets/players_normalized_stopwords_stemming.csv", index=False)
