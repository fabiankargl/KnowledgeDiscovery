import pickle
from collections import Counter
import re
from build_index import simple_tokenize, tf_weight, FIELD_BOOSTS, FIELDS_TO_INDEX


with open("indexes/index.pkl", "rb") as f: index = pickle.load(f)
with open("indexes/idf.pkl", "rb") as f: idf = pickle.load(f)
with open("indexes/doc_norms.pkl", "rb") as f: doc_norms = pickle.load(f)
with open("indexes/doc_meta.pkl", "rb") as f: doc_meta = pickle.load(f)
with open("indexes/ontology.pkl", "rb") as f: ontology = pickle.load(f)


def expand_query_with_ontology(term):
    term = term.lower()
    expansions = set()
    for pred, rels in ontology.get("relationships", {}).items():
        for subj, objs in rels.items():
            if term in subj.lower() or any(term in o.lower() for o in objs):
                expansions.update([subj, *objs])
    return list(expansions)


def parse_query(query):
    field_filters = {}
    ontology_terms = []
    free_text = []

    tokens = query.split()
    for t in tokens:
        m = re.match(r"(\w+):(.+)", t)
        if m:
            key, value = m.groups()
            key_lower = key.lower()
            if key_lower == "related_to":
                ontology_terms.append(value)
            else:
                values = [v.strip() for v in value.split(",")]
                field_filters.setdefault(key_lower, []).extend(values)
        else:
            free_text.append(t)
    return free_text, field_filters, ontology_terms

def search(query: str, top_k: int = 10):
    free_text, field_filters, ontology_terms = parse_query(query)
    tokens = []
    for term in free_text:
        tokens.extend(simple_tokenize(term))
    expanded_tokens = set(tokens)
    for term in ontology_terms:
        expanded_tokens.update(expand_query_with_ontology(term))

    scores = Counter()

    for field in FIELDS_TO_INDEX:
        boost = FIELD_BOOSTS.get(field, 1.0)
        field_lower = field.lower()
        filter_terms = field_filters.get(field, None)
        for term in expanded_tokens:
            if filter_terms and term.lower() not in [t.lower() for t in filter_terms]:
                continue
            if term not in idf[field]:
                continue
            postings = index["text"][field].get(term, {})
            for doc_id, tf_count in postings.items():
                scores[doc_id] += tf_weight(tf_count) * idf[field][term] * boost
    for doc_id in scores:
        scores[doc_id] /= doc_norms.get(doc_id, 1.0)

    results = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
    return [(doc_meta[doc_id]["player_name"], score) for doc_id, score in results]

if __name__ == "__main__":
    print("Type 'exit' or 'quit' to stop the program.\n")
    while True:
        q = input("Query: ")
        results = search(q)
        if not results:
            print("No results found.")
        else:
            for name, score in results:
                print(f"{name}  (score={score:.3f})")
