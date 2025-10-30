import pickle
from collections import Counter
from build_index import tf_weight, FIELD_BOOSTS, FIELDS_TO_INDEX
from operator import itemgetter


with open("indexes/index.pkl", "rb") as f: index = pickle.load(f)
with open("indexes/idf.pkl", "rb") as f: idf = pickle.load(f)
with open("indexes/doc_norms.pkl", "rb") as f: doc_norms = pickle.load(f)
with open("indexes/doc_meta.pkl", "rb") as f: doc_meta = pickle.load(f)
with open("indexes/ontology.pkl", "rb") as f: ontology = pickle.load(f)


def expand_query_with_ontology(term):
    term = term.lower()
    expansions = set()
    for rels in ontology.get("relationships", {}).values():
        for subj, objs in rels.items():
            if term in subj.lower() or any(term in o.lower() for o in objs):
                expansions.add(subj)
                expansions.update(objs)
    return list(expansions)


def parse_query(query):
    free_text = []
    field_filters = {}
    ontology_terms = []
    
    for t in query.split():
        if ":" in t:
            key, value = t.split(":", 1)
            key = key.lower()
            if key == "related_to":
                ontology_terms.append(value)
            else:
                field_filters.setdefault(key, []).extend(v.strip() for v in value.split(","))
        else:
            free_text.append(t)
    
    return free_text, field_filters, ontology_terms


def search(query, top_k=10):
    free_text, field_filters, ontology_terms = parse_query(query)
    tokens = set(free_text)
    for term in ontology_terms:
        tokens.update(expand_query_with_ontology(term))
    candidate_docs = set()
    if tokens:
        for field in FIELDS_TO_INDEX:
            for token in tokens:
                candidate_docs.update(index["text"][field].get(token, {}).keys())
    else:
        candidate_docs = set(doc_meta.keys())
    filtered_docs = set()
    for doc_id in candidate_docs:
        meta = doc_meta.get(doc_id, {})
        ok = True
        for field, allowed_values in field_filters.items():
            value_tokens = set(str(meta.get(field, "")).lower().split())
            allowed_tokens = set(v.lower() for val in allowed_values for v in val.split())
            if not value_tokens & allowed_tokens:
                ok = False
                break
        if ok:
            filtered_docs.add(doc_id)
    scores = Counter()
    for field in FIELDS_TO_INDEX:
        boost = FIELD_BOOSTS.get(field, 1.0)
        for token in tokens:
            if token not in idf[field]:
                continue
            postings = index["text"][field].get(token, {})
            for doc_id, tf_count in postings.items():
                if doc_id in filtered_docs:
                    scores[doc_id] += tf_weight(tf_count) * idf[field][token] * boost
    for doc_id in scores:
        scores[doc_id] /= doc_norms.get(doc_id, 1.0)
    if scores:
        results = sorted(scores.items(), key=itemgetter(1), reverse=True)[:top_k]
        return [(doc_meta[doc_id]["player_name"], score) for doc_id, score in results]
    else:
        return [(doc_meta[doc_id]["player_name"], 1.0) for doc_id in list(filtered_docs)[:top_k]]

if __name__ == "__main__":
    while True:
        q = input("Query: ")
        results = search(q)
        if not results:
            print("No results")
        else:
            for name, score in results:
                print(f"{name}  (score={score:.3f})")