import pickle
from pathlib import Path
from collections import defaultdict
from argparse import ArgumentParser
from rdflib import Graph, RDF, RDFS, URIRef, Literal

OUT_ONTOLOGY = "indexes/ontology.pkl"
OUT_ONTOLOGY_META = "indexes/ontology_meta.pkl"

def persist(obj, fname):
    with open(fname, "wb") as f:
        pickle.dump(obj, f)


def extract_ontology(ttl_path: str):
    g = Graph()
    g.parse(ttl_path, format="turtle")

    ontology = {
        "classes": defaultdict(set),
        "properties": defaultdict(set),
        "relationships": {},
        "labels": {},
    }
    meta = defaultdict(dict)

    for s, p, o in g:
        if o == RDFS.Class:
            ontology["classes"][str(s)].add(str(s))

        elif o == RDF.Property:
            ontology["properties"][str(s)].add(str(s))

        elif p == RDFS.label and isinstance(o, Literal):
            ontology["labels"][str(s)] = str(o)

        elif isinstance(o, URIRef):
            p_str, s_str, o_str = str(p), str(s), str(o)
            if p_str not in ontology["relationships"]:
                ontology["relationships"][p_str] = defaultdict(set)
            ontology["relationships"][p_str][s_str].add(o_str)

        elif isinstance(o, Literal):
            meta[str(s)][str(p)] = str(o)

    return ontology, meta

def main(input_ttl: str):
    ontology, meta = extract_ontology(input_ttl)
    persist(ontology, OUT_ONTOLOGY)
    persist(meta, OUT_ONTOLOGY_META)

if __name__ == "__main__":
    parser = ArgumentParser(description="Build ontology index")
    parser.add_argument("--input", default="athletes.ttl", help="Path to RDF/Turtle file")
    args = parser.parse_args()
    main(args.input)
