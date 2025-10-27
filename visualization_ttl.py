import re
from rdflib import Graph, URIRef
from graphviz import Digraph
from tqdm import tqdm

def make_node_id(s):
    s = str(s)
    s_id = re.sub(r'[^a-zA-Z0-9_-]', '_', s)
    return s_id


g = Graph()
g.parse("athletes_enriched.ttl", format="turtle")

dot = Digraph(comment="Athletes Ontology")

triples = list(g)
for s, p, o in tqdm(triples, desc="Tripel verarbeiten"):
    if not isinstance(o, URIRef):
        continue

    subj_id = make_node_id(s)
    obj_id = make_node_id(o)
    pred = str(p).split("/")[-1]

    dot.node(subj_id, str(s).split("/")[-1], shape="box", style="filled", color="yellow")
    dot.node(obj_id, str(o).split("/")[-1], shape="box", style="filled", color="orange")
    dot.edge(subj_id, obj_id, label=pred)

dot.engine = "dot"
dot.render("athletes_ontology_diagram_fast", format="png", cleanup=True)
