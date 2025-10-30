from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL, FOAF, XSD
import requests
import time
import re

EX = Namespace("http://example.org/ontology#Athlete/")
g = Graph()
g.parse("athletes.ttl", format="turtle")

print("Triples loaded:", len(g))

HEADERS = {
    "User-Agent": "KnowledgeDiscoveryProject/1.0 (fabian@example.com) Python/3.12"
}


def generate_name_variants(name):
    """Erzeugt verschiedene Namensvarianten für robustere Suche."""
    base = re.sub(r'\s+', ' ', name.strip())
    base_cap = " ".join([w.capitalize() for w in base.split()])
    dotted = re.sub(r'(?<=\w)\.(?=\w)', '. ', base_cap)
    variants = set([
        base,
        base_cap,
        dotted,
        dotted.replace(" .", "."),
        base_cap.replace(".", ""),
    ])
    return list(variants)


def find_wikidata_uri(name):
    """Sucht Wikidata-URI für einen gegebenen Namen (nur Basketball-Spieler)."""
    url = "https://www.wikidata.org/w/api.php"
    name_variants = generate_name_variants(name)

    for variant in name_variants:
        search_terms = [
            variant,
            f"{variant} basketball",
            f"{variant} basketball player"
        ]
        for term in search_terms:
            params = {
                "action": "wbsearchentities",
                "format": "json",
                "language": "en",
                "search": term
            }
            try:
                r = requests.get(url, params=params, headers=HEADERS, timeout=10)
                if r.status_code != 200:
                    continue
                data = r.json()
                results = data.get("search", [])
                if results:
                    for res in results:
                        label = res.get("label", "").lower()
                        desc = res.get("description", "").lower()
                        if "basketball" in label or "basketball" in desc:
                            uri = res["concepturi"]
                            print(f"[INFO] Found basketball match for {name}: {res['label']} -> {uri}")
                            return uri
                time.sleep(0.5)
            except Exception as e:
                print(f"[ERROR] Error searching for {name}: {e}")
        time.sleep(0.5)
    print(f"[INFO] No match found for {name}")
    return None


def fetch_wikidata_info(qid):
    endpoint = "https://query.wikidata.org/sparql"
    query = f"""
    SELECT ?citizenshipLabel ?sportLabel ?leagueLabel WHERE {{
      OPTIONAL {{ wd:{qid} wdt:P27 ?citizenship . }}
      OPTIONAL {{ wd:{qid} wdt:P641 ?sport . }}
      OPTIONAL {{ wd:{qid} wdt:P118 ?league . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "KnowledgeDiscoveryProject/1.0 (fabian@example.com)"
    }
    try:
        r = requests.get(endpoint, params={'query': query}, headers=headers, timeout=20)
        if r.status_code != 200:
            print(f"[ERROR] SPARQL error {r.status_code} for {qid}")
            return None
        data = r.json()
        results = data.get("results", {}).get("bindings", [])
        if not results:
            return None
        res = results[0]
        return {
            "citizenship": res.get("citizenshipLabel", {}).get("value"),
            "sport": res.get("sportLabel", {}).get("value"),
            "league": res.get("leagueLabel", {}).get("value")
        }
    except Exception as e:
        print(f"[ERROR] Error fetching data for {qid}: {e}")
        return None

count = 0
athlete_type = URIRef("http://example.org/ontology#Athlete/Athlete")

limit = 10

for i, athlete in enumerate(g.subjects(RDF.type, athlete_type)):
    # if i >= limit:
    #     break

    name = g.value(athlete, FOAF.name)
    if not name:
        name = g.value(athlete, RDFS.label)
    if not name:
        continue
    name = str(name)

    wikidata_uri = find_wikidata_uri(name)
    if not wikidata_uri:
        continue

    g.add((athlete, OWL.sameAs, URIRef(wikidata_uri)))
    count += 1

    qid = wikidata_uri.split("/")[-1]
    info = fetch_wikidata_info(qid)

    if info:
        if info["citizenship"]:
            g.add((athlete, EX.countryOfCitizenship, Literal(info["citizenship"], datatype=XSD.string)))
        if info["sport"]:
            g.add((athlete, EX.sport, Literal(info["sport"], datatype=XSD.string)))
        if info["league"]:
            g.add((athlete, EX.leagueOrCompetition, Literal(info["league"], datatype=XSD.string)))

        print(f"[INFO] Added data for {name}: {info}")

    time.sleep(1.5)

g.serialize("athletes_enriched.ttl", format="turtle")
print(f"[SUCCESS] Done. {count} athletes linked and enriched.")