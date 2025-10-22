from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, FOAF, XSD, DCTERMS
import csv
import re
import unicodedata
from datetime import datetime, timezone
import ast
import os
from dateutil.parser import parse

def slugify(value: str) -> str:
    value = unicodedata.normalize('NFKD', value)
    value = value.encode('ascii', 'ignore').decode('ascii')
    value = value.lower().strip()
    value = re.sub(r'[^a-z0-9]+', '-', value).strip('-')
    return value.strip('-')

def create_graph(base_ns: str):
    g = Graph()
    EX = Namespace(base_ns.rstrip('/') + '/')
    g.bind('ex', EX)
    g.bind('foaf', FOAF)
    g.bind('rdfs', RDFS)
    g.bind('dct', DCTERMS)

    classes = ['Athlete', 'Team', 'Education', 'City', 'Country', 'CareerEvent', 'Dataset']
    for cls in classes:
        g.add((EX[cls], RDF.type, RDFS.Class))
    props = ['position', 'shoots', 'birthDate', 'weight', 'age', 'hasDraft', 'playsFor', 'hasTransaction', 'attendedCollege', 'attendedHighSchool', 'birthCity', 'birthCountry']
    for p in props:
        g.add((EX[p], RDF.type, RDF.Property))
    g.add((EX.hasTransaction, RDF.type, RDF.Property))
    return g, EX

def row_to_rdf(g: Graph, EX, row, line_num):
    raw_name = row.get('player_name')
    name = raw_name.strip() if raw_name else None
    if name:
        athlete_uri = EX['athlete/' + slugify(name)]
    else:
        athlete_uri = EX[f'athlete/row-{line_num}']
    g.add((athlete_uri, RDF.type, EX.Athlete))
    if name:
        g.add((athlete_uri, RDFS.label, Literal(name)))
        g.add((athlete_uri, FOAF.name, Literal(name)))

    profile = row.get('profile_url')
    if profile:
        g.add((athlete_uri, FOAF.page, URIRef(profile)))

    for col, prop in [('position_clean', EX.position), ('shoots', EX.shoots)]:
        val = row.get(col)
        if val and val != '-':
            g.add((athlete_uri, prop, Literal(val)))

    birthday = row.get('birthday')
    if birthday:
        try:
            bd = parse(birthday.strip())
            g.add((athlete_uri, EX.birthDate, Literal(bd.date(), datatype=XSD.date)))
        except:
            g.add((athlete_uri, EX.birthDate, Literal(birthday)))

    for col, prop in [('weight', EX.weight), ('age', EX.age)]:
        val = row.get(col)
        if val and val.isdigit():
            g.add((athlete_uri, prop, Literal(int(val), datatype=XSD.integer)))

    college = row.get('college')
    if college and college != '-':
        college_uri = EX['education/' + slugify(college)]
        g.add((college_uri, RDF.type, EX.College))
        g.add((college_uri, RDFS.label, Literal(college)))
        g.add((athlete_uri, EX.attendedCollege, college_uri))

    hs = row.get('high_school')
    if hs and hs != '-':
        hs_uri = EX['education/' + slugify(hs)]
        g.add((hs_uri, RDF.type, EX.HighSchool))
        g.add((hs_uri, RDFS.label, Literal(hs)))
        g.add((athlete_uri, EX.attendedHighSchool, hs_uri))

    for col, cls, prop in [('birth_city', 'City', EX.birthCity), ('birth_country', 'Country', EX.birthCountry)]:
        val = row.get(col)
        if val and val != '-':
            val_uri = EX[f'{cls.lower()}/{slugify(val)}']
            g.add((val_uri, RDF.type, getattr(EX, cls)))
            g.add((val_uri, RDFS.label, Literal(val)))
            g.add((athlete_uri, prop, val_uri))

    draft_str = row.get('draft')
    if draft_str and draft_str != '-':
        parts = [p.strip() for p in draft_str.split(',')]
        team_name = parts[0] if parts else None
        year_match = re.search(r'(\d{4})', draft_str)

        draft_uri = EX[f'careerEvent/{slugify(name)}-draft']
        g.add((draft_uri, RDF.type, EX.CareerEvent))
        g.add((draft_uri, RDFS.label, Literal(draft_str)))

        if year_match:
            year = year_match.group(1)
            g.add((draft_uri, EX.draftYear, Literal(year, datatype=XSD.gYear)))

        if team_name:
            team_uri = EX['team/' + slugify(team_name)]
            g.add((team_uri, RDF.type, EX.Team))
            g.add((team_uri, RDFS.label, Literal(team_name)))
            g.add((draft_uri, EX.draftedBy, team_uri))
            g.add((athlete_uri, EX.playsFor, team_uri))
        g.add((athlete_uri, EX.hasDraft, draft_uri))

    tx_list_str = row.get('transactions_list')
    if tx_list_str and tx_list_str != '[]':
        try:
            tx_list = ast.literal_eval(tx_list_str)
            for i, tx in enumerate(tx_list, 1):
                tx_uri = EX[f'transaction/{slugify(name)}-{i}' if name else f'transaction/row-{line_num}-{i}']
                g.add((tx_uri, RDF.type, EX.Transaction))
                g.add((tx_uri, RDFS.label, Literal(tx)))
                g.add((athlete_uri, EX.hasTransaction, tx_uri))
        except:
            g.add((athlete_uri, EX.hasTransaction, Literal(tx_list_str)))

def main():
    base_ns = 'http://example.org/ontology#Athlete'
    output_file = 'athletes.ttl'
    rdf_format = 'turtle'
    os.makedirs('datasets', exist_ok=True)

    g, EX = create_graph(base_ns)

    input_csv = 'datasets/players_clean_abbr.csv'
    with open(input_csv, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for i, row in enumerate(reader, start=1):
            row_to_rdf(g, EX, row, i)

    dataset_uri = URIRef(base_ns + '/dataset/athletes')
    g.add((dataset_uri, RDF.type, EX.Dataset))
    g.add((dataset_uri, DCTERMS.created, Literal(datetime.now(tz=timezone.utc).isoformat(), datatype=XSD.dateTime)))

    g.serialize(destination=output_file, format=rdf_format)

if __name__ == '__main__':
    main()
