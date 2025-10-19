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

    classes = ['Player', 'Team', 'College', 'HighSchool', 'City', 'Country', 'Transaction', 'Dataset']
    for cls in classes:
        g.add((EX[cls], RDF.type, RDFS.Class))

    props = ['position', 'shoots', 'birthDate', 'weight', 'age', 'draftInfo', 
             'teamLocation', 'stadium', 'foundingYear']
    for p in props:
        g.add((EX[p], RDF.type, RDF.Property))
    g.add((EX.hasTransaction, RDF.type, RDF.Property))

    return g, EX

def row_to_rdf(g: Graph, EX, row, line_num):
    raw_name = row.get('player_name')
    name = raw_name.strip() if raw_name else None

    if name:
        player_uri = EX['player/' + slugify(name)]
    else:
        player_uri = EX[f'player/row-{line_num}']

    g.add((player_uri, RDF.type, EX.Player))
    if name:
        g.add((player_uri, RDFS.label, Literal(name)))
        g.add((player_uri, FOAF.name, Literal(name)))

    profile = row.get('profile_url')
    if profile:
        g.add((player_uri, FOAF.page, URIRef(profile)))

    for col, prop in [('position_clean', EX.position), ('shoots', EX.shoots)]:
        val = row.get(col)
        if val and val != '-':
            g.add((player_uri, prop, Literal(val)))

    birthday = row.get('birthday')
    if birthday:
        try:
            bd = parse(birthday.strip())
            g.add((player_uri, EX.birthDate, Literal(bd.date(), datatype=XSD.date)))
        except:
            g.add((player_uri, EX.birthDate, Literal(birthday)))

    for col, prop in [('weight', EX.weight), ('age', EX.age)]:
        val = row.get(col)
        if val and val.isdigit():
            g.add((player_uri, prop, Literal(int(val), datatype=XSD.integer)))

    college = row.get('college')
    if college and college != '-':
        college_uri = EX['college/' + slugify(college)]
        g.add((college_uri, RDF.type, EX.College))
        g.add((college_uri, RDFS.label, Literal(college)))
        g.add((player_uri, EX.college, college_uri))

    hs = row.get('high_school')
    if hs and hs != '-':
        hs_uri = EX['highschool/' + slugify(hs)]
        g.add((hs_uri, RDF.type, EX.HighSchool))
        g.add((hs_uri, RDFS.label, Literal(hs)))
        g.add((player_uri, EX.highSchool, hs_uri))

    for col, cls, prop in [('birth_city', 'City', EX.birthCity), ('birth_country', 'Country', EX.birthCountry)]:
        val = row.get(col)
        if val and val != '-':
            val_uri = EX[f'{cls.lower()}/{slugify(val)}']
            g.add((val_uri, RDF.type, getattr(EX, cls)))
            g.add((val_uri, RDFS.label, Literal(val)))
            g.add((player_uri, prop, val_uri))

    draft = row.get('draft')
    if draft and draft != '-':
        g.add((player_uri, EX.draftInfo, Literal(draft)))

    tx_list_str = row.get('transactions_list')
    if tx_list_str and tx_list_str != '[]':
        try:
            tx_list = ast.literal_eval(tx_list_str)
            for i, tx in enumerate(tx_list, 1):
                tx_uri = EX[f'transaction/{slugify(name)}-{i}' if name else f'transaction/row-{line_num}-{i}']
                g.add((tx_uri, RDF.type, EX.Transaction))
                g.add((tx_uri, RDFS.label, Literal(tx)))
                g.add((player_uri, EX.hasTransaction, tx_uri))
        except:
            g.add((player_uri, EX.hasTransaction, Literal(tx_list_str)))

def main():
    base_ns = 'https://groupg.kdir/basketball'
    output_file = 'players.ttl'
    rdf_format = 'turtle'
    os.makedirs('datasets', exist_ok=True)

    g, EX = create_graph(base_ns)

    input_csv = 'datasets/players_clean_abbr.csv'
    with open(input_csv, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for i, row in enumerate(reader, start=1):
            row_to_rdf(g, EX, row, i)

    dataset_uri = URIRef(base_ns + '/dataset/players')
    g.add((dataset_uri, RDF.type, EX.Dataset))
    g.add((dataset_uri, DCTERMS.created, Literal(datetime.now(tz=timezone.utc).isoformat(), datatype=XSD.dateTime)))
    g.add((dataset_uri, DCTERMS.creator, Literal('rdfize_players.py script')))

    g.serialize(destination=output_file, format=rdf_format)

if __name__ == '__main__':
    main()
