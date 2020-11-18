from elasticsearch import Elasticsearch
import re
from pprint import pprint

# vypise pocet zaznamov v danom indexe
def get_num_docs(index_name):
    es.indices.refresh(index_name)
    print(es.cat.count(index_name, params={"format": "json"}))


# vyhlada zadany retazec
def find_records(record_name, index):
    record_name = re.sub(' ', '_', record_name)
    record_name = re.sub('[ \"\.*<|,>/?:]', '', record_name)  # odstrani nejake specialne znaky
    record_name = re.sub('_\(.*\)', '', record_name)    # odstrani doplnujucu informaciu napr. Goo_(album) bude len Goo
    

    body = {
        'query': {
            'match': {
                'name': record_name
            }
        }
    }
    res = es.search(index=index, body=body, size=100)

    return res['hits']['hits']


def find_records2(record_name, index):
    body = {
        'query': {
            'match': {
                'subject': record_name
            }
        }
    }
    res = es.search(index=index, body=body, size=100)

    return res['hits']['hits']


def search():
    query = input('Co hladas? ')

    ret = find_records(query, 'facts_idx')

    subjects = set()
    for hit in ret:
        subject = hit['_source']['subject']
        subjects.add(subject)

    subs = []
    for i in subjects:
        subs.append(i)

    if not subs:
        print('Nic sa nenaslo.')
        return

    for i, sub in enumerate(subs):
        print(f"[{i}] {sub}")

    query = int(input('Vyber zo zoznamu: '))

    wanted_item = subs[query]

    end = {}
    for index in indexes:
        ret = find_records2(wanted_item, index)
        
        for hit in ret:
            if hit['_source']['subject'] == wanted_item:
                relation = hit['_source']['relation']

                if relation in end:
                    end[relation].append(hit['_source']['object'])
                else:
                    end[relation] = [hit['_source']['object']]

    pprint(end)

es = Elasticsearch()

indexes = ['facts_idx', 'class_idx', 'full_types_idx', 'simple_types_idx', 'labels_idx', 'sameas_idx', 'schema_idx']
search()
# get_num_docs('sameas_idx')