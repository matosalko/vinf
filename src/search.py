from elasticsearch import Elasticsearch
import re
from pprint import pprint

# pomocna metoda vypise pocet zaznamov v danom indexe
def get_num_docs(index_name):
    es.indices.refresh(index_name)
    print(es.cat.count(index_name, params={"format": "json"}))


# vyhlada zadany retazec
def find_records(record_name, record_label, index):
    record_name = re.sub(' ', '_', record_name)
    if record_label == 'name':
        record_name = re.sub('[ \"\.*<|,>/?:]', '', record_name)  # odstrani nejake specialne znaky
        record_name = re.sub('_\(.*\)', '', record_name)    # odstrani doplnujucu informaciu napr. Goo_(album) bude len Goo
    
    body = {
        'query': {
            'match': {
                record_label: record_name
            }
        }
    }
    res = es.search(index=index, body=body, size=10000)

    return res['hits']['hits']


def search():
    query = input('Co hladas? ')

    ret = find_records(query, 'name', 'facts_idx')

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

    try:
        wanted_item = subs[int(input('Vyber zo zoznamu: '))]

        end = {}
        ret = find_records(wanted_item, 'subject', 'yago_idx')
        
        for hit in ret:
            if hit['_source']['subject'] == wanted_item:
                relation = hit['_source']['relation']
                if relation in end:
                    end[relation].append(hit['_source']['object'])
                else:
                    end[relation] = [hit['_source']['object']]
        pprint(end)
    
    except:
        print('Nespravny index')

es = Elasticsearch()
search()