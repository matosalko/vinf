from elasticsearch import Elasticsearch
import re

# vypise pocet zaznamov v danom indexe
def get_num_docs(index_name):
    es.indices.refresh(index_name)
    print(es.cat.count(index_name, params={"format": "json"}))


# vyhlada zadany retazec
def search(record_name):
    record_name = record_name.lower() 
    record_name = re.sub('[ \"*<|,>/?:]', '', record_name)
    
    body = {
        'query': {
            'match': {
                'name': record_name
            }
        }
    }
    res = es.search(index="facts_idx", body=body, size=100)

    return res['hits']['hits']


es = Elasticsearch()

query = input('Co hladas? ')

ret = search(query)

subjects = set()
for hit in ret:
    subject = hit['_source']['subject']
    subjects.add(subject)

subs = []
for i in subjects:
    subs.append(i)

for i, sub in enumerate(subs):
    print(f"[{i}] {sub}")

query = int(input('Vyber zo zoznamu: '))

print(subs[query])

# get_num_docs('sameas_idx')