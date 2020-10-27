from elasticsearch import Elasticsearch
import re

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
    res = es.search(index="facts_idx", body=body)

    return res['hits']['hits']


es = Elasticsearch()