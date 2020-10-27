import re
from elasticsearch import Elasticsearch
import os

from time import time


class Record():

    def __init__(self, sub_type, subject, relation, obj_type, object, name):
        self.sub_type = sub_type
        self.subject =  subject
        self.relation = relation
        self.obj_type = obj_type
        self.object = object
    
        self.name = name    # na zaklade tohto atributu sa bude dany zaznam vyhladavat

    def toJSON(self):
        return self.__dict__


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
    res = es.search(index="record_idx", body=body)

    return res['hits']['hits']


def index_data(file_name, index_name):

    with open(f"data/{file_name}", encoding='utf-8') as file:    
        for count, line in enumerate(file):
            
            if re.findall('_:', line):  # ignoruje riadky, ktore nie su reprezentovane ako N-triple
                continue

            line = re.sub('#', '/', line)
            results = re.findall('[^/]*/[^/]*>', line)

            subject = results[0].split('/')
            relation = results[1].split('/')
            
            id = subject[1][:-1].lower() 
            id = re.sub('[ \"\.*<|,>/?:]', '', id)  # odstrani nejake specialne znaky
            id = re.sub('_\(.*\)', '', id)    # odstrani doplnujucu informaciu napr. Goo_(album) bude len Goo

            if len(results) == 3:
                object = results[2].split('/')
                record = Record(subject[0], subject[1][:-1], relation[1][:-1], object[0], object[1][:-1], id)
            
            else:
                object = re.findall('\".*\"', line)
                record = Record(subject[0], subject[1][:-1], relation[1][:-1], 'text', object[0], id)
            

            es.index(index=index_name, id=count, body=record.toJSON())
            # print(record.toJSON())



es = Elasticsearch()
cwd = os.getcwd()
all_files = os.listdir(f"{cwd}/data")

for file in all_files:
    print(f"indexing file: {file}")

    index_name = re.findall('[^-.*]*\.nt', file)
    index_name = re.sub('.nt', '_idx', index_name[0])
    index_name = index_name.lower()

    start = time()
    index_data(file, index_name)
    end = time()

    print(f"time needed for indexing: {end - start}")