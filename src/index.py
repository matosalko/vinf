import re
from elasticsearch import Elasticsearch
import os


class Record():

    def __init__(self, sub_type, subject, relation, obj_type, object):
        self.sub_type = sub_type
        self.subject =  subject
        self.relation = relation
        self.obj_type = obj_type
        self.object = object

    def toJSON(self):
        return self.__dict__


# vyhlada zadany retazec
def search(index):

    res = es.search(index=index.lower(), body={"query": {"match_all": {}}})
    
    return res['hits']['hits']


# zaindexuje riadky zo suboru
def index_file(file_name):
    
    with open(f"data/{file_name}", encoding='utf-8') as file:    
        for count, line in enumerate(file):
            results = re.findall('[^/]*/[^/]*>', line)
            
            subject = results[0].split('/')
            relation = results[1].split('/')
            object = results[2].split('/')

            record = Record(subject[0], subject[1][:-1], relation[1][:-1], object[0], object[1][:-1])

            index = record.subject.lower() 
            index = re.sub('[ \"*<|,>/?:]', '', index)  # odstrani znaky, ktore su zakazane v ES indexe
            index = re.sub('_\(.*\)', '', index)    # odstrani doplnujucu informaciu z indexu napr. Goo_(album) bude len Goo

            # print(f"indexing #{count} {index}")

            es.index(index=index, id=count, body=record.toJSON())
            es.indices.refresh(index=index)


es = Elasticsearch()
cwd = os.getcwd()
all_files = os.listdir(f"{cwd}/data")

index_file('yago-wd-facts.nt')