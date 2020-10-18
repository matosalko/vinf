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

    res = es.get(index='record_idx', id=index.lower())
    
    return res


# zaindexuje riadky zo suboru
def index_file(file_name):
    
    with open(f"data/{file_name}", encoding='utf-8') as file:    
        for count, line in enumerate(file):
            results = re.findall('[^/]*/[^/]*>', line)
            
            subject = results[0].split('/')
            relation = results[1].split('/')
            object = results[2].split('/')

            record = Record(subject[0], subject[1][:-1], relation[1][:-1], object[0], object[1][:-1])

            id = record.subject.lower() 
            id = re.sub('[ \"*<|,>/?:]', '', id)  # odstrani znaky, ktore su zakazane v ES indexe
            id = re.sub('_\(.*\)', '', id)    # odstrani doplnujucu informaciu z indexu napr. Goo_(album) bude len Goo

            es.index(index='record_idx', id=id, body=record.toJSON())
            # break


es = Elasticsearch()
cwd = os.getcwd()
all_files = os.listdir(f"{cwd}/data")

index_file('yago-wd-facts.nt')