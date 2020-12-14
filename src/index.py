import re
from elasticsearch import Elasticsearch, helpers
import os
from time import time

class Record():
    """Objekt reprezentujuci jeden zaznam, ktory sa bude indexovat"""

    def __init__(self, sub_type, subject, relation, obj_type, object, name):
        self.sub_type = sub_type
        self.subject =  subject
        self.relation = relation
        self.obj_type = obj_type
        self.object = object
    
        self.name = name

    def toJSON(self):
        return self.__dict__


def index_data(file_name, index_name):
    """Funkcia, ktora parsuje vstupny file po riadkoch a vytvara z nich objekty, ktore sa maju zaindexovat

    Args:
        file_name (string): nazov suboru, ktory sa ma spracovat
        index_name (string): nazov indexu, pod ktory sa maju jednotlive zaznamy indexovat

    Yields:
        dict: elasticsearch query, pomozou ktoreho sa zaindexuje prave jeden zaznam
    """

    with open(f"data_all/{file_name}", encoding='utf-8') as file:    
        for line in file:
            
            if re.findall('_:', line):  # ignoruje riadky, ktore nie su reprezentovane ako N-triple
                continue

            if re.findall('<geo:', line):   # upravi sa riadok s geo suradnicami, aby bol dalej parsovatelny
                line = re.sub('<geo:', '<http://geo/', line)

            line = re.sub('#', '/', line)
            results = re.findall('[^/]*/[^/]*>', line)

            subject = results[0].split('/')
            relation = results[1].split('/')
            
            id = subject[1][:-1].lower() 
            id = re.sub('[ \"\.*<|,>/?:]', '', id)  # odstrani nejake specialne znaky
            id = re.sub('_\(.*\)', '', id)    # odstrani doplnujucu informaciu napr. Goo_(album) bude len Goo

            if len(results) == 3:
                if object := re.findall('\".*\"\^\^', line):
                    object = object[0]
                    object = re.sub('(\"|\^)', '', object)

                    record = Record(subject[0], subject[1][:-1], relation[1][:-1], 'info', object, id)

                # ak sa jedna o link na wikidata, tu sa vyparsuje
                elif relation[0] + relation[1] == 'owlsameAs>':
                    object = re.findall('<http://(www.wikidata.*>|dbpedia.*>|rdf.freebase.*>)', line)

                    object = object[0][:-1]
                    record = Record(subject[0], subject[1][:-1], relation[1][:-1], 'link', object, id)

                else:    
                    object = results[2].split('/')
                    record = Record(subject[0], subject[1][:-1], relation[1][:-1], object[0], object[1][:-1], id)
            
            else:
                object = re.findall('\".*\"', line)              
                record = Record(subject[0], subject[1][:-1], relation[1][:-1], 'text', object[0], id)

            yield {
                "_index": index_name,
                "_source": record.toJSON(),
            }


es = Elasticsearch()
cwd = os.getcwd()
all_files = os.listdir(f"{cwd}/data_all")

for file in all_files:
    print(f"indexing file: {file}")

    start = time()
    helpers.bulk(es, index_data(file, 'yago_idx'))  # zaindexuje jednotlive riadky
    end = time()

    print(f"time needed for indexing: {end - start}")
