import re
import os
from tqdm import tqdm
import json
import statistics
from beautifultable import BeautifulTable

NEED_PARSE = False

categories = {}
entities = {}
all_types = {}

def stats(file_name):
    with open(f"../data_all/{file_name}", encoding='utf-8') as file:    
        for line in tqdm(file):
            
            if re.findall('_:', line):  # ignoruje riadky, ktore nie su reprezentovane ako N-triple
                continue

            if re.findall('<geo:', line):   # upravi sa riadok s geo suradnicami, aby bol dalej parsovatelny
                line = re.sub('<geo:', '<http://geo/', line)

            line = re.sub('#', '/', line)
            results = re.findall('[^/]*/[^/]*>', line)

            subject = results[0].split('/')
            relation = results[1].split('/')

            if len(results) == 3:
                if re.findall('\".*\"\^\^', line):
                    object = re.findall('\".*\"\^\^', line)
                    object = object[0]
                    object = re.sub('(\"|\^)', '', object)

                elif relation[0] + relation[1] == 'owlsameAs>':
                    object = re.findall('<http://(www.wikidata.*>|dbpedia.*>|rdf.freebase.*>)', line)
                    object = object[0][:-1]

                else:    
                    object = results[2].split('/')
                    object = object[1][:-1]
            
            else:
                object = re.findall('\".*\"', line)
                object = object[0]

            # pocty jendotilych kategorii
            relation = relation[1][:-1]
            if relation in categories:
                    categories[relation] += 1
            else:
                categories[relation] = 1

            # pocty kategorii pre jednotlive entitiy
            subject = subject[1][:-1]
            if subject in entities:
                if relation in entities[subject]:
                    entities[subject][relation] += 1
                else:
                    entities[subject][relation] = 1
            else:
                entities[subject] = {}
                entities[subject][relation] = 1

            if relation == 'type':
                if object in all_types:
                    all_types[object] += 1
                else:
                    all_types[object] = 1

    with open('../stats/categories.json', 'w', encoding='utf-8') as file:
        json.dump(categories, file)
    with open('../stats/entities.json', 'w', encoding='utf-8') as file:
        json.dump(entities, file)   
    with open('../stats/types.json', 'w', encoding='utf-8') as file:
        json.dump(all_types, file)       


def make_categories_dict(categories):
    categories_dict = {}
    for key in categories:
        categories_dict[key] = 0

    with open('../stats/entities.json', encoding='utf-8') as file:
        data = json.load(file)
        for key in data:
            for key2 in data[key]:
                if key2 == 'all' or key2 == 'unique':
                    continue
                if re.findall('comment', key2):
                    categories_dict['comment'] += 1
                elif re.findall('alternateName', key2):
                    categories_dict['alternateName'] += 1
                elif re.findall('label', key2):
                    categories_dict['label'] += 1
                else:
                    categories_dict[key2] += 1

    return categories_dict


def categories_stats():
    with open('../stats/categories.json', encoding='utf-8') as file:
        categories = json.load(file)

        values = []
        for key in categories:
            values.append(categories[key])

        n_categories = len(values)
        mean = statistics.mean(values)
        median = statistics.median(values)

        categories = make_categories_dict(categories)
        categories = dict(reversed(sorted(categories.items(), key=lambda item: item[1])))
        
        table = BeautifulTable()
        table.columns.header = ['Nazov kategorie', 'Pocet entit obsahujucich kategoriu']
        for key in categories:
            table.rows.append([key, categories[key]])

        table2 = BeautifulTable()
        table2.columns.header = ['Nazov kategorie', '% pocet entit obsahujucich kategoriu']
        for key in categories:
            table2.rows.append([key, round((categories[key] / 5878287) * 100, 3)])

        with open('../stats/categories_table.txt', 'w') as file:
            file.write(str(table))
        with open('../stats/categories_table_percentage.txt', 'w') as file:
            file.write(str(table2))

        with open('../stats/categories.txt', 'w') as file:
            file.write(f'Celkovy pocet kategorii: {n_categories}\n')
            file.write(f'Priemerny pocet vyskytov kategorie v entitach: {round(mean, 3)}\n')
            file.write(f'Median vyskytov kategorie v entitach: {median}\n')
            file.write(f'\n10 najcastejsie vyskytujucich sa kategorii:\n')
            file.write(str(table[:10]))
            file.write(f'\n\nPercentualny pocet vyskytov v entitach: \n')
            file.write(str(table2[:10]))


def entities_stats():
    with open('../stats/entities.json', encoding='utf-8') as file:
        data = json.load(file)
        unique = {}
        for key in data:
            unique[key] = len(data[key])
        
        unique = dict(reversed(sorted(unique.items(), key=lambda item: item[1])))

        values = []
        for key in unique:
            values.append(unique[key])

        n_entities = len(values)
        mean = statistics.mean(values)
        median = statistics.median(values)

        table = BeautifulTable()
        table.columns.header = ['Nazov entity', 'Pocet unikatnych kategorii']
        for key in unique:
            table.rows.append([key, unique[key]])

        table2 = BeautifulTable()
        table2.columns.header = ['Nazov entity', '% pocet unikatnych kategorii v entite']
        for key in unique:
            table2.rows.append([key, round((unique[key] / 141) * 100, 3)])

        with open('../stats/entities.txt', 'w') as file:
            file.write(f'Celkovy pocet entit: {n_entities}\n')
            file.write(f'Priemerny pocet unikatnych kategorii v entitach: {round(mean, 3)}\n')
            file.write(f'Median poctu unikatnych kategorii v entitach: {median}\n\n')
            file.write(f'10 entit s najviac unikatnymi kategoriammi: \n')
            file.write(str(table[:10]))
            file.write(f'\n\n10 entit s najviac unikatnymi kategoriammi percentualne: \n')
            file.write(str(table2[:10]))


def types_stats():
    with open('../stats/types.json', encoding='utf-8') as file:
        all_types = json.load(file)

        table = BeautifulTable()
        table.columns.header = ['Typ', '% pocet entit daneho typu']
        for key in all_types:
            table.rows.append([key, round((all_types[key] / 5878287) * 100, 3)])

        with open('../stats/types_table.txt', 'w', encoding='utf-8') as file:
            file.write(str(table))


def main():
    all_files = os.listdir(f"../data_all")

    if NEED_PARSE:
        for file in all_files:
            stats(file)

    categories_stats()
    entities_stats()
    types_stats()


if __name__ == "__main__":
    main()