import re
import json

with open('data/yago-wd-facts.nt') as file:
    
    for line in file:
        result = re.findall('[^/]*/[^/]*>', line)
        
        subject = result[0].split('/')
        relation = result[1].split('/')
        object = result[2].split('/')

        item = {
            "subj_type": subject[0],
            "subject": subject[1][:-1],
            "relation": relation[1][:-1],
            "obj_type": object[0],
            "object": object[1][:-1]
        }

        print(item)
        break

