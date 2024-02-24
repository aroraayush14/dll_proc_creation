import json

def create_hash_key_from_json(json_file):
    with open(json_file, 'r') as jsonfile:
        data = json.load(jsonfile)
        
        return data



json_file = 'pk_info.json'

dt = create_hash_key_from_json(json_file)


for data in dt.items():
    ab = data['Primary Key']

print(ab)