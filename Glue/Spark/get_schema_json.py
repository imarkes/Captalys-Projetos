import datetime
import json

path = '/home/ivan/Documentos/Singulare/ESTOQUE_FIDC_BULLA_20210311.csv'

def muda(docs):
    if docs =='liquidados':
        docs = 'liquidacao'
    print(docs)


muda('liquidados')
# def get_types(obj):
#     integer = r'^[\+0-9]\d+$'
#     character_varying = r'^[\a-zA-Z]$',
#     if isinstance(obj, int):
#         return 'integer'
#     if isinstance(obj, str):
#         return 'character varying'

#
# def get_schema_types():
#     schema = []
#     with open(path, 'r', encoding='cp1252') as result:
#         for rr in result:
#             resp = rr.strip().split(';')
#             data_type = ''
#             for col in resp:
#                 if 'DATA' in col:
#                     data_type = 'timestamp'
#                 elif 'VALOR' in col:
#                     data_type = 'double precision'
#                 else:
#                     data_type = 'character varying'
#                 schema.append(dict(
#                     table_name='estoque',
#                     column_name=col.strip(),
#                     data_type=data_type,
#                     primary_key=False
#
#                 ))
#             break
#     schema_json = json.dumps(schema, indent=4)
#     schema_file = open(f"./estoque.json", "w")
#     schema_file.write(schema_json)
#     schema_file.close()
#
#
# get_schema_types()
