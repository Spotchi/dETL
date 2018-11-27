from detl.store.mydb import db_client
from pprint import pprint

db = db_client()

cursor = list(db.list_results('accuracy'))
rec_list = [db.recursive_get(res) for res in cursor]
pprint(rec_list)
print(len(rec_list))
filtered = list(filter(lambda x: db.has_ancestor(x, {'kwargs':{'kernel': 'poly'}}), cursor))
pprint(db.recursive_get(filtered[0]))
pprint(len(filtered))


'''
cursor = list(db.list_results('accuracy'))
rec_list = [db.recursive_get(res) for res in cursor]
pprint(rec_list)
'''
