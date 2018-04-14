from detl.mydb import db_client
from detl.wrapper import Wrapper
from pprint import pprint

db = db_client()
acc_config = db.coll.find_one({'name':'accuracy'})
pprint(acc_config)
with db.as_default():   
    loaded_accuracy = Wrapper.from_hash(acc_config['config_hash'])
print('Loaded accuracy', loaded_accuracy.data)
