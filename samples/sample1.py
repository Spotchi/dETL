import pandas as pd
import numpy as np
import json
from io_utils import pd_to_csv, json_dump
from detl.processor import Processor, load_and_save, change_state
from detl.identity import Identity, identify
from detl.mydb import MyDb, db_client

class PreProcessor(Processor):

    def __init__(self, num_mul=2):
        '''
        The positional and
        :param num_mul:
        '''
        super(PreProcessor, self).__init__(num_mul=num_mul)
        # TODO : move all assignments to Processor class and make sure they're immutable
        self.num_mul = num_mul
    
    @change_state
    def set_num(self, new_mul):
        self.num_mul = new_mul

    @load_and_save(pd.read_csv, pd_to_csv)
    def processor_1(self, df_arg):
        return df_arg.drop(['B', 'C'], axis=1)

    @load_and_save(pd.read_csv, pd_to_csv)
    def processor_2(self, df_arg):
        return df_arg * self.num_mul

    @load_and_save(json.loads, json_dump)
    def statistics(self, df_arg):
        return {'A nice statistic': 42}


sample_db = db_client()

with sample_db.as_default():

    df = pd.DataFrame(np.arange(12).reshape(3,4),
                      columns=['A', 'B', 'C', 'D'])
    df_ided = identify(df, 'data source')

    preprocessor = PreProcessor(num_mul=50)

    mult = preprocessor.processor_2(df_ided)
# statistics = preprocessor.statistics(df_ided)
#preprocessor.set_num(5)
#mult_2 = preprocessor.processor_2(df_ided)
#stats2 = preprocessor.statistics(df_ided)
    print(mult)
# print(statistics)
#print(mult_2)
#print(stats2)
