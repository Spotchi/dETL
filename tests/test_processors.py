import detl
from detl.processor import load_and_save, identity_wrapper, Processor, change_state
from detl.mydb import MyDb
import shutil
import os
from test_util import save_int, load_int
import unittest
import pytest
from detl.data_wrap import DatasetWrapper
from detl.identity import identify

class DecoratorTest(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def init_db(self):

        host = 'localhost'
        port = 27017
        db = 'detl_test_db'
        collection = 'decorator_test_coll'
        dummy_data_folder = 'dummy_data'

        if os.path.exists(dummy_data_folder):
            shutil.rmtree(dummy_data_folder)
        os.mkdir(dummy_data_folder)

        self.db = MyDb(host, port, db, collection, dummy_data_folder)
        self.db.drop_all()


    # Make sure that the function is not recomputed the second time we run load_and_save
    # Make sure that if a function is called with the same arguments, the config_hash is found in the db
    def test_save_and_load(self):
        
        pytest.execution_count = 0

        @load_and_save(load_int, save_int)
        def multiply_by(first_int, second_int):
            print('executint')
            pytest.execution_count += 1

            return DatasetWrapper(first_int.data * second_int.data)

        with self.db.as_default():
            a = DatasetWrapper(4)
            a = identify(a, '4')
            c = DatasetWrapper(11)
            c = identify(c, '11')

            ac = multiply_by(a, c)
            assert pytest.execution_count == 1
            ac = multiply_by(a, c)
            assert pytest.execution_count == 1
            
            b = DatasetWrapper(5)
            b = identify(b, '5')
            bc = multiply_by(b, c)
            assert pytest.execution_count == 2

        self.db.drop_all()


    # Make sure that when something is identified, it is saved to the db and can be retrieved even if there is no
    # file saved to disk
    # I KNOW THERE IS SOMETHING FISHY WITH THAT
    def test_identify(self):

        pass




    # Make sure that the change_state decorator is saving a DIFFERENT config to the db everytime
    # I KNOW IT DOES NOT
    def test_change_state(self):

        class DummyModel(Processor):

            def __init__(self, number):
                    
                super(DummyModel, self).__init__(number)
                self.number = number
            
            @change_state
            def set_num(self, new_num):
                self.number = new_num
        
            @load_and_save(load_int, save_int)
            def multiply_by(self, other_int):

                return DatasetWrapper(self.number * other_int.data)

        with self.db.as_default():

            mod = DummyModel(46)

            a = DatasetWrapper(2)
            a = identify(a, '2')
            
            print(mod.multiply_by(a))
            mod.set_num(20)
            print(mod.multiply_by(a))

#        self.db.drop_all()
        assert False
