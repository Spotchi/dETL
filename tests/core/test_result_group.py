import unittest

from detl.core.result_group import ResultGroup, rg_dict, get_result_group_from_name


class TestResultGroup(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def testResultIsAdded(self):

        rg_name = 'Run_forest'
        rg = ResultGroup(None, rg_name)

        self.assertTrue(get_result_group_from_name(rg_name).namespace, rg_name)

