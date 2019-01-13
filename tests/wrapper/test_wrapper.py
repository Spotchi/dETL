import unittest
from detl.wrapper.wrapper import Wrapper, Returner, FunctionWrapper, Product, unpack_wrapper
from detl.core.result_group import ResultGroup, ResultFunction
from detl.core.saving import FileSaver


class TestWrapper(unittest.TestCase):

    def test_returner_initialization_and_run(self):

        a = 3
        w_a = Returner(a, 'a')

        res_a = w_a.run()
        res_a_data = res_a._data
        self.assertEqual(a, res_a_data, "The result should be the same as the initial value")

    def test_function_wrapper_returns_result(self):

        def mult_2(x):
            return 2 * x

        a = 4
        b = mult_2(a)

        w_a = Returner(a, 'a')
        rg = ResultFunction(mult_2, FileSaver(), '2_multiplier')
        # TODO : this is map function
        w_b = FunctionWrapper(rg, w_a)
        res_b = w_b.run()
        res_b_data = res_b._data

        self.assertEqual(b, res_b_data)

    def test_product_wrapper_returns_a_tuple(self):

        a = 'hello'
        b = 'world'

        w_a = Returner(a, 'a')
        w_b = Returner(b, 'b')

        w_prod = Product(w_a, w_b)
        prod_res = w_prod.run()
        prod_val = prod_res._data

        self.assertIsInstance(prod_val, tuple)
        self.assertEqual(prod_val[0], a)
        self.assertEqual(prod_val[1], b)
        self.assertEqual(len(prod_val), 2)

    def test_coproduct_wrapper_unpacks_correct_number_of_elems(self):
        a = 'hello'
        b = 'world'

        # w_c = Returner((a, b))
        w_a = Returner(a, 'a')
        w_b = Returner(b, 'b')
        w_c = Product(w_a, w_b)
        w_u_a, w_u_b = unpack_wrapper(w_c, 2)

        val_a = w_u_a.run()._data
        val_b = w_u_b.run()._data
        self.assertEqual(val_a, a)
        self.assertEqual(val_b, b)

    def test_identities_are_different_with_different_scalars(self):
        a = 'hello'
        b = 'whazaaa'

        w_first = Returner(a, 'a')
        w_sec = Returner(b, 'b')

        def append_universe(str):
            return str + ' universe'

        rg = ResultFunction(append_universe, FileSaver(), 'uni')
        w_hel = FunctionWrapper(rg, w_first)
        w_wha = FunctionWrapper(rg, w_sec)

        self.assertNotEqual(w_hel.identity().__id_hash__(), w_wha.identity().__id_hash__())
        self.assertIsInstance(w_hel.identity().__id_hash__(), str)
        self.assertIsInstance(w_wha.identity().__id_hash__(), str)


    def test_original_run_is_not_called_when_recomputing_result(self):
        pass
