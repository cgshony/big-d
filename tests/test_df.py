import unittest

from dataframe import DataFrame

class TestDataFrame(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"name": ["Гошо", "Пешо"], "age": [30, 16]})
    
    def test_initialize_invalid_input(self):
        with self.assertRaises(ValueError):
            DataFrame([("Гошо", "Пешо"), ("age"), [30, 16]])

    def tearDown(self):
        del self.df
    
    def test_df(self):
        self.assertEqual(self.df.shape, (2,2))

    def test_shape(self):
        self.assertEqual

    def test_getitem(self):
        self.assertEqual(self.df['age'], [30,16])

    def test_setitem(self):
        height = [175, "по-висок от Стан"]
        self.df["height"] = height
        self.assertEqual(self.df.shape, (2,3))
        self.assertEqual(self.df['height'], height)

    def test_inconsistent_column_len(self):
        """When initializing DF with inconsistentcolumns length, an Error must be raised!"""
        with self.assertRaises(ValueError):
            DataFrame({"name": ["Гошо", "Пешо"], "age": [16]})

    def test_incompatible_data_types(self):
        """When initializing DF with inconsistent data types, an Error must be raised!"""
        with self.assertRaises(TypeError):
            DataFrame({"name": [22, "Пешо"], "age": [30, 16]})

    def test_setitem_invalid(self):
        pass

if __name__ == '__main__':
    unittest.main()