import unittest
from batchify import batchify

class TestBatchifyFunction(unittest.TestCase):
    
    def test_happy_path(self):
        records = ["a" * 100, "b" * 200, "c" * 300]
        result = list(batchify(records))
        expected = [["a" * 100, "b" * 200, "c" * 300]]
        self.assertEqual(result, expected)

    def test_record_size_equlas_to_limit(self):
        records = ["a" * (1024**2), "b" * 100, "c" * 200]
        result = list(batchify(records))
        expected = [["a" * (1024**2), "b" * 100, "c" * 200]]
        self.assertEqual(result, expected)            
    
    def test_oversized_records(self):
        records = ["a" * (1024**2 + 100), "b" * 100, "c" * 200]
        result = list(batchify(records))
        expected = [["b" * 100, "c" * 200]]
        self.assertEqual(result, expected)    

    def test_max_batch_size(self):
        records = ["a" * (1024 * 1024), "b" * (1024 * 1024)]
        result = list(batchify(records, batch_max_size=1024 * 1024 + 100))
        expected = [["a" * (1024 * 1024)], ["b" * (1024 * 1024)]]
        self.assertEqual(result, expected)

    def test_batch_size_equlas_to_limit(self):
        records = ["a" * (1024 * 1024), "b" * (1024 * 1024)]
        result = list(batchify(records, batch_max_size=1024 * 1024))
        expected = [["a" * (1024 * 1024)], ["b" * (1024 * 1024)]]
        self.assertEqual(result, expected)    

    def test_max_batch_records(self):
        records = ["a"] * 700
        result = list(batchify(records, batch_max_records=500))
        expected = [["a"] * 500, ["a"] * 200]
        self.assertEqual(result, expected)

    def test_records_number_equals_to_limit(self):
        records = ["a"] * 500
        result = list(batchify(records, batch_max_records=500))
        expected = [["a"] * 500]
        self.assertEqual(result, expected)     

    def test_custom_record_max_size(self):
        records = ["a" * 400, "b" * 600, "c" * 500]
        result = list(batchify(records, record_max_size=500))
        expected = [["a" * 400, "c" * 500]]
        self.assertEqual(result, expected)      

    def test_large_batch(self):
        records = ["a" * (1024 * 1024) for _ in range(550)]
        result = list(batchify(records))
        self.assertEqual(len(result), 110)
        for batch in result:
            self.assertEqual(len(batch), 5)         

    def test_custom_size_function(self):
        records = ["a" * 10, "b" * 10]
        result = list(batchify(records, size_func=lambda r: len(r) * 2, batch_max_size=20, record_max_size=20))  # Set batch_max_size to 20
        expected = [["a" * 10], ["b" * 10]]
        self.assertEqual(result, expected)

    def test_empty_records(self):
        records = []
        result = list(batchify(records))
        expected = []
        self.assertEqual(result, expected)

    def test_single_record(self):
        records = ["a" * 100]
        result = list(batchify(records))
        expected = [["a" * 100]]
        self.assertEqual(result, expected)   

    def test_all_records_too_large(self):
        records = ["a" * (1024**2 + 1) for _ in range(3)]
        result = list(batchify(records))
        expected = []
        self.assertEqual(result, expected) 

    def test_invalid_record(self):
        records = ["a" * 100, None, "b" * 200]
        result = list(batchify(filter(None, records)))
        expected = [["a" * 100, "b" * 200]]
        self.assertEqual(result, expected)

    def test_batch_size_close_to_limit(self):
        records = ["a" * (1024 * 1024 - 100), "b" * 100]
        result = list(batchify(records, batch_max_size=1024 * 1024))
        expected = [["a" * (1024 * 1024 - 100), "b" * 100]]
        self.assertEqual(result, expected)     

    def test_size_func_utf8(self):
        records = ["a", "é", "😊"]  # ASCII, Latin-1, Emoji
        # "a" (1 byte), "é" (2 bytes), "😊" (4 bytes)
        result = list(batchify(records, size_func=lambda r: len(r.encode('utf-8')), batch_max_size=5, record_max_size=5))
        expected = [["a", "é"], ["😊"]]  
        self.assertEqual(result, expected)    

    def test_invalid_size_func(self):
        records = ["a", "b"]
        with self.assertRaises(TypeError):
            list(batchify(records, size_func=lambda r: "string_size", record_max_size=10, batch_max_size=10))  # Invalid size function

if __name__ == "__main__":
    unittest.main()
