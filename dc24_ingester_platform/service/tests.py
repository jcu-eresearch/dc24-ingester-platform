"""This module tests the service CRUD functionality
"""
import unittest
from dc24_ingester_platform.service import ingesterdb


class TestServiceModels(unittest.TestCase):
    def setUp(self):
        self.service = ingesterdb.IngesterServiceDB("sqlite://")
        
    def tearDown(self):
        del self.service
        
    def test_data_types(self):
        dataset = {"latitude":30, "longitude": 20, "schema": {"file":"file"}, "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}
        dataset2 = self.service.persistDataset(dataset)
        
        self.assertEquals(dataset["latitude"], dataset2["latitude"])
        self.assertEquals(dataset["longitude"], dataset2["longitude"])
        self.assertEquals(dataset["data_source"], dataset2["data_source"])
        self.assertEquals(dataset["sampling"], dataset2["sampling"])
        self.assertEquals(dataset["schema"], dataset2["schema"])

if __name__ == '__main__':
    unittest.main()