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
        dataset = {"class":"dataset", "schema": {"file":"file"}, "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}
        dataset2 = self.service.persist(dataset)
        
        self.assertEquals(dataset["data_source"], dataset2["data_source"])
        self.assertEquals(dataset["sampling"], dataset2["sampling"])
        self.assertEquals(dataset["schema"], dataset2["schema"])
        
    def test_unit(self):
        unit = {"insert":[{"id":-2, "class":"dataset", "location":-1, "schema": {"file":"file"}, "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}, {"id":-1, "latitude":30, "longitude": 20, "class":"location"}], "delete":[], "update":[]}
        unit2 = self.service.commit(unit)
        for obj in unit2:
            if obj["class"] == "location":
                self.assertEquals(obj["correlationid"], -1)
            elif obj["class"] == "dataset":
                self.assertEquals(obj["correlationid"], -2)

if __name__ == '__main__':
    unittest.main()
