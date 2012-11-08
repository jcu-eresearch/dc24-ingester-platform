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
        schema1 = {"class":"dataset_metadata_schema", "attributes": {"file":"file"}}
        schema1a = self.service.persist(schema1)
        dataset = {"class":"dataset", "schema": schema1a["id"], "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}
        dataset1a = self.service.persist(dataset)
        
        self.assertEquals(dataset["data_source"], dataset1a["data_source"])
        self.assertEquals(dataset["sampling"], dataset1a["sampling"])
        dataset1b = self.service.getDataset(dataset1a["id"])
        self.assertEquals(dataset1a, dataset1b)
        
    def test_unit(self):
        unit = {"insert":[{"id":-2, "class":"dataset", "location":-1, "schema": -3, "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}, {"id":-1, "latitude":30, "longitude": 20, "class":"location"}, {"id":-3, "attributes":{"file":"file"}, "class":"data_entry_schema"}], "delete":[], "update":[]}
        unit2 = self.service.commit(unit)
        for obj in unit2:
            if obj["class"] == "location":
                self.assertEquals(obj["correlationid"], -1)
            elif obj["class"] == "dataset":
                self.assertEquals(obj["correlationid"], -2)

if __name__ == '__main__':
    unittest.main()
