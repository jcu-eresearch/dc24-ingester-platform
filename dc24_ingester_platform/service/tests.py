"""This module tests the service CRUD functionality
"""
import unittest
import tempfile
import shutil
from dc24_ingester_platform.service import ingesterdb, repodb


class TestServiceModels(unittest.TestCase):
    def setUp(self):
        self.files = tempfile.mkdtemp()
        self.repo = repodb.RepositoryDB({"db":"sqlite://", "files":self.files})
        self.service = ingesterdb.IngesterServiceDB("sqlite://", self.repo)
        
    def tearDown(self):
        del self.service
        del self.repo
        shutil.rmtree(self.files)
        
    def test_data_types(self):
        schema1 = {"class":"dataset_metadata_schema", "attributes": {"file":"file"}}
        schema1a = self.service.persist(schema1)
        self.assertIn("attributes", schema1a)
        self.assertIn("file", schema1a["attributes"])
        schema2 = {"class":"data_entry_schema", "attributes": {"file":"file"}}
        schema2a = self.service.persist(schema2)

        dataset = {"class":"dataset", "schema": schema1a["id"], "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}
        # We've trying to use a dataset_metadata schema, so this should fail
        self.assertRaises(ValueError, self.service.persist, dataset)

        loc = {"class":"location", "latitude":0, "longitude":0}
        loca = self.service.persist(loc)

        dataset = {"class":"dataset", "schema": schema1a["id"], "location":loca["id"], "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}
        # We've trying to use a dataset_metadata schema, so this should fail
        self.assertRaises(ValueError, self.service.persist, dataset)
        dataset["schema"] = schema2a["id"]
        # Now we're using the correct type of schema
        dataset1a = self.service.persist(dataset)
        
        self.assertEquals(dataset["data_source"], dataset1a["data_source"])
        self.assertEquals(dataset["sampling"], dataset1a["sampling"])
        dataset1b = self.service.getDataset(dataset1a["id"])
        self.assertEquals(dataset1a, dataset1b)
        
        schema1b = self.service.getSchema(schema1a["id"])
        self.assertEquals(schema1a, schema1b)
        
    def test_region(self):
        region1 = {"class":"region", "name": "Region1", "region_points":[(1, 1), (1, 2)]}
        region1a = self.service.persist(region1)
        self.assertIn("region_points", region1a)
        self.assertEqual(2, len(region1a["region_points"]), "Not 2 region points")
        
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
