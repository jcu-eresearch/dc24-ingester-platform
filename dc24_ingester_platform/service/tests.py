"""This module tests the service CRUD functionality
"""
import unittest
import tempfile
import shutil
from dc24_ingester_platform.service import ingesterdb, repodb, PersistenceError
from jcudc24ingesterapi.models.locations import Region, Location
from jcudc24ingesterapi.models.dataset import Dataset
from jcudc24ingesterapi.schemas.data_entry_schemas import DataEntrySchema
from jcudc24ingesterapi.schemas.metadata_schemas import DatasetMetadataSchema, DataEntryMetadataSchema
from jcudc24ingesterapi.schemas.data_types import FileDataType, String, Double
from jcudc24ingesterapi.ingester_platform_api import UnitOfWork

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
        schema1 = DatasetMetadataSchema("schema1")
        schema1.addAttr(FileDataType("file"))
        schema1a = self.service.persist(schema1)
        
        self.assertEquals(1, len(schema1a.attrs))

        schema2 = DataEntrySchema("schema2")
        schema2.addAttr(FileDataType("file"))
        schema2a = self.service.persist(schema2)
        
        loc = Location(10.0, 11.0)
        loca = self.service.persist(loc)

        dataset = Dataset()
        dataset.schema = schema1a.id   
        dataset.location = loca.id
        # We've trying to use a dataset_metadata schema, so this should fail
        self.assertRaises(ValueError, self.service.persist, dataset)

        dataset.schema = schema2a.id
        # Now we're using the correct type of schema
        dataset1a = self.service.persist(dataset)
        
        dataset1b = self.service.getDataset(dataset1a.id)
        self.assertEquals(dataset1a.id, dataset1b.id)
        self.assertDictEqual(dataset1a.__dict__, dataset1b.__dict__)
        
        schema1b = self.service.getSchema(schema1a.id)
        self.assertEquals(schema1a.id, schema1b.id)
        
        datasets = self.service.search("dataset")
        self.assertEquals(1, len(datasets))
        
        schemas = self.service.search("data_entry_schema")
        self.assertEquals(1, len(schemas))
        
        schemas = self.service.search("dataset_metadata_schema")
        self.assertEquals(1, len(schemas))        
        
        locs = self.service.search("location")
        self.assertEquals(1, len(locs))
        
    def test_region(self):
        #{"class":"region", "name": "Region1", "region_points":[(1, 1), (1, 2)]}
        region1 = Region("Region 1")
        region1.region_points = [(1, 1), (1, 2)]
        
        region1a = self.service.persist(region1)
        self.assertEqual(2, len(region1a.region_points), "Not 2 region points")
        
    def test_unit(self):
        unit = {"insert":[{"id":-2, "class":"dataset", "location":-1, "schema": -3, "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}, 
                            {"id":-1, "latitude":30, "longitude": 20, "class":"location"}, 
                            {"id":-3, "attributes":[{"name":"file", "class":"file"}], "class":"data_entry_schema"}], "delete":[], "update":[], "enable":[], "disable":[]}
        unit2 = self.service.commit(unit, None)
        for obj in unit2:
            if obj["class"] == "location":
                self.assertEquals(obj["correlationid"], -1)
            elif obj["class"] == "dataset":
                self.assertEquals(obj["correlationid"], -2)

    def test_schema_persistence(self):
        """This test creates a simple schema hierarchy, and tests updates, etc"""
        schema1 = DataEntrySchema("base1")
        schema1.addAttr(FileDataType("file"))
        schema1 = self.service.persist(schema1)
        self.assertGreater(schema1.id, 0, "ID does not appear valid")
        self.assertEquals(1, len(schema1.attrs))
        
        schema2 = DataEntrySchema("child1")
        schema2.addAttr(FileDataType("file2"))
        schema2.extends.append(schema1.id)
        schema2 = self.service.persist(schema2)
        self.assertGreater(schema2.id, 0, "ID does not appear valid")
        self.assertEquals(1, len(schema2.attrs))
        
    def test_schema_persistence_unit(self):
        """This test creates a simple schema hierarchy, and tests updates, etc"""
        unit = UnitOfWork(None)
        ids = []
        
        schema1 = DataEntrySchema("base1")
        schema1.addAttr(FileDataType("file"))
        ids.append(unit.post(schema1))
        
        schema2 = DataEntrySchema("child1")
        schema2.addAttr(FileDataType("file2"))
        schema2.extends.append(schema1.id)
        ids.append(unit.post(schema2))
        
        ret = self.service.commit(unit, None)
        
        for obj in ret:
            self.assertGreater(obj.id, 0)
            self.assertIn(obj.correlationid, ids)

    def test_schema_persistence_clash(self):
        """This test creates a simple schema hierarchy, that has a field name clash"""
        schema1 = DataEntrySchema("base1")
        schema1.addAttr(FileDataType("file"))
        schema1 = self.service.persist(schema1)
        self.assertGreater(schema1.id, 0, "ID does not appear valid")
        self.assertEquals(1, len(schema1.attrs))
        
        schema2 = DataEntrySchema("child1")
        schema2.addAttr(FileDataType("file"))
        schema2.extends.append(schema1.id)
        
        self.assertRaises(PersistenceError, self.service.persist, schema2)

if __name__ == '__main__':
    unittest.main()
