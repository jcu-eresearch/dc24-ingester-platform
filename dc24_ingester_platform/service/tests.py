"""This module tests the service CRUD functionality
"""
import unittest
import tempfile
import shutil
import datetime
from dc24_ingester_platform.service import ingesterdb, repodb, PersistenceError
from jcudc24ingesterapi.models.locations import Region, Location
from jcudc24ingesterapi.models.dataset import Dataset
from jcudc24ingesterapi.schemas.data_entry_schemas import DataEntrySchema
from jcudc24ingesterapi.schemas.metadata_schemas import DatasetMetadataSchema, DataEntryMetadataSchema
from jcudc24ingesterapi.schemas.data_types import FileDataType, String, Double
from jcudc24ingesterapi.ingester_platform_api import UnitOfWork
from jcudc24ingesterapi.models.data_sources import PullDataSource,\
    DatasetDataSource
from jcudc24ingesterapi.models.sampling import PeriodicSampling
from jcudc24ingesterapi.models.data_entry import DataEntry
from jcudc24ingesterapi.ingester_exceptions import InvalidObjectError,\
    InvalidCall

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
        schema2.addAttr(Double("x"))
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
        
        # Update and add a data source
        dataset1b.data_source = PullDataSource("http://www.abc.net.au", None, recursive=False, field="file", processing_script="TEST", sampling=PeriodicSampling(10000))
        dataset1b.enabled = True
        dataset1c = self.service.persist(dataset1b)
        self.assertNotEqual(None, dataset1c.data_source)
        self.assertEqual("TEST", dataset1c.data_source.processing_script)
        self.assertNotEqual(None, dataset1c.data_source.sampling)
        
        datasets = self.service.getActiveDatasets()
        self.assertEquals(1, len(datasets))
        self.assertNotEqual(None, datasets[0].data_source)
        self.assertEqual("TEST", datasets[0].data_source.processing_script)
        self.assertNotEqual(None, datasets[0].data_source.sampling)

        # Test with criteria
        datasets = self.service.getActiveDatasets(kind="pull_data_source")
        self.assertEquals(1, len(datasets))
        
        datasets = self.service.getActiveDatasets(kind="push_data_source")
        self.assertEquals(0, len(datasets))
        
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
        
        # Test ingest
        data_entry_1 = DataEntry(dataset1b.id, datetime.datetime.now())
        data_entry_1['x'] = 27.8              
        data_entry_1 = self.service.persist(data_entry_1)
        self.assertIsNotNone(data_entry_1.id)
        
    def test_region(self):
        #{"class":"region", "name": "Region1", "region_points":[(1, 1), (1, 2)]}
        region1 = Region("Region 1")
        region1.region_points = [(1, 1), (1, 2)]
        
        region1a = self.service.persist(region1)
        self.assertEqual(2, len(region1a.region_points), "Not 2 region points")
        
#    def test_unit(self):
#        unit = {"insert":[{"id":-2, "class":"dataset", "location":-1, "schema": -3, "data_source":{"class":"test", "param1":"1", "param2":"2"}, "sampling":{"class":"schedule1", "param1":"1", "param2":"2"}}, 
#                            {"id":-1, "latitude":30, "longitude": 20, "class":"location"}, 
#                            {"id":-3, "attributes":[{"name":"file", "class":"file"}], "class":"data_entry_schema"}], "delete":[], "update":[], "enable":[], "disable":[]}
#        unit2 = self.service.commit(unit, None)
#        for obj in unit2:
#            if obj["class"] == "location":
#                self.assertEquals(obj["correlationid"], -1)
#            elif obj["class"] == "dataset":
#                self.assertEquals(obj["correlationid"], -2)

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
        self.assertEquals("file2", schema2.attrs["file2"].name)
        
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

    def test_state_persistence(self):
        """Test that the state of samplers and data sources can be persisted."""
        sampler_state = self.service.getSamplerState(1)
        self.assertEquals(0, len(sampler_state))
        self.service.persistSamplerState(1, {"test":"abc","test2":123})
        sampler_state = self.service.getSamplerState(1)
        self.assertEquals(2, len(sampler_state))
        self.assertEquals("abc", sampler_state["test"])
        self.assertEquals("123", sampler_state["test2"])
        
        del sampler_state["test"]
        sampler_state["test2"] = "xyz"
        self.service.persistSamplerState(1, sampler_state)
        sampler_state = self.service.getSamplerState(1)
        self.assertEquals(1, len(sampler_state))
        self.assertEquals("xyz", sampler_state["test2"])
        
        # Now test the same thing on the data source state
        data_source_state = self.service.getDataSourceState(1)
        self.assertEquals(0, len(data_source_state))
        self.service.persistDataSourceState(1, {"test":"abc","test2":123})
        data_source_state = self.service.getDataSourceState(1)
        self.assertEquals(2, len(data_source_state))
        self.assertEquals("abc", data_source_state["test"])
        self.assertEquals("123", data_source_state["test2"])
        
        del data_source_state["test"]
        data_source_state["test2"] = "xyz"
        self.service.persistDataSourceState(1, data_source_state)
        data_source_state = self.service.getDataSourceState(1)
        self.assertEquals(1, len(data_source_state))
        self.assertEquals("xyz", data_source_state["test2"])
                
                
    def test_dataset_data_source_unit(self):
        """This test creates a simple schema hierarchy, and tests updates, etc"""
        unit = UnitOfWork(None)
        
        schema1 = DataEntrySchema("base1")
        schema1.addAttr(FileDataType("file"))
        schema_id = unit.post(schema1)
        
        loc = Location(10.0, 11.0)
        loc.name = "Location"
        loc_id = unit.post(loc)
        
        dataset1 = Dataset()
        dataset1.schema = schema_id   
        dataset1.location = loc_id
        dataset1_id = unit.post(dataset1)
        
        dataset2 = Dataset()
        dataset2.schema = schema_id   
        dataset2.location = loc_id
        dataset2.data_source = DatasetDataSource(dataset1_id, "")
        dataset2_id = unit.post(dataset2)
        
        ret = self.service.commit(unit, None)
        
        found = False
        for r in ret:
            if isinstance(r, Dataset) and dataset1_id == r.correlationid:
                dataset1_id = r.id
            elif isinstance(r, Dataset) and dataset2_id == r.correlationid:
                self.assertEquals(dataset1_id, r.data_source.dataset_id, "Data source dataset_id was not updated")
                found = True
        
        self.assertTrue(found, "Didn't find the dataset with the dataset data source")

    def test_region_persist(self):
        """Test that the region persists correctly, including version numbering, and that
        region points are correctly updated"""
        region = Region("Region 1")
        region.region_points = [(1, 1), (1, 2)]
        
        region1 = self.service.persist(region)
        self.assertEquals(1, region1.version)
        region1.version = 0
        
        self.assertRaises(InvalidObjectError, self.service.persist, region1)
        
        region1.version = 1
        region1.region_points = [(99,100)]
        region2 = self.service.persist(region1)
        self.assertEquals(2, region2.version)
        self.assertEquals(1, len(region2.region_points))
        self.assertEquals((99, 100), region2.region_points[0])
        
    def test_location_persist(self):
        loc = Location(10.0, 11.0)
        loc.name = "Location"
        loc1 = self.service.persist(loc)
        self.assertEquals(1, loc1.version)
        loc1.version = 0
        
        self.assertRaises(InvalidObjectError, self.service.persist, loc1)
        
        loc1.version = 1
        loc2 = self.service.persist(loc1)
        self.assertEquals(2, loc2.version)
        
    def test_schema_persist(self):
        schema = DataEntrySchema("base1")
        schema.addAttr(FileDataType("file"))
        
        schema1 = self.service.persist(schema)
        self.assertEquals(1, schema1.version)
        schema1.version = 0
        
        self.assertRaises(InvalidCall, self.service.persist, schema1)
        
        schema1.version = 1
        self.assertRaises(InvalidCall, self.service.persist, schema1)
        
    def test_dataset_persist(self):
        schema = DataEntrySchema("base1")
        schema.addAttr(FileDataType("file"))
        schema = self.service.persist(schema)
        
        loc = Location(10.0, 11.0)
        loc.name = "Location"
        loc = self.service.persist(loc)
        
        dataset = Dataset()
        dataset.schema = schema.id   
        dataset.location = loc.id

        dataset1 = self.service.persist(dataset)
        self.assertEquals(1, dataset1.version)
        
        dataset1.version = 0
        
        self.assertRaises(InvalidObjectError, self.service.persist, dataset1)
        
        dataset1.version = 1
        dataset2 = self.service.persist(dataset1)
        self.assertEquals(2, dataset2.version)
        
if __name__ == '__main__':
    unittest.main()
