This project has been developed as part of EnMaSSe (Environmental Monitoring and Sensor Storage) and is related to:

* [Documentation](https://github.com/jcu-eresearch/TDH-Rich-Data-Capture-Documentation) - Contains full user, administrator and developer guides.
* [Deployment](https://github.com/jcu-eresearch/EnMaSSe-Deployment) - Recommended way to install
* [Provisioning Interface](https://github.com/jcu-eresearch/TDH-rich-data-capture) - User interface/website for EnMaSSe
* [Ingester API](https://github.com/jcu-eresearch/jcu.dc24.ingesterapi) - API for integrating the EnMaSSe provisioning interface with the ingester platform (this)
* [SimpleSOS](https://github.com/jcu-eresearch/python-simplesos) - Library used for the SOSScraperDataSource.
 
**Unless you are using the Ingester Platform as a standalone component it is recommended that you use the [Deployment](https://github.com/jcu-eresearch/EnMaSSe-Deployment) repository for installation.**

# EnMaSSe Ingester Platform

This project aims to implement a scheduling system, with XMLRPC management interface for ingesting into a repository data from:
* External files (PullDataSource)
* Manual input (FormDataSource)
* Sensors (SOSScraperDataSource)
* Chained processing from ingested data (DatasetDataSource)

The project is implemented using Twisted.

## Getting Started

1. Clone the project
2. Run **buildout** to setup the required dependencies
3. Run the service using **./bin/twistd -n -y dc24_ingester_platform.tac**
4. Now you can run the client, currently in an IPython instance (see below)
5. To run the unit tests use **./bin/tests**

Additionally, you may need to clone, and **python setup.py develop** install
the API client into the this projects virtual env.

### Client example usage

```python
import dc24_ingester_platform.client as client
c=client.Client("http://localhost:8080")
ds = client.Dataset()
ds.latitude = 9
c.insert(ds)
ds2=c.insert(ds)
```

### Running as a unit testable service

In order to run service so that it can be integrated with unit tests more easily, there is a special
implementation of the service that allows all the data to be reset remotely. To use this version start
the service with the folling command: **./bin/twistd -n -y dc24_ingester_platform_test_service.tac**


Credits
-------

This project is supported by [the Australian National Data Service (ANDS)](http://www.ands.org.au/) through the National Collaborative Research Infrastructure Strategy Program and the Education Investment Fund (EIF) Super Science Initiative, as well as through the [Queensland Cyber Infrastructure Foundation (QCIF)](http://www.qcif.edu.au/).

License
-------

See `LICENCE.txt`.
