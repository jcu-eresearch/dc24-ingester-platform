# Project DC24 Ingester Platform

This project aims to implement a scheduling system, with XMLRPC management interface for ingesting into a repository data from:
* Video
* Audio
* Manual input
* Sensors (SOS)

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
