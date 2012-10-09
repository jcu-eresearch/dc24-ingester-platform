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

### Client example usage

```python
import dc24_ingester_platform.client as client
c=client.Client("http://localhost:8080")
ds = client.Dataset()
ds.latitude = 9
c.insert(ds)
ds2=c.insert(ds)
```

