import requests, lxml

from lxml import etree

class SOSVersions:
    v_1_0_0 = "1.0.0"

class SOSMimeTypes:
    sensorML_1_0_1 = 'text/xml;subtype="sensorML/1.0.1"'
    om_1_0_0 = 'text/xml;subtype="om/1.0.0"'

def create_namespace_dict(default_prefix=None):
    namespaces = {
        "sos": "http://www.opengis.net/sos/1.0",
        "om":"http://www.opengis.net/om/1.0",
        "ows":"http://www.opengis.net/ows/1.1",
        "ogc":"http://www.opengis.net/ogc",
        "xsi":"http://www.w3.org/2001/XMLSchema-instance"
    }
    toRet = {}
    for ns in namespaces:
        if ns is default_prefix:
            toRet[None] = namespaces[ns]
        else:
            toRet[ns] = namespaces[ns]
    return toRet


class SOS:
    __default_output_format = 'text/xml;subtype="sensorML/1.0.1"'
    def __init__(self, sos_url, version):
        self.sos_url = sos_url
        self.version = version

    namespaces = create_namespace_dict("sos")

    def get_namespaced_tag(self, ns, tag):
        if ns in self.namespaces:
            return "{%s}%s"%(self.namespaces[ns], tag)
        return "{%s}%s"%(self.namespaces[None], tag)

    def create_GetCapabilities(self, sections):
        """
        generate a GetCapabilities document. The call:

            create_GetCapabilities(["OperationsMetadata","ServiceIdentification", "ServiceProvider", "Filter_Capabilities", "Contents"]

        creates the following document:

        <?xml version="1.0" encoding="UTF-8"?>
        <GetCapabilities xmlns="http://www.opengis.net/sos/1.0"
                         xmlns:ows="http://www.opengis.net/ows/1.1"
                         xmlns:ogc="http://www.opengis.net/ogc"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://www.opengis.net/sos/1.0 http://schemas.opengis.net/sos/1.0.0/sosGetCapabilities.xsd"
                         service="SOS">
            <ows:AcceptVersions>
                <ows:Version>1.0.0</ows:Version>
            </ows:AcceptVersions>
            <ows:Sections>
                <ows:Section>OperationsMetadata</ows:Section>
                <ows:Section>ServiceIdentification</ows:Section>
                <ows:Section>ServiceProvider</ows:Section>
                <ows:Section>Filter_Capabilities</ows:Section>
                <ows:Section>Contents</ows:Section>
            </ows:Sections>
        </GetCapabilities>
        """
        caps = etree.Element("{http://www.opengis.net/sos/1.0}GetCapabilities", service="SOS", nsmap = self.namespaces)

        doc = etree.ElementTree(caps)

        tmp = etree.SubElement(caps, self.get_namespaced_tag("ows", "AcceptVersions"))
        tmp = etree.SubElement(tmp, self.get_namespaced_tag("ows", "Version"))
        tmp.text = self.version

        tmp = etree.SubElement(caps, self.get_namespaced_tag("ows","Sections"))
        for i in sections:
            sec = etree.SubElement(tmp, self.get_namespaced_tag("ows","Section"))
            sec.text = i

        return doc

    def getCapabilities(self, sections):
        caps = etree.tostring(
            self.create_GetCapabilities(
                ["OperationsMetadata","ServiceIdentification", "ServiceProvider", "Filter_Capabilities", "Contents"]),
            pretty_print=True)
        response = requests.post(self.sos_url, data=caps)
        return etree.fromstring(response.text.encode("utf8"))


    def create_DescribeSensor(self, sensorID, outputFormat):
        """
        generate a DescribeSensor document.

        <?xml version="1.0" encoding="UTF-8"?>
        <DescribeSensor version="1.0.0" service="SOS"
            xmlns="http://www.opengis.net/sos/1.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.opengis.net/sos/1.0
            http://schemas.opengis.net/sos/1.0.0/sosDescribeSensor.xsd"
            outputFormat="text/xml;subtype=&quot;sensorML/1.0.1&quot;">

            <procedure>urn:ogc:object:feature:Sensor:IFGI:ifgi-sensor-1</procedure>

        </DescribeSensor>
        """
        describe = etree.Element("{http://www.opengis.net/sos/1.0}DescribeSensor",
            service="SOS",
            outputFormat=outputFormat,
            version=self.version,
            nsmap = self.namespaces
        )
        doc = etree.ElementTree(describe)
        procedure = etree.SubElement(describe, self.get_namespaced_tag("sos", "procedure"))
        procedure.text = sensorID
        return doc

    def describeSensor(self, sensorID, outputFormat=__default_output_format):
        caps = etree.tostring(
            self.create_DescribeSensor(sensorID, outputFormat),
            pretty_print=True)
        response = requests.post(self.sos_url, data=caps)
        return etree.fromstring(response.text.encode("utf8"))

    def create_GetObservationByID(self, o_id, resultFormat="om:Measurement"):
        """
        <GetObservationById
            xmlns="http://www.opengis.net/sos/1.0"
            xmlns:om="http://www.opengis.net/om/1.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.opengis.net/sos/1.0 http://schemas.opengis.net/sos/1.0.0/sosAll.xsd"
            service="SOS" version="1.0.0" srsName="EPSG:4326">

            <ObservationId>urn:MyOrg:Observation:5678</ObservationId>
            <resultModel>om:Observation</resultModel>

        </GetObservationById>
        """
        gobid = etree.Element("{http://www.opengis.net/sos/1.0}GetObservationById",
            service="SOS",
            version=self.version,
            srsName="urn:ogc:def:crs:EPSG:4326",
            nsmap = self.namespaces
        )
        oid = etree.SubElement(gobid, self.get_namespaced_tag("sos", "ObservationId"))
        oid.text = o_id
        result = etree.SubElement(gobid, self.get_namespaced_tag("sos", "resultModel"))
        result.text = resultFormat
        return etree.ElementTree(gobid)

    def getObservationByID(self, o_id, resultFormat="om:Measurement"):
        observation = etree.tostring(
            self.create_GetObservationByID(o_id, resultFormat),
            pretty_print=True)
        response = requests.post(self.sos_url, data=observation)
        return etree.fromstring(response.text.encode("utf8"))




