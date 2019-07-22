import subprocess
import time


from django.test import TestCase
from core.models import Transformation
from tests.utils import TestConfiguration

XSLT_PAYLOAD = '''<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
    <xsl:output method="xml" indent="yes"/>

    <xsl:template match="/">
        <xsl:call-template name="foo"/>
    </xsl:template>

    <xsl:template name="foo">
        <bar>
            <xsl:value-of select="*/foo"/>
        </bar>
    </xsl:template>
</xsl:stylesheet>'''

PYTHON_PAYLOAD = '''from lxml import etree
def python_record_transformation(record):
    foo_elem_query = record.xml.xpath('foo', namespaces=record.nsmap)
    foo_elem = foo_elem_query[0]
    foo_elem.attrib['type'] = 'bar'
    return [etree.tostring(record.xml), '', True]
'''


class TransformationTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.xslt_attributes = {
            'name': 'Test XSLT Transformation',
            'payload': XSLT_PAYLOAD,
            'transformation_type': 'xslt'
        }
        cls.xslt_transformation = Transformation(**cls.xslt_attributes)
        cls.xslt_transformation.save()
        python_attributes = {
            'name': 'Test Python Transformation',
            'payload': PYTHON_PAYLOAD,
            'transformation_type': 'python'
        }
        cls.python_transformation = Transformation(**python_attributes)
        cls.python_transformation.save()
        cls.config = TestConfiguration()

    def test_str(self):
        self.assertEqual('Transformation: Test XSLT Transformation, transformation type: xslt',
                         format(TransformationTestCase.xslt_transformation))

    def test_as_dict(self):
        as_dict = TransformationTestCase.xslt_transformation.as_dict()
        for k, v in TransformationTestCase.xslt_attributes.items():
            self.assertEqual(as_dict[k], v)

    def test_transform_record_xslt(self):
        command = "pyjxslt 6767"
        process = subprocess.Popen(command.split())
        try:
            time.sleep(0.5)
            record = TransformationTestCase.config.record
            transformed_record = TransformationTestCase.xslt_transformation.transform_record(record)
            self.assertEqual(transformed_record, '''<?xml version="1.0" encoding="UTF-8"?>
<bar>test document</bar>
''')
        finally:
            process.kill()

    def test_transform_record_python(self):
        record = TransformationTestCase.config.record
        transformed_record = TransformationTestCase.python_transformation.transform_record(record)
        self.assertEqual(transformed_record, '''<root xmlns:internet="http://internet.com">
<foo type="bar">test document</foo>
</root>''')
