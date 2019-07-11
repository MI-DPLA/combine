from django.test import TestCase
from core.models import Transformation


class TransformationTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        xslt_payload = '''<?xml version="1.0" encoding="UTF-8"?>
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
        attributes = {
            'name': 'Test Transformation',
            'payload': xslt_payload,
            'transformation_type': 'xslt'
        }
        cls.transformation = Transformation(**attributes)
        cls.transformation.save()

    def test_str(self):
        self.assertEqual('Transformation: Test Transformation, transformation type: xslt',
                         format(TransformationTestCase.transformation))
