from django.test import TestCase
from django.core import serializers
from core.models import Transformation, StateIOClient

XSLT_PAYLOAD = "<?xml version='1.0' encoding='UTF-8'?><xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='2.0'>    <xsl:output method='xml' indent='yes'/>    <xsl:template match='/'>        <xsl:call-template name='foo'/>    </xsl:template>    <xsl:template name='foo'>        <bar>            <xsl:value-of select='*/foo'/>        </bar>    </xsl:template></xsl:stylesheet>"

class StateIOTestCase(TestCase):
    def setUp(self):
        self.io_client = StateIOClient()
        self.io_client.initialize_import_manifest('','')
        transformation_json = ('[{'
            '"model": "core.transformation",'
            '"pk": 25,'
            '"fields": {'
                '"name": "Test Import Transform",'
                f'"payload": "{XSLT_PAYLOAD}",'
                '"transformation_type": "xslt",'
                '"filepath": "/some/nonsense/file/path",'
                '"use_as_include": false'
            '}'
        '}]')
        self.io_client.deserialized_django_objects = []
        for obj in serializers.deserialize('json', transformation_json):
            self.io_client.deserialized_django_objects.append(obj)


    def test_import_new_transformation(self):
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[0])
        new_transform = Transformation.objects.get(name="Test Import Transform")
        self.assertEqual(new_transform.id, self.io_client.import_manifest['pk_hash']['transformations'][25])
        self.assertEqual(new_transform.filepath,
                         self.io_client.import_manifest['pk_hash']['transformation_file_paths']['/some/nonsense/file/path'])

    def test_import_existing_transformation(self):
        existing_transform = Transformation(name="Test Import Transform",
                                            payload=XSLT_PAYLOAD,
                                            use_as_include=False)
        existing_transform.save()
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[0])
        self.assertEqual(existing_transform.id, self.io_client.import_manifest['pk_hash']['transformations'][25])
        self.assertEqual(existing_transform.filepath,
                         self.io_client.import_manifest['pk_hash']['transformation_file_paths']['/some/nonsense/file/path'])


