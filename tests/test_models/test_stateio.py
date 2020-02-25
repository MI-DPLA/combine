from django.test import TestCase
from django.core import serializers
from django.contrib.auth.models import User
from core.models import Transformation, StateIOClient, Job

XSLT_PAYLOAD = "<?xml version='1.0' encoding='UTF-8'?><xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='2.0'>    <xsl:output method='xml' indent='yes'/>    <xsl:template match='/'>        <xsl:call-template name='foo'/>    </xsl:template>    <xsl:template name='foo'>        <bar>            <xsl:value-of select='*/foo'/>        </bar>    </xsl:template></xsl:stylesheet>"
XSLT_PAYLOAD_2 = "<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='1.0'> <xsl:include href='/some/nonsense/file/path'/></xsl:stylesheet>"

class StateIOTestCase(TestCase):
    def setUp(self):
        self.io_client = StateIOClient()
        self.io_client.initialize_import_manifest('','')
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        dehydrated_objects_json = (
        '['
            '{'
                '"model": "core.transformation",'
                '"pk": 25,'
                '"fields": {'
                    '"name": "Test Import Transform",'
                    f'"payload": "{XSLT_PAYLOAD}",'
                    '"transformation_type": "xslt",'
                    '"filepath": "/some/nonsense/file/path",'
                    '"use_as_include": true'
                '}'
            '},'
            '{'
                '"model": "core.transformation",'
                '"pk": 24,'
                '"fields": {'
                    '"name": "Test Including Transform",'
                    f'"payload": "{XSLT_PAYLOAD_2}",'
                    '"transformation_type": "xslt",'
                    '"filepath": "/some/other/file/path",'
                    '"use_as_include": false'
                '}'
            '},'
            '{'
                '"model": "core.job",'
                '"pk": 29,'
                '"fields": {'
                    '"job_type": "TransformJob",'
                    '"name": "Test Transform Job",'
                    '"record_group_id": 20,'
                    '"user_id": 1,'
                    '"job_details": '
                        '{"transformation": '
                            '{"scenarios": [{'
                                '"id": 25,'
                                '"index": 0}],'
                                '"scenarios_json": [{"index":0,"trans_id":25}]}}'
                '}'
            '},'
            '{'
                '"model": "core.recordgroup",'
                '"pk": 20,'
                '"fields": {'
                    '"name": "Test Record Group",'
                    '"organization": "9"'
                '}'
            '},'
            '{'
                '"model": "core.organization",'
                '"pk": "9",'
                '"fields": {'
                    '"name": "Test Organization"'
                '}'
            '}'
        ']')
        self.io_client.deserialized_django_objects = []
        for obj in serializers.deserialize('json', dehydrated_objects_json):
            self.io_client.deserialized_django_objects.append(obj)


    def test_import_new_transformation(self):
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[0])
        new_transform = Transformation.objects.get(name="Test Import Transform")
        self.assertEqual(new_transform.id, self.io_client.import_manifest['pk_hash']['transformations'][25])
        self.assertEqual('/some/nonsense/file/path',
                         self.io_client.import_manifest['pk_hash']['transformation_file_paths'][25])

    def test_import_existing_transformation(self):
        existing_transform = Transformation(name="Test Import Transform",
                                            payload=XSLT_PAYLOAD)
        existing_transform.save()
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[0])
        self.assertEqual(existing_transform.id, self.io_client.import_manifest['pk_hash']['transformations'][25])
        self.assertEqual('/some/nonsense/file/path',
                         self.io_client.import_manifest['pk_hash']['transformation_file_paths'][25])

    def test_import_transformation_with_includes(self):
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[0])
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[1])
        self.io_client.fix_transformation_includes()
        new_transform = Transformation.objects.get(name="Test Including Transform")
        new_included_transform = Transformation.objects.get(name="Test Import Transform")
        self.assertIn(new_included_transform.filepath, new_transform.payload)

    def test_import_existing_transformation_with_includes(self):
        existing_1 = Transformation(name="Test Import Transform",
                                    payload=XSLT_PAYLOAD,
                                    filepath='blah')
        existing_1.save()
        existing_2 = Transformation(name="Test Including Transform",
                                    payload=XSLT_PAYLOAD_2)
        existing_2.save()
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[0])
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[1])
        self.io_client.fix_transformation_includes()
        existing_2 = Transformation.objects.get(name="Test Including Transform")
        existing_1 = Transformation.objects.get(name="Test Import Transform")
        self.assertIn(existing_1.filepath, existing_2.payload)

    def test_import_transform_job(self):
        self.io_client.export_manifest = {"jobs": [29],
                                          "record_groups": [20],
                                          "orgs": [9]}
        self.io_client._import_hierarchy()
        self.io_client.rehydrate_transformation(self.io_client.deserialized_django_objects[0])
        self.io_client._import_job_related_instances()
        job = Job.objects.get(name="Test Transform Job")
        transform = Transformation.objects.get(name="Test Import Transform")
        print(job.job_details)
        print(transform)
        self.assertEqual(job.job_details_dict['transformation']['scenarios'][0]["id"], transform.id)
        self.assertEqual(job.job_details_dict['transformation']['scenarios_json'],
                         f'[{{"index": 0, "trans_id": {transform.id}}}]')

