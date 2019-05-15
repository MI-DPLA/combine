from django.test import Client, TestCase

from core.models import FieldMapper
from tests.test_views.utils import TestConfiguration


class FieldMapperTestCase(TestCase):
    def setUp(self):
        self.c = Client()
        self.config = TestConfiguration()

    def test_field_mapper_payload_config_json(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json='{}')
        response = self.c.get(
            f'/combine/configuration/field_mapper/{field_mapper.id}/payload')
        self.assertEqual(b'"{}"', response.content)


    def test_field_mapper_payload(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  payload='test payload')
        response = self.c.get(
            f'/combine/configuration/field_mapper/{field_mapper.id}/payload?type=payload')
        self.assertEqual(b'"test payload"', response.content)

    def test_field_mapper_payload_config_config(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json='{}')
        response = self.c.get(
            f'/combine/configuration/field_mapper/{field_mapper.id}/payload?type=config')
        self.assertEqual(b'"{}"', response.content)

    def test_field_mapper_update_new(self):
        response = self.c.post('/combine/configuration/field_mapper/update',
                               {
                                   'update_type': 'new',
                                   'fm_name': 'test field mapper',
                                   'fm_config_json': '{}'
                               })
        json = response.json()
        self.assertEqual(json['results'], True)
        self.assertEqual(json['msg'],
                         'New Field Mapper configurations were <strong>saved</strong> as: <strong>test field mapper</strong>')

    def test_field_mapper_update_update(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json='{}',
                                                  name='test field mapper')
        response = self.c.post('/combine/configuration/field_mapper/update',
                               {
                                   'update_type': 'update',
                                   'fm_id': field_mapper.id,
                                   'fm_config_json': '{"new_key": "new value"}'
                               })
        json = response.json()
        self.assertEqual(json['results'], True)
        self.assertEqual(json['msg'],
                         'Field Mapper configurations for <strong>test field mapper</strong> were <strong>updated</strong>')

    def test_field_mapper_update_delete(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json='{}',
                                                  name='test field mapper')
        response = self.c.post('/combine/configuration/field_mapper/update',
                               {
                                   'update_type': 'delete',
                                   'fm_id': field_mapper.id
                               })
        json = response.json()
        self.assertEqual(json['results'], True)
        self.assertEqual(json['msg'],
                         'Field Mapper configurations for <strong>test field mapper</strong> were <strong>deleted</strong>')

    def test_get_test_field_mapper(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json='{}',
                                                  name='test field mapper')
        response = self.c.get(f'/combine/configuration/test_field_mapper?fmid={field_mapper.id}')
        self.assertIn(b'Test Field Mapper', response.content)

    def test_post_test_field_mapper(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json='{}',
                                                  name='test field mapper')
        response = self.c.post('/combine/configuration/test_field_mapper',
                               {
                                   'db_id': self.config.record.id,
                                   'fm_config_json': field_mapper.config_json
                               })
        print(response.json())
        # TODO: put in some valid config_json
