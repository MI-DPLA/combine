from django.test import Client, TestCase
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from core.models import FieldMapper
from tests.test_views.utils import TestConfiguration


class FieldMapperTestCase(TestCase):
    def setUp(self):
        self.c = Client()
        self.config = TestConfiguration()
        self.c.force_login(self.config.user)

    def test_create_field_mapper_get(self):
        response = self.c.get(reverse('create_field_mapper'))
        self.assertIn(b'Create new Field Mapper', response.content)

    def test_create_field_mapper_post(self):
        post_body = {
            'name': 'Test Field Mapper',
            'field_mapper_type': 'python',
        }
        response = self.c.post(reverse('create_field_mapper'), post_body)
        self.assertRedirects(response, reverse('configuration'))
        field_mapper = FieldMapper.objects.get(name='Test Field Mapper')
        self.assertIsNotNone(field_mapper.id)
        field_mapper_dict = field_mapper.as_dict()
        for item in post_body:
            self.assertEqual(field_mapper_dict[item], post_body[item])

    def test_edit_field_mapper_get(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='python',
                                                  payload='some code')
        response = self.c.get(reverse('edit_field_mapper', args=[field_mapper.id]))
        self.assertIn(b'Test Field Mapper', response.content)

    def test_edit_field_mapper_post(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='python',
                                                  payload='some code')
        id = field_mapper.id
        post_body = {
            'name': 'Test Field Mapper',
            'field_mapper_type': 'python',
            'payload': 'some other code',
        }
        response = self.c.post(reverse('edit_field_mapper', args=[field_mapper.id]), post_body)
        self.assertRedirects(response, reverse('configuration'))
        field_mapper = FieldMapper.objects.get(pk=int(id))
        field_mapper_dict = field_mapper.as_dict()
        for item in post_body:
            self.assertEqual(field_mapper_dict[item], post_body[item])

    def test_delete_field_mapper(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='python',
                                                  payload='some code')
        response = self.c.delete(reverse('delete_field_mapper', args=[field_mapper.id]))
        self.assertRedirects(response, reverse('configuration'))
        with self.assertRaises(ObjectDoesNotExist):
            FieldMapper.objects.get(pk=int(field_mapper.id))

    def test_delete_field_mapper_nonexistent(self):
        response = self.c.delete(reverse('delete_field_mapper', args=[12345]))
        self.assertRedirects(response, reverse('configuration'))

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
