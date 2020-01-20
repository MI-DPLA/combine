from django.test import TestCase
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from core.models import FieldMapper
from tests.utils import TestConfiguration, json_string


class FieldMapperTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_create_field_mapper_get(self):
        response = self.client.get(reverse('create_field_mapper'))
        self.assertIn(b'Create new Field Mapper', response.content)
        self.assertNotIn(b'Python Code Snippet', response.content)

    def test_create_python_field_mapper_get(self):
        with self.settings(ENABLE_PYTHON='true'):
            response = self.client.get(reverse('create_field_mapper'))
            self.assertIn(b'Create new Field Mapper', response.content)
            self.assertIn(b'Python Code Snippet', response.content)

    def test_create_field_mapper_post(self):
        post_body = {
            'name': 'Test Field Mapper',
            'field_mapper_type': 'xslt',
        }
        response = self.client.post(reverse('create_field_mapper'), post_body)
        self.assertRedirects(response, reverse('configuration'))
        field_mapper = FieldMapper.objects.get(name='Test Field Mapper')
        self.assertIsNotNone(field_mapper.id)
        field_mapper_dict = field_mapper.as_dict()
        for item in post_body:
            self.assertEqual(field_mapper_dict[item], post_body[item])

    def test_create_python_field_mapper_post(self):
        with self.settings(ENABLE_PYTHON='true'):
            post_body = {
                'name': 'Test Python Field Mapper',
                'field_mapper_type': 'python',
            }
            response = self.client.post(reverse('create_field_mapper'), post_body)
            self.assertRedirects(response, reverse('configuration'))
            field_mapper = FieldMapper.objects.get(name='Test Python Field Mapper')
            self.assertIsNotNone(field_mapper.id)
            field_mapper_dict = field_mapper.as_dict()
            for item in post_body:
                self.assertEqual(field_mapper_dict[item], post_body[item])

    def test_create_prohibited_python_field_mapper_post(self):
        post_body = {
            'name': 'Test Python Field Mapper',
            'field_mapper_type': 'python',
        }
        response = self.client.post(reverse('create_field_mapper'), post_body)
        self.assertIn(b'Select a valid choice', response.content)

    def test_create_field_mapper_invalid(self):
        response = self.client.post(reverse('create_field_mapper'), {})
        self.assertIn(b'This field is required.', response.content)

    def test_edit_field_mapper_get(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='python',
                                                  payload='some code')
        response = self.client.get(reverse('edit_field_mapper', args=[field_mapper.id]))
        self.assertIn(b'Test Field Mapper', response.content)

    def test_edit_field_mapper_post(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='python',
                                                  payload='some code')
        fm_id = field_mapper.id
        post_body = {
            'name': 'Test Field Mapper',
            'field_mapper_type': 'python',
            'payload': 'some other code',
        }
        response = self.client.post(reverse('edit_field_mapper', args=[field_mapper.id]), post_body)
        self.assertRedirects(response, reverse('configuration'))
        field_mapper = FieldMapper.objects.get(pk=int(fm_id))
        field_mapper_dict = field_mapper.as_dict()
        for item in post_body:
            self.assertEqual(field_mapper_dict[item], post_body[item])

    def test_edit_field_mapper_invalid(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='python',
                                                  payload='some code')
        post_body = {
            'payload': 'some other code',
        }
        response = self.client.post(reverse('edit_field_mapper', args=[field_mapper.id]), post_body)
        self.assertIn(b'This field is required.', response.content)

    def test_delete_field_mapper(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='python',
                                                  payload='some code')
        response = self.client.delete(reverse('delete_field_mapper', args=[field_mapper.id]))
        self.assertRedirects(response, reverse('configuration'))
        with self.assertRaises(ObjectDoesNotExist):
            FieldMapper.objects.get(pk=int(field_mapper.id))

    def test_delete_field_mapper_nonexistent(self):
        response = self.client.delete(reverse('delete_field_mapper', args=[12345]))
        self.assertRedirects(response, reverse('configuration'))

    def test_field_mapper_payload_config_json(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='xml2kvp',
                                                  config_json='{}')
        response = self.client.get(reverse('field_mapper_payload', args=[field_mapper.id]))
        self.assertEqual(b'{}', response.content)

    def test_field_mapper_payload(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='xml2kvp',
                                                  payload='{"payload": "test payload"}')
        response = self.client.get(f'{reverse("field_mapper_payload", args=[field_mapper.id])}?type=payload')
        self.assertEqual(b'{"payload": "test payload"}', response.content)

    def test_field_mapper_payload_config_config(self):
        field_mapper = FieldMapper.objects.create(name='Test Field Mapper',
                                                  field_mapper_type='xml2kvp',
                                                  config_json='{}')
        response = self.client.get(f'{reverse("field_mapper_payload", args=[field_mapper.id])}?type=config')
        self.assertEqual(b'{}', response.content)

    def test_field_mapper_update_new(self):
        response = self.client.post(reverse('field_mapper_update'), {
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
        response = self.client.post(reverse('field_mapper_update'), {
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
        response = self.client.post(reverse('field_mapper_update'), {
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
        response = self.client.get(f'{reverse("test_field_mapper")}?fmid={field_mapper.id}')
        self.assertIn(b'Test Field Mapper', response.content)

    def test_post_test_field_mapper(self):
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json=json_string({"add_literals": {"foo": "bar"}}),
                                                  name='test field mapper')
        response = self.client.post(reverse('test_field_mapper'), {
            'db_id': self.config.record.id,
            'fm_config_json': field_mapper.config_json
        })
        self.assertEqual(response.json(), {'root_foo': 'test document', 'foo': 'bar'})
