from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase
from django.urls import reverse

from core.models import RecordIdentifierTransformation
from tests.utils import TestConfiguration


class RecordIdentifierTransformationTestCase(TestCase):
    simple_python_payload = '''
def transform_identifier(ident):
    return 'transformed'
    '''
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_create_rits_get(self):
        response = self.client.get(reverse('create_rits'))
        self.assertIn(b'Create new Record Identifier Transformation Scenario', response.content)
        self.assertNotIn(b'Python Code Snippet', response.content)

    def test_create_permitted_python_rits_get(self):
        with self.settings(ENABLE_PYTHON='true'):
            response = self.client.get(reverse('create_rits'))
            self.assertIn(b'Create new Record Identifier Transformation Scenario', response.content)
            self.assertIn(b'Python Code Snippet', response.content)

    def test_create_rits_post(self):
        post_body = {
            'name': 'Test RITS',
            'transformation_type': 'xpath',
            'transformation_target': 'document',
            'xpath_payload': 'test payload'
        }
        response = self.client.post(reverse('create_rits'), post_body)
        self.assertRedirects(response, reverse('configuration'))
        rits = RecordIdentifierTransformation.objects.get(name='Test RITS')
        self.assertIsNotNone(rits.id)
        rits_dict = rits.as_dict()
        for item in post_body:
            self.assertEqual(rits_dict[item], post_body[item])

    def test_create_python_rits_post(self):
        post_body = {
            'name': 'Test RITS',
            'transformation_type': 'python',
            'transformation_target': 'document',
            'python_payload': 'test payload'
        }
        response = self.client.post(reverse('create_rits'), post_body)
        self.assertIn(b'Select a valid choice', response.content)

    def test_create_permitted_python_rits_post(self):
        with self.settings(ENABLE_PYTHON='true'):
            post_body = {
                'name': 'Test RITS',
                'transformation_type': 'python',
                'transformation_target': 'document',
                'python_payload': 'test payload'
            }
            response = self.client.post(reverse('create_rits'), post_body)
            self.assertRedirects(response, reverse('configuration'))
            rits = RecordIdentifierTransformation.objects.get(name='Test RITS')
            self.assertIsNotNone(rits.id)
            rits_dict = rits.as_dict()
            for item in post_body:
                self.assertEqual(rits_dict[item], post_body[item])

    def test_create_rits_invalid(self):
        response = self.client.post(reverse('create_rits'), {})
        self.assertIn(b'This field is required.', response.content)

    def test_edit_rits_get(self):
        rits = RecordIdentifierTransformation.objects.create(
            name='Test RITS',
            transformation_type='xpath',
            transformation_target='document',
            xpath_payload='test payload'
        )
        response = self.client.get(reverse('edit_rits', args=[rits.id]))
        self.assertIn(b'Test RITS', response.content)

    def test_edit_python_rits_get(self):
        rits = RecordIdentifierTransformation.objects.create(
            name='Test RITS',
            transformation_type='python',
            transformation_target='document',
            python_payload='test payload'
        )
        response = self.client.get(reverse('edit_rits', args=[rits.id]))
        self.assertIn(b'Select a valid choice. python is not one of the available choices', response.content)

    def test_edit_permitted_python_rits_get(self):
        with self.settings(ENABLE_PYTHON='true'):
            rits = RecordIdentifierTransformation.objects.create(
                name='Test RITS',
                transformation_type='python',
                transformation_target='document',
                python_payload='test payload'
            )
            response = self.client.get(reverse('edit_rits', args=[rits.id]))
            self.assertNotIn(b'Select a valid choice. python is not one of the available choices', response.content)

    def test_edit_rits_post(self):
        rits = RecordIdentifierTransformation.objects.create(
            name='Test RITS',
            transformation_type='xpath',
            transformation_target='document',
            xpath_payload='test payload'
        )
        response = self.client.post(reverse('edit_rits', args=[rits.id]), {
            'xpath_payload': 'some other payload',
            'transformation_type': rits.transformation_type,
            'transformation_target': rits.transformation_target,
            'name': rits.name
        })
        self.assertRedirects(response, reverse('configuration'))
        updated_rits = RecordIdentifierTransformation.objects.get(name='Test RITS')
        self.assertEqual(updated_rits.xpath_payload, 'some other payload')
        self.assertEqual(updated_rits.id, rits.id)

    def test_edit_python_rits_post(self):
        rits = RecordIdentifierTransformation.objects.create(
            name='Test RITS',
            transformation_type='python',
            transformation_target='document',
            python_payload='test payload'
        )
        response = self.client.post(reverse('edit_rits', args=[rits.id]), {
            'python_payload': 'some other payload',
            'transformation_type': rits.transformation_type,
            'transformation_target': rits.transformation_target,
            'name': rits.name
        })
        self.assertIn(b'Select a valid choice. python is not one of the available choices', response.content)

    def test_edit_permitted_python_rits_post(self):
        with self.settings(ENABLE_PYTHON='true'):
            rits = RecordIdentifierTransformation.objects.create(
                name='Test RITS',
                transformation_type='python',
                transformation_target='document',
                python_payload='test payload'
            )
            response = self.client.post(reverse('edit_rits', args=[rits.id]), {
                'python_payload': 'some other payload',
                'transformation_type': rits.transformation_type,
                'transformation_target': rits.transformation_target,
                'name': rits.name
            })
            self.assertRedirects(response, reverse('configuration'))
            updated_rits = RecordIdentifierTransformation.objects.get(name='Test RITS')
            self.assertEqual(updated_rits.python_payload, 'some other payload')
            self.assertEqual(updated_rits.id, rits.id)

    def test_edit_rits_post_invalid(self):
        rits = RecordIdentifierTransformation.objects.create(
            name='Test RITS',
            transformation_type='python',
            transformation_target='document',
            python_payload='test payload'
        )
        response = self.client.post(reverse('edit_rits', args=[rits.id]), {
            'python_payload': 'some other payload',
        })
        self.assertIn(b'This field is required.', response.content)

    def test_rits_delete(self):
        rits = RecordIdentifierTransformation.objects.create(
            name='Test RITS',
            transformation_type='python',
            transformation_target='document',
            python_payload='test payload'
        )
        response = self.client.delete(reverse('delete_rits', args=[rits.id]))
        self.assertRedirects(response, reverse('configuration'))
        with self.assertRaises(ObjectDoesNotExist):
            RecordIdentifierTransformation.objects.get(pk=int(rits.id))

    def test_rits_delete_nonexistent(self):
        response = self.client.delete(reverse('delete_rits', args=[12345]))
        self.assertRedirects(response, reverse('configuration'))

    def test_rits_payload(self):
        rits = RecordIdentifierTransformation.objects.create(regex_match_payload='test match',
                                                             regex_replace_payload='test replace',
                                                             transformation_type='regex',
                                                             transformation_target='document')
        response = self.client.get(reverse('rits_payload', args=[rits.id]))
        json = response.json()
        self.assertEqual(json['transformation_type'], 'regex')
        self.assertEqual(json['transformation_target'], 'document')
        self.assertEqual(json['regex_match_payload'], 'test match')
        self.assertEqual(json['regex_replace_payload'], 'test replace')

    def test_get_test_rits(self):
        response = self.client.get(reverse('test_rits'))
        self.assertIn(b'select a pre-existing Record Identifier Transformation Scenario', response.content)
        self.assertNotIn(b'Python Code Snippet', response.content)

    def test_get_test_rits_python_permitted(self):
        with self.settings(ENABLE_PYTHON='true'):
            response = self.client.get(reverse('test_rits'))
            self.assertIn(b'select a pre-existing Record Identifier Transformation Scenario', response.content)
            self.assertIn(b'Python Code Snippet', response.content)

    def test_post_test_rits(self):
        post_body = {
            'regex_match_payload': 'test document',
            'regex_replace_payload': 'test replace',
            'record_id_transform_type': 'regex',
            'record_id_transform_target': 'document',
            'db_id': self.config.record.id
        }
        response = self.client.post(reverse('test_rits'), post_body)
        self.assertEqual(response.json(), {
                             'results': '<?xml version="1.0" encoding="UTF-8"?>\n<root xmlns:internet="http://internet.com">\n<foo>test replace</foo>\n</root>\n',
                             'success': True
                         })

    def test_post_test_rits_python(self):
        post_body = {
            'record_id_transform_type': 'python',
            'python_payload': '',
            'record_id_transform_target': 'record_id',
            'db_id': self.config.record.id
        }
        response = self.client.post(reverse('test_rits'), post_body)
        self.assertEqual(response.json(), {'results': 'requested invalid type for RITS: python', 'success': False})

    def test_post_test_rits_python_permitted(self):
        with self.settings(ENABLE_PYTHON='true'):
            post_body = {
                'record_id_transform_type': 'python',
                'python_payload': self.simple_python_payload,
                'record_id_transform_target': 'record_id',
                'db_id': self.config.record.id
            }
            response = self.client.post(reverse('test_rits'), post_body)
            self.assertEqual(response.json(), {'results': 'transformed', 'success': True})
