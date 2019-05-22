from django.test import Client, TestCase
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from core.models import RecordIdentifierTransformationScenario
from tests.test_views.utils import TestConfiguration


class RecordIdentifierTransformationScenarioTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.c = Client()
        self.c.force_login(self.config.user)

    def test_create_rits_get(self):
        response = self.c.get(reverse('create_rits'))
        self.assertIn(b'Create new Record Identifier Transformation Scenario', response.content)

    def test_create_rits_post(self):
        response = self.c.post(reverse('create_rits'), {
            'name': 'Test RITS',
            'transformation_type': 'python',
            'transformation_target': 'document',
            'python_payload': 'test payload'
        })
        self.assertRedirects(response, reverse('configuration'))
        rits = RecordIdentifierTransformationScenario.objects.get(name='Test RITS')
        self.assertIsNotNone(rits.id)
        self.assertEqual(rits.transformation_type, 'python')
        self.assertEqual(rits.transformation_target, 'document')
        self.assertEqual(rits.python_payload, 'test payload')

    def test_rits_get(self):
        rits = RecordIdentifierTransformationScenario.objects.create(
            name='Test RITS',
            transformation_type='python',
            transformation_target='document',
            python_payload='test payload'
        )
        response = self.c.get(reverse('edit_rits', args=[rits.id]))
        self.assertIn(b'Test RITS', response.content)

    def test_rits_post(self):
        rits = RecordIdentifierTransformationScenario.objects.create(
            name='Test RITS',
            transformation_type='python',
            transformation_target='document',
            python_payload='test payload'
        )
        response = self.c.post(reverse('edit_rits', args=[rits.id]), {
            'python_payload': 'some other payload',
            'transformation_type': rits.transformation_type,
            'transformation_target': rits.transformation_target,
            'name': rits.name
        })
        self.assertRedirects(response, reverse('configuration'))
        updated_rits = RecordIdentifierTransformationScenario.objects.get(name='Test RITS')
        self.assertEqual(updated_rits.python_payload, 'some other payload')
        self.assertEqual(updated_rits.id, rits.id)

    def test_rits_delete(self):
        rits = RecordIdentifierTransformationScenario.objects.create(
            name='Test RITS',
            transformation_type='python',
            transformation_target='document',
            python_payload='test payload'
        )
        response = self.c.delete(reverse('delete_rits', args=[rits.id]))
        self.assertRedirects(response, reverse('configuration'))
        try:
            RecordIdentifierTransformationScenario.objects.get(pk=int(rits.id))
            self.fail('Did not delete rits')
        except ObjectDoesNotExist:
            pass

    def test_rits_delete_nonexistent(self):
        response = self.c.delete(reverse('delete_rits', args=[12345]))
        self.assertRedirects(response, reverse('configuration'))

    def test_rits_payload(self):
        rits = RecordIdentifierTransformationScenario.objects.create(regex_match_payload='test match',
                                                                     regex_replace_payload='test replace',
                                                                     transformation_type='regex',
                                                                     transformation_target='document')
        response = self.c.get(reverse('rits_payload', args=[rits.id]))
        json = response.json()
        self.assertEqual(json['transformation_type'], 'regex')
        self.assertEqual(json['transformation_target'], 'document')
        self.assertEqual(json['regex_match_payload'], 'test match')
        self.assertEqual(json['regex_replace_payload'], 'test replace')