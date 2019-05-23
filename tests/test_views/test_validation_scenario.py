from django.test import Client, TestCase
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from core.models import ValidationScenario
from tests.test_views.utils import TestConfiguration


class ValidationScenarioTestCase(TestCase):
    simple_validation_payload = """def test_record_has_words(record, test_message='record has words'):
    return True"""

    def setUp(self):
        self.config = TestConfiguration()
        self.c = Client()
        self.c.force_login(self.config.user)

    def test_create_validation_scenario_get(self):
        response = self.c.get(reverse('create_validation_scenario'))
        self.assertIn(b'Create new Validation Scenario', response.content)

    def test_create_validation_scenario_post(self):
        post_body = {
            'name': 'Test Validate',
            'payload': 'Some python code',
            'validation_type': 'python'}
        response = self.c.post(reverse('create_validation_scenario'), post_body)
        self.assertRedirects(response, '/combine/configuration')
        scenario = ValidationScenario.objects.get(name='Test Validate')
        self.assertIsNotNone(scenario.id)
        scenario_dict = scenario.as_dict()
        for item in post_body:
            self.assertEqual(scenario_dict[item], post_body[item])

    def test_validation_scenario_get(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.c.get(reverse('validation_scenario', args=[scenario.id]))
        self.assertIn(b'Test Validate', response.content)

    def test_validation_scenario_post(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.c.post(reverse('validation_scenario', args=[scenario.id]),
                               {
                                   'payload': ValidationScenarioTestCase.simple_validation_payload,
                                   'name': scenario.name,
                                   'validation_type': scenario.validation_type
                               })
        self.assertRedirects(response, '/combine/configuration')
        scenario = ValidationScenario.objects.get(name='Test Validate')
        self.assertIsNotNone(scenario.id)
        self.assertEqual(scenario.name, 'Test Validate')
        self.assertEqual(scenario.payload, ValidationScenarioTestCase.simple_validation_payload)

    def test_validation_scenario_delete(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.c.delete(reverse('delete_validation_scenario', args=[scenario.id]))
        self.assertRedirects(response, '/combine/configuration')
        try:
            ValidationScenario.objects.get(pk=int(scenario.id))
            self.fail('Did not delete validation scenario')
        except ObjectDoesNotExist:
            pass

    def test_validation_scenario_delete_nonexistent(self):
        response = self.c.delete(reverse('delete_validation_scenario', args=[12345]))
        self.assertRedirects(response, '/combine/configuration')

    def test_validation_scenario_payload(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.c.get(reverse('validation_scenario_payload', args=[scenario.id]))
        self.assertEqual(b'Some python code', response.content)

    def test_validation_scenario_payload_xml(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some schematron',
                                                     validation_type='sch')
        response = self.c.get(reverse('validation_scenario_payload', args=[scenario.id]))
        self.assertEqual(b'Some schematron', response.content)

    def test_validation_scenario_test(self):
        response = self.c.get(reverse('test_validation_scenario'))
        self.assertIn(b'Test Validation Scenario', response.content)

    def test_validation_scenario_test_post_raw(self):
        response = self.validation_scenario_test('raw')
        self.assertEqual(response.__getitem__('content-type'), 'text/plain')
        self.assertEqual(b'{"fail_count": 0, "passed": ["record has words"], "failed": [], "total_tests": 1}',
                         response.content)

    def test_validation_scenario_test_post_parsed(self):
        response = self.validation_scenario_test('parsed')
        self.assertEqual(response.__getitem__('content-type'), 'application/json')
        self.assertEqual(b'{"fail_count": 0, "passed": ["record has words"], "failed": [], "total_tests": 1}',
                         response.content)

    def test_validation_scenario_test_post_unrecognized(self):
        response = self.validation_scenario_test('other')
        self.assertEqual(b'validation results format not recognized', response.content)

    def validation_scenario_test(self, results_format):
        return self.c.post(reverse('test_validation_scenario'),
                           {
                               'vs_payload': ValidationScenarioTestCase.simple_validation_payload,
                               'vs_type': 'python',
                               'db_id': self.config.record.id,
                               'vs_results_format': results_format
                           })
