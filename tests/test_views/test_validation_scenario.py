from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase
from django.urls import reverse

from core.models import ValidationScenario
from tests.utils import TestConfiguration


class ValidationScenarioTestCase(TestCase):
    python_validation_payload = """def test_record_has_words(record, test_message='record has words'):
    return True"""

    SCHEMATRON_PAYLOAD = '''<?xml version="1.0" encoding="UTF-8"?>
    <schema xmlns="http://purl.oclc.org/dsdl/schematron" xmlns:internet="http://internet.com">
    	<ns prefix="internet" uri="http://internet.com"/>
    	<!-- Required top level Elements for all records record -->
    	<pattern>
    		<title>Required Elements for Each MODS record</title>
    		<rule context="root">
    			<assert test="foo">There must be a foo element</assert>
    		</rule>
    	</pattern>
    </schema>'''

    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_create_validation_scenario_get(self):
        response = self.client.get(reverse('create_validation_scenario'))
        self.assertIn(b'Create new Validation Scenario', response.content)
        self.assertNotIn(b'Python Code Snippet', response.content)

    def test_create_permitted_python_field_mapper_get(self):
        with self.settings(ENABLE_PYTHON='true'):
            response = self.client.get(reverse('create_validation_scenario'))
            self.assertIn(b'Create new Validation Scenario', response.content)
            self.assertIn(b'Python Code Snippet', response.content)

    def test_create_validation_scenario_post(self):
        post_body = {
            'name': 'Test Validate',
            'payload': 'Some elasticsearch query',
            'validation_type': 'es_query'}
        response = self.client.post(reverse('create_validation_scenario'), post_body)
        self.assertRedirects(response, reverse('configuration'))
        scenario = ValidationScenario.objects.get(name='Test Validate')
        self.assertIsNotNone(scenario.id)
        scenario_dict = scenario.as_dict()
        for item in post_body:
            self.assertEqual(scenario_dict[item], post_body[item])

    def test_create_python_validation_scenario_post(self):
        post_body = {
            'name': 'Test Validate',
            'payload': 'Some python code',
            'validation_type': 'python'}
        response = self.client.post(reverse('create_validation_scenario'), post_body)
        self.assertIn(b'Select a valid choice', response.content)

    def test_create_permitted_python_validation_scenario_post(self):
        with self.settings(ENABLE_PYTHON='true'):
            post_body = {
                'name': 'Test Validate',
                'payload': 'Some python code',
                'validation_type': 'python'}
            response = self.client.post(reverse('create_validation_scenario'), post_body)
            self.assertRedirects(response, reverse('configuration'))
            scenario = ValidationScenario.objects.get(name='Test Validate')
            self.assertIsNotNone(scenario.id)
            scenario_dict = scenario.as_dict()
            for item in post_body:
                self.assertEqual(scenario_dict[item], post_body[item])

    def test_create_validation_scenario_invalid(self):
        response = self.client.post(reverse('create_validation_scenario'), {})
        self.assertIn(b'This field is required.', response.content)

    def test_edit_validation_scenario_get(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some elasticsearch query',
                                                     validation_type='es_query')
        response = self.client.get(reverse('validation_scenario', args=[scenario.id]))
        self.assertIn(b'Test Validate', response.content)

    def test_edit_python_validation_scenario_get(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.client.get(reverse('validation_scenario', args=[scenario.id]))
        self.assertIn(b'Select a valid choice. python is not one of the available choices', response.content)

    def test_edit_permitted_python_validation_scenario_get(self):
        with self.settings(ENABLE_PYTHON='true'):
            scenario = ValidationScenario.objects.create(name='Test Validate',
                                                         payload='Some python code',
                                                         validation_type='python')
            response = self.client.get(reverse('validation_scenario', args=[scenario.id]))
            self.assertNotIn(b'Select a valid choice. python is not one of the available choices', response.content)

    def test_edit_validation_scenario_post(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some schematron thing',
                                                     validation_type='sch')
        response = self.client.post(reverse('validation_scenario', args=[scenario.id]), {
            'payload': ValidationScenarioTestCase.SCHEMATRON_PAYLOAD,
            'name': scenario.name,
            'validation_type': scenario.validation_type
        })
        self.assertRedirects(response, reverse('configuration'))
        scenario = ValidationScenario.objects.get(name='Test Validate')
        self.assertIsNotNone(scenario.id)
        self.assertEqual(scenario.name, 'Test Validate')
        self.assertEqual(scenario.payload, ValidationScenarioTestCase.SCHEMATRON_PAYLOAD)

    def test_edit_python_validation_scenario_post(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.client.post(reverse('validation_scenario', args=[scenario.id]), {
            'payload': ValidationScenarioTestCase.python_validation_payload,
            'name': scenario.name,
            'validation_type': scenario.validation_type
        })
        self.assertIn(b'Select a valid choice. python is not one of the available choices', response.content)

    def test_edit_permitted_python_validation_scenario_post(self):
        with self.settings(ENABLE_PYTHON='true'):
            scenario = ValidationScenario.objects.create(name='Test Validate',
                                                         payload='Some python code',
                                                         validation_type='python')
            response = self.client.post(reverse('validation_scenario', args=[scenario.id]), {
                'payload': ValidationScenarioTestCase.python_validation_payload,
                'name': scenario.name,
                'validation_type': scenario.validation_type
            })
            self.assertRedirects(response, reverse('configuration'))
            scenario = ValidationScenario.objects.get(name='Test Validate')
            self.assertIsNotNone(scenario.id)
            self.assertEqual(scenario.name, 'Test Validate')
            self.assertEqual(scenario.payload, ValidationScenarioTestCase.python_validation_payload)

    def test_edit_validation_scenario_invalid(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.client.post(reverse('validation_scenario', args=[scenario.id]), {
            'payload': ValidationScenarioTestCase.python_validation_payload,
        })
        self.assertIn(b'This field is required.', response.content)

    def test_validation_scenario_delete(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.client.delete(reverse('delete_validation_scenario', args=[scenario.id]))
        self.assertRedirects(response, reverse('configuration'))
        with self.assertRaises(ObjectDoesNotExist):
            ValidationScenario.objects.get(pk=int(scenario.id))

    def test_validation_scenario_delete_nonexistent(self):
        response = self.client.delete(reverse('delete_validation_scenario', args=[12345]))
        self.assertRedirects(response, reverse('configuration'))

    def test_validation_scenario_payload(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        response = self.client.get(reverse('validation_scenario_payload', args=[scenario.id]))
        self.assertEqual(b'Some python code', response.content)

    def test_validation_scenario_payload_xml(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some schematron',
                                                     validation_type='sch')
        response = self.client.get(reverse('validation_scenario_payload', args=[scenario.id]))
        self.assertEqual(b'Some schematron', response.content)

    def test_validation_scenario_test(self):
        response = self.client.get(reverse('test_validation_scenario'))
        self.assertIn(b'Test Validation Scenario', response.content)
        self.assertNotIn(b'Python Code Snippet', response.content)

    def test_get_test_validation_scenario_python_permitted(self):
        with self.settings(ENABLE_PYTHON='true'):
            response = self.client.get(reverse('test_validation_scenario'))
            self.assertIn(b'Test Validation Scenario', response.content)
            self.assertIn(b'Python Code Snippet', response.content)

    def test_validation_scenario_test_post_raw(self):
        response = self.validation_scenario_test('raw')
        self.assertEqual(response.__getitem__('content-type'), 'text/plain')
        self.assertEqual(b'<svrl:schematron-output xmlns:svrl="http://purl.oclc.org/dsdl/svrl" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:schold="http://www.ascc.net/xml/schematron" xmlns:sch="http://www.ascc.net/xml/schematron" xmlns:iso="http://purl.oclc.org/dsdl/schematron" xmlns:internet="http://internet.com" title="" schemaVersion=""><!--  &#160;\n\t\t  &#160;\n\t\t  &#160;\n\t\t --><svrl:ns-prefix-in-attribute-values uri="http://internet.com" prefix="internet"/><svrl:active-pattern name="Required Elements for Each MODS record"/><svrl:fired-rule context="root"/></svrl:schematron-output>',
                         response.content)

    def test_validation_scenario_test_post_parsed(self):
        response = self.validation_scenario_test('parsed')
        self.assertEqual(response.__getitem__('content-type'), 'application/json')
        self.assertEqual(b'{"fail_count": 0, "passed": ["There must be a foo element"], "failed": [], "total_tests": 1}',
                         response.content)

    def test_validation_scenario_test_post_unrecognized(self):
        response = self.validation_scenario_test('other')
        self.assertEqual(b'validation results format not recognized', response.content)

    def test_post_test_validation_scenario_python(self):
        post_body = {
            'vs_payload': ValidationScenarioTestCase.python_validation_payload,
            'vs_type': 'python',
            'db_id': self.config.record.id,
            'vs_results_format': 'parsed'
        }
        response = self.client.post(reverse('test_validation_scenario'), post_body)
        print(response.content)
        self.assertEqual(b'requested invalid type for validation scenario: python', response.content)

    def validation_scenario_test(self, results_format):
        return self.client.post(reverse('test_validation_scenario'), {
            'vs_payload': ValidationScenarioTestCase.SCHEMATRON_PAYLOAD,
            'vs_type': 'sch',
            'db_id': self.config.record.id,
            'vs_results_format': results_format
        })

