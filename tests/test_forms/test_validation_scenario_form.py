from django.test import TestCase

from core.forms import ValidationScenarioForm
from core.models import get_validation_scenario_choices


class ValidationScenarioTestCase(TestCase):
    def test_python_prohibited(self):
        test_body = {
            'name': 'Test Validation Scenario',
            'payload': 'test payload',
            'validation_type': 'python'
        }
        form = ValidationScenarioForm(test_body)
        self.assertFalse(form.is_valid())

    def test_python_permitted(self):
        with self.settings(ENABLE_PYTHON='true'):
            test_body = {
                'name': 'Test Validation Scenario',
                'payload': 'test payload',
                'validation_type': 'python'
            }
            form = ValidationScenarioForm(test_body)
            self.assertTrue(form.is_valid())

    def test_get_type_choices(self):
        choices = get_validation_scenario_choices()
        self.assertEqual(choices, [
            ('sch', 'Schematron'),
            ('es_query', 'ElasticSearch DSL Query'),
            ('xsd', 'XML Schema')
        ])

    def test_get_python_type_choices(self):
        with self.settings(ENABLE_PYTHON='true'):
            choices = get_validation_scenario_choices()
            self.assertEqual(choices, [
                ('sch', 'Schematron'),
                ('es_query', 'ElasticSearch DSL Query'),
                ('xsd', 'XML Schema'),
                ('python', 'Python Code Snippet')
            ])
