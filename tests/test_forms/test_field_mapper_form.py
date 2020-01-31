from django.test import TestCase

from core.forms import FieldMapperForm
from core.models import get_field_mapper_choices


class FieldMapperFormTestCase(TestCase):
    def test_python_prohibited(self):
        test_body = {
            'name': 'Test Field Mapper',
            'field_mapper_type': 'python',
        }
        form = FieldMapperForm(test_body)
        self.assertFalse(form.is_valid())

    def test_python_permitted(self):
        with self.settings(ENABLE_PYTHON='true'):
            test_body = {
                'name': 'Test Field Mapper',
                'field_mapper_type': 'python',
            }
            form = FieldMapperForm(test_body)
            self.assertTrue(form.is_valid())

    def test_get_type_choices(self):
        choices = get_field_mapper_choices()
        self.assertEqual(choices, [
            ('xml2kvp', 'XML to Key/Value Pair (XML2kvp)'),
            ('xslt', 'XSL Stylesheet')
        ])

    def test_get_python_type_choices(self):
        with self.settings(ENABLE_PYTHON='true'):
            choices = get_field_mapper_choices()
            self.assertEqual(choices, [
                ('xml2kvp', 'XML to Key/Value Pair (XML2kvp)'),
                ('xslt', 'XSL Stylesheet'),
                ('python', 'Python Code Snippet')
            ])
