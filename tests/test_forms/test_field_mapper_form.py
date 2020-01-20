from django.test import TestCase
from core.forms import FieldMapperForm

class FormsTestCase(TestCase):
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
