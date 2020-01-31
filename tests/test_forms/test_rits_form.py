from django.test import TestCase

from core.forms import RITSForm
from core.models import get_rits_choices


class RITSFormTestCase(TestCase):
    def test_python_prohibited(self):
        test_body = {
            'name': 'Test RITS',
            'transformation_type': 'python',
        }
        form = RITSForm(test_body)
        self.assertFalse(form.is_valid())

    def test_python_permitted(self):
        with self.settings(ENABLE_PYTHON='true'):
            test_body = {
                'name': 'Test RITS',
                'transformation_type': 'python',
            }
            form = RITSForm(test_body)
            self.assertFalse(form.is_valid())

    def test_get_type_choices(self):
        choices = get_rits_choices()
        self.assertEqual(choices, [
            ('regex', 'Regular Expression'),
            ('xpath', 'XPath')
        ])

    def test_get_python_type_choices(self):
        with self.settings(ENABLE_PYTHON='true'):
            choices = get_rits_choices()
            self.assertEqual(choices, [
                ('regex', 'Regular Expression'),
                ('xpath', 'XPath'),
                ('python', 'Python Code Snippet')
            ])
