from django.test import TestCase
from core.forms import RITSForm

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
