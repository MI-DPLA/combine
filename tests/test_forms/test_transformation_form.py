from django.test import TestCase

from core.forms import TransformationForm
from core.models import get_transformation_type_choices


class TransformationFormTestCase(TestCase):
    def test_python_prohibited(self):
        test_body = {
            'name': 'Test Transformation',
            'payload': 'test payload',
            'transformation_type': 'python'
        }
        form = TransformationForm(test_body)
        self.assertFalse(form.is_valid())

    def test_python_permitted(self):
        with self.settings(ENABLE_PYTHON='true'):
            test_body = {
                'name': 'Test Transformation',
                'payload': 'test payload',
                'transformation_type': 'python'
            }
            form = TransformationForm(test_body)
            self.assertTrue(form.is_valid())

    def test_get_transformation_choices(self):
        choices = get_transformation_type_choices()
        self.assertEqual(choices, [
            ('xslt', 'XSLT Stylesheet'),
            ('openrefine', 'Open Refine Actions')
        ])

    def test_get_python_transformation_choices(self):
        with self.settings(ENABLE_PYTHON='true'):
            choices = get_transformation_type_choices()
            self.assertEqual(choices, [
                ('xslt', 'XSLT Stylesheet'),
                ('openrefine', 'Open Refine Actions'),
                ('python', 'Python Code Snippet')
            ])
