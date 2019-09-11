from django.test import Client, TestCase

from core.models import ValidationScenario
from tests.utils import TestConfiguration, TEST_DOCUMENT


class RecordTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client = Client()
        self.client.force_login(self.config.user)

    def test_get_record(self):
        response = self.client.get(f'{self.config.record_path()}')
        self.assertIn(self.config.record.record_id, str(response.content, 'utf-8'))
        self.assertIn(str(self.config.record.id), str(response.content, 'utf-8'))

    def test_record_document(self):
        response = self.client.get(f'{self.config.record_path()}/document')
        self.assertEqual(TEST_DOCUMENT, str(response.content, 'utf-8'))

    def test_record_indexed_document(self):
        response = self.client.get(f'{self.config.record_path()}/indexed_document')
        self.assertEqual(b'{}', response.content)

    def test_record_error(self):
        self.config.record.error = 'test error'
        self.config.record.save()
        response = self.client.get(f'{self.config.record_path()}/error')
        self.assertEqual(b'<pre>test error</pre>', response.content)

    def test_record_validation_scenario(self):
        validation_scenario = ValidationScenario.objects.create(name='Test Validation',
                                                                payload="""
def test_record_has_words(record, test_message='record has words'):
    return True
            """,
                                                                validation_type='python')
        response = self.client.get(f'{self.config.record_path()}/validation_scenario/{validation_scenario.id}')
        self.assertEqual(b'{"fail_count": 0, "passed": ["record has words"], "failed": [], "total_tests": 1}',
                         response.content)

    def test_record_combined_diff_html(self):
        response = self.client.get(f'{self.config.record_path()}/diff/combined')
        self.assertEqual(b'Record was not altered during Transformation.', response.content)

    def test_record_side_by_side_diff_html(self):
        response = self.client.get(f'{self.config.record_path()}/diff/side_by_side')
        self.assertEqual(b'Record was not altered during Transformation.', response.content)
