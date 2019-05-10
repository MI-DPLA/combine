from django.test import Client, TestCase

from core.models import ValidationScenario, Organization, Record, RecordGroup, Job, User


class ValidationScenarioTestCase(TestCase):
    simple_validation_payload = """
def test_record_has_words(record, test_message='record has words'):
    return True
    """

    def test_save_validation_scenario(self):
        c = Client()
        response = c.post('/combine/configuration/save_validation_scenario',
                          {'vs_name': 'Test Validate',
                           'vs_payload': 'Some python code',
                           'vs_type': 'python'})
        json = response.json()
        self.assertIsNotNone(json['id'])
        self.assertEqual(json['name'], 'Test Validate')
        self.assertEqual(json['payload'], 'Some python code')
        self.assertEqual(json['validation_type'], 'python')

    def test_validation_scenario_payload(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some python code',
                                                     validation_type='python')
        c = Client()
        response = c.get(f'/combine/configuration/validation/{scenario.id}/payload')
        self.assertEqual(b'Some python code', response.content)

    def test_validation_scenario_payload_xml(self):
        scenario = ValidationScenario.objects.create(name='Test Validate',
                                                     payload='Some schematron',
                                                     validation_type='sch')
        c = Client()
        response = c.get(f'/combine/configuration/validation/{scenario.id}/payload')
        self.assertEqual(b'Some schematron', response.content)

    def test_validation_scenario_test(self):
        c = Client()
        response = c.get('/combine/configuration/test_validation_scenario')
        self.assertIn(b'Test Validation Scenario', response.content)

    def test_validation_scenario_test_post(self):
        org = Organization.objects.create(name="Test Organization")
        user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        record_group = RecordGroup.objects.create(
            organization=org, name="Test Record Group")
        job = Job.objects.create(record_group=record_group,
                                 user=user,
                                 job_type="HarvestJob")
        record = Record.objects.create(
            job_id=job.id,
            record_id='testrecord',
            document='Some strings!'
        )
        c = Client()
        response = c.post('/combine/configuration/test_validation_scenario',
                          {
                              'vs_payload': ValidationScenarioTestCase.simple_validation_payload,
                              'vs_type': 'python',
                              'db_id': record.id,
                              'vs_results_format': 'raw'
                          })
        self.assertEqual(b'{"fail_count": 0, "passed": ["record has words"], "failed": [], "total_tests": 1}', response.content)