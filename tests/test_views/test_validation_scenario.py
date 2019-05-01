from django.test import Client, TestCase


class ValidationScenarioTestCase(TestCase):
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
