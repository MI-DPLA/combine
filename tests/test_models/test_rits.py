from django.test import TestCase
from core.models import RecordIdentifierTransformationScenario

class RecordIdentifierTransformationScenarioTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.attributes = {
            'name': 'Test RITS',
            'transformation_type': 'regex',
            'transformation_target': 'document',
            'regex_match_payload': 'foo',
            'regex_replace_payload': 'bar'
        }
        cls.rits = RecordIdentifierTransformationScenario(**cls.attributes)

    def test_str(self):
        self.assertEqual('Test RITS, RITS: #{}'.format(RecordIdentifierTransformationScenarioTestCase.rits.id),
                         format(RecordIdentifierTransformationScenarioTestCase.rits))

    def test_as_dict(self):
        as_dict = RecordIdentifierTransformationScenarioTestCase.rits.as_dict()
        for k, v in RecordIdentifierTransformationScenarioTestCase.attributes.items():
            self.assertEqual(as_dict[k], v)