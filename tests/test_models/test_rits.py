from django.test import TestCase
from core.models import RecordIdentifierTransformation

class RecordIdentifierTransformationTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.attributes = {
            'name': 'Test RITS',
            'transformation_type': 'regex',
            'transformation_target': 'document',
            'regex_match_payload': 'foo',
            'regex_replace_payload': 'bar'
        }
        cls.rits = RecordIdentifierTransformation(**cls.attributes)

    def test_str(self):
        self.assertEqual('Test RITS, RITS: #{}'.format(RecordIdentifierTransformationTestCase.rits.id),
                         format(RecordIdentifierTransformationTestCase.rits))

    def test_as_dict(self):
        as_dict = RecordIdentifierTransformationTestCase.rits.as_dict()
        for k, v in RecordIdentifierTransformationTestCase.attributes.items():
            self.assertEqual(as_dict[k], v)
