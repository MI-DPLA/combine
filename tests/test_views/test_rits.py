from django.test import Client, TestCase

from core.models import RecordIdentifierTransformationScenario


class RecordIdentifierTransformationScenarioTestCase(TestCase):
    def test_rits_payload(self):
        rits = RecordIdentifierTransformationScenario.objects.create(regex_match_payload='test match',
                                                                     regex_replace_payload='test replace',
                                                                     transformation_type='regex',
                                                                     transformation_target='document')
        c = Client()
        response = c.get(f'/combine/configuration/rits/{rits.id}/payload')
        json = response.json()
        self.assertEqual(json['transformation_type'], 'regex')
        self.assertEqual(json['transformation_target'], 'document')
        self.assertEqual(json['regex_match_payload'], 'test match')
        self.assertEqual(json['regex_replace_payload'], 'test replace')