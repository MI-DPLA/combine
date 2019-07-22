from django.test import TestCase
from core.models import OAIEndpoint


class OAIEndpointTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.attributes = {
            'name': 'Test OAI Endpoint',
            'endpoint': 'http://oai.example.com',
            'verb': 'ListRecords',
            'metadataPrefix': 'mods',
            'scope_type': 'setList',
            'scope_value': 'someset, anotherset'
        }
        cls.oai_endpoint = OAIEndpoint(**cls.attributes)
        cls.oai_endpoint.save()

    def test_str(self):
        self.assertEqual('OAI endpoint: Test OAI Endpoint', format(OAIEndpointTestCase.oai_endpoint))

    def test_as_dict(self):
        as_dict = OAIEndpointTestCase.oai_endpoint.as_dict()
        for k, v in OAIEndpointTestCase.attributes.items():
            self.assertEqual(as_dict[k], v)