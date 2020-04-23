from django.test import TestCase
from django.urls import reverse

from core.models import Transformation, OAIEndpoint, ValidationScenario, \
    RecordIdentifierTransformation, DPLABulkDataDownload, FieldMapper
from tests.utils import TestConfiguration


class ConfigurationTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.transformation = Transformation(name='Class Test Transformation')
        cls.transformation.save()
        cls.oai_endpoint = OAIEndpoint(name='Class Test OAI endpoint')
        cls.oai_endpoint.save()
        cls.validation_scenario = ValidationScenario(name='Class Test Validation Scenario',
                                                     validation_type='python')
        cls.validation_scenario.save()
        cls.rits = RecordIdentifierTransformation(name='Class Test RITS')
        cls.rits.save()
        cls.dpla_download = DPLABulkDataDownload()
        cls.dpla_download.save()
        cls.field_mapper = FieldMapper()
        cls.field_mapper.save()

    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_configuration(self):
        response = self.client.get(reverse('configuration'))
        self.assertEqual(list(response.context['transformations']),
                         [ConfigurationTestCase.transformation])
        self.assertEqual(list(response.context['oai_endpoints']),
                         [ConfigurationTestCase.oai_endpoint])
        self.assertEqual(list(response.context['validation_scenarios']),
                         [ConfigurationTestCase.validation_scenario])
        self.assertEqual(list(response.context['rits']),
                         [ConfigurationTestCase.rits])
        self.assertEqual(list(response.context['field_mappers']),
                         [ConfigurationTestCase.field_mapper])
        self.assertEqual(list(response.context['bulk_downloads']),
                         [ConfigurationTestCase.dpla_download])

    def test_get_dpla_bulk_data_download(self):
        response = self.client.get(reverse('dpla_bulk_data_download'))
        # TODO: this entire path is kind of out of date and disused
        self.assertFalse(response.context['bulk_data_keys'])
