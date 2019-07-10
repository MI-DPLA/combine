from django.test import Client, TestCase
from django.urls import reverse

from tests.utils import TestConfiguration


class ConfigurationTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_configuration(self):
        response = self.client.get(reverse('configuration'))
        # TODO: test something real
        print(response)

    def test_get_dpla_bulk_data_download(self):
        response = self.client.get(reverse('dpla_bulk_data_download'))
        # TODO: test something real
        print(response)
