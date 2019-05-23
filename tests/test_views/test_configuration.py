from django.test import Client, TestCase
from django.urls import reverse

from core.models import User


class ConfigurationTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        self.client.force_login(self.user)

    def test_configuration(self):
        response = self.client.get(reverse('configuration'))
        # TODO: test something real
        print(response)

    def test_get_dpla_bulk_data_download(self):
        response = self.client.get(reverse('dpla_bulk_data_download'))
        # TODO: test something real
        print(response)
