from django.test import Client, TestCase

from core.models import OAIEndpoint, User


class ConfigurationTestCase(TestCase):
    def setUp(self):
        self.c = Client()
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        self.c.force_login(self.user)

    def test_configuration(self):
        response = self.c.get('/combine/configuration')
        # TODO: test something real
        print(response)

    def test_get_dpla_bulk_data_download(self):
        response = self.c.get('/combine/configuration/dpla_bulk_data/download')
        # TODO: test something real
        print(response)
