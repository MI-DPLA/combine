from django.test import Client, TestCase

from tests.utils import TestConfiguration


class IndexTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_index(self):
        response = self.client.get('/combine/')
        self.assertIn(b'Welcome to Combine!', response.content)
