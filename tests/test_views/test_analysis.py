from django.test import Client, TestCase
from django.urls import reverse

from tests.utils import TestConfiguration


class AnalysisTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_get_analysis(self):
        response = self.client.get(reverse('analysis'))
        # TODO: test something real
        print(response)

    def test_get_job_analysis(self):
        response = self.client.get(reverse('job_analysis'))
        # TODO: test something real
        print(response)

    def test_post_job_analysis(self):
        response = self.client.post(reverse('job_analysis'))
        # TODO: test something real
        print(response)
