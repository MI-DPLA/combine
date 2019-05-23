from django.test import Client, TestCase
from django.urls import reverse

from core.models import User


class AnalysisTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        self.client.force_login(self.user)

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
