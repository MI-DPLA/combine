from django.test import Client, TestCase

from core.models import User


class AnalysisTestCase(TestCase):
    def setUp(self):
        self.c = Client()
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)

    def test_get_analysis(self):
        response = self.c.get('/combine/analysis')
        # TODO: test something real
        print(response)

    def test_get_job_analysis(self):
        self.c.force_login(self.user)
        response = self.c.get('/combine/analysis/new')
        # TODO: test something real
        print(response)

    def test_post_job_analysis(self):
        self.c.force_login(self.user)
        response = self.c.post('/combine/analysis/new')
        # TODO: test something real
        print(response)
