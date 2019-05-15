from django.test import Client, TestCase

from tests.test_views.utils import TestConfiguration


class ExportTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.c = Client()
        self.c.force_login(self.config.user)
        self.config.job.publish(publish_set_id='test publish id')

    def test_export_documents_job(self):
        response = self.c.post(f'/combine/export/documents/job/{self.config.job.id}')
        self.assertRedirects(response,
                             f'/combine/organization/{self.config.org.id}/record_group/{self.config.record_group.id}/job/{self.config.job.id}/details')

    def test_export_documents_published(self):
        response = self.c.post(f'/combine/export/documents/published/{self.config.job.id}')
        self.assertRedirects(response, '/combine/published')

