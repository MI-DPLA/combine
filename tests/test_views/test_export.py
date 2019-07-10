from django.test import Client, TestCase
from django.urls import reverse

from tests.utils import TestConfiguration


class ExportTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)
        self.config.job.publish(publish_set_id='test publish id')

    def test_export_documents_job(self):
        response = self.client.post(reverse('export_documents', kwargs={'export_source': 'job',
                                                                        'job_id': self.config.job.id}))
        self.assertRedirects(response, reverse('job_details', args=[self.config.org.id,
                                                                    self.config.record_group.id,
                                                                    self.config.job.id]))

    def test_export_documents_published(self):
        response = self.client.post(reverse('export_documents', kwargs={'export_source': 'published',
                                                                        'job_id': self.config.job.id}))
        self.assertRedirects(response, reverse('published'))
