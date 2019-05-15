from django.test import Client, TestCase

from core.models import Organization, User, Record, RecordGroup, Job
from tests.test_views.utils import TestConfiguration


class RecordTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client = Client()
        self.client.force_login(self.config.user)

    def test_get_record(self):
        record = Record.objects.create(
            job_id=self.config.job.id, record_id='testrecord')
        response = self.client.get(
            f'/combine/organization/{self.config.org.id}/record_group/{self.config.record_group.id}/job/{self.config.job.id}/record/{record.id}')
        self.assertIn('testrecord', str(response.content, 'utf-8'))
        self.assertIn(str(record.id), str(response.content, 'utf-8'))
