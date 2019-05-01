from django.test import Client, TestCase

from core.models import Organization, User, Record, RecordGroup, Job


class RecordTestCase(TestCase):
    def setUp(self):
        self.org = Organization.objects.create(name="Test Organization")
        self.user = User.objects.create(username='combine', password='combine', is_superuser=True)
        self.record_group = RecordGroup.objects.create(organization=self.org, name="Test Record Group")
        self.job = Job.objects.create(record_group=self.record_group,
                                      user=self.user,
                                      job_type="HarvestJob")
        self.client = Client()
        self.client.force_login(self.user)

    def test_get_record(self):
        record = Record.objects.create(job_id=self.job.id, record_id='testrecord')
        response = self.client.get(f'/combine/organization/{self.org.id}/record_group/{self.record_group.id}/job/{self.job.id}/record/{record.id}')
        self.assertIn('testrecord', str(response.content, 'utf-8'))
        self.assertIn(str(record.id), str(response.content, 'utf-8'))
