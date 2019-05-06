from django.test import Client, TestCase

from core.models import User, Organization, RecordGroup, Job, Record


class ExportTestCase(TestCase):
    def test_export_documents(self):
        user = User.objects.create(username='combine', password='combine', is_superuser=True)
        c = Client()
        c.force_login(user)
        org = Organization.objects.create(name="Test Organization")
        record_group = RecordGroup.objects.create(organization=org,
                                                  name="Test Record Group")
        job = Job.objects.create(record_group=record_group,
                                 user=user,
                                 job_type="HarvestJob")
        Record.objects.create(job_id=job.id, record_id='testrecord', document='test document')
        job.publish(publish_set_id='test publish id')
        response = c.post(f'/combine/export/documents/job/{job.id}')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url,
                         f'/combine/organization/{org.id}/record_group/{record_group.id}/job/{job.id}/details')
