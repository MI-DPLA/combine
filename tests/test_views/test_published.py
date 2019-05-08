from django.test import Client, TestCase

from core import models
from core.models import Job, Organization, Record, RecordGroup, User


class PublishedTestCase(TestCase):
    def test_get_published(self):
        user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        c = Client()
        c.force_login(user)
        org = Organization.objects.create(name="Test Organization")
        record_group = RecordGroup.objects.create(organization=org,
                                                  name="Test Record Group")
        job = Job.objects.create(record_group=record_group,
                                 user=user,
                                 job_type="HarvestJob")
        Record.objects.create(
            job_id=job.id, record_id='testrecord', document='test document')
        job.publish(publish_set_id='test publish id')
        publish_records = models.PublishedRecords().records
        # For some reason this accumulates records every time I run it
        # TODO: what the heck?
        print(publish_records.count())
        self.assertGreater(publish_records.count(), 0)
        published_page = c.get('/combine/published')
        self.assertIn('test publish id', str(published_page.content, 'utf-8'))
        self.assertIn(b'Published Records', published_page.content)
