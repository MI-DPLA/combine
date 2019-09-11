from django.test import TestCase

from tests.utils import TestConfiguration
from core.models import RecordGroup, Job, Record


class OrganizationModelTestCase(TestCase):
    def setUp(self) -> None:
        self.config = TestConfiguration()

    def test_all_jobs(self):
        other_rg = RecordGroup.objects.create(organization=self.config.org,
                                              name="Other Record Group")
        Job.objects.create(record_group=other_rg,
                           user=self.config.user,
                           job_type='MergeJob',
                           job_details='{"test_key": "test value"}',
                           name="Other Job")
        all_jobs = self.config.org.all_jobs()
        self.assertEqual(len(all_jobs), 3)
        assert(all_jobs[0].id < all_jobs[1].id)
        assert(all_jobs[1].id < all_jobs[2].id)

    def test_str(self):
        self.assertEqual(str(self.config.org), "Organization: Test Organization")

    def test_total_record_count(self):
        self.assertEqual(self.config.org.total_record_count(), 1)
        Record.objects.create(job_id=self.config.job.id,
                              record_id='testrecord2',
                              document='test document2')
        self.assertEqual(self.config.org.total_record_count(), 1)
        self.config.job.update_record_count()
        self.assertEqual(self.config.org.total_record_count(), 2)
