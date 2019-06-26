from django.test import TestCase

from tests.utils import TestConfiguration
from core.models import Job


class RecordGroupModelTestCase(TestCase):
    def setUp(self) -> None:
        self.config = TestConfiguration()

    def test_all_jobs(self):
        Job.objects.create(record_group=self.config.record_group,
                           user=self.config.user,
                           job_type='MergeJob',
                           job_details='{"test_key": "test value"}',
                           name="Other Job")
        all_jobs = self.config.org.all_jobs()
        self.assertEqual(len(all_jobs), 3)
        assert(all_jobs[0].id < all_jobs[1].id)
        assert(all_jobs[1].id < all_jobs[2].id)

    def test_total_record_count(self):
        self.assertEqual(self.config.record_group.total_record_count(), 1)

    def test_get_jobs_lineage(self):
        lineage = self.config.record_group.get_jobs_lineage()
        self.assertEqual(len(lineage['nodes']), 2)
        self.assertEqual(len(lineage['edges']), 1)

    def test_published_jobs(self):
        self.assertEqual(len(self.config.record_group.published_jobs()), 0)
        self.config.job.publish()
        published_jobs = self.config.record_group.published_jobs()
        self.assertEqual(len(published_jobs), 1)
        self.assertEqual(published_jobs.first().name, 'Test Job')

    def test_is_published(self):
        self.assertFalse(self.config.record_group.is_published())
        self.config.job.publish()
        self.assertTrue(self.config.record_group.is_published())