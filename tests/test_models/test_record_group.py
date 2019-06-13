from django.test import TestCase

from tests.utils import TestConfiguration
from core.models import Job


class RecordGroupModelTestCase(TestCase):
    def test_all_jobs(self):
        config = TestConfiguration()
        Job.objects.create(record_group=config.record_group,
                           user=config.user,
                           job_type='MergeJob',
                           job_details='{"test_key": "test value"}',
                           name="Other Job")
        all_jobs = config.org.all_jobs()
        self.assertEqual(len(all_jobs), 2)
        assert(all_jobs[0].id < all_jobs[1].id)
