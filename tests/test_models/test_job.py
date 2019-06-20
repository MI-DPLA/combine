from django.test import TestCase

from tests.utils import TestConfiguration


class JobModelTestCase(TestCase):

    def setUp(self) -> None:
        self.config = TestConfiguration()

    def test_downstream_jobs(self):
        downstream_jobs = self.config.job.get_downstream_jobs(include_self=False)
        self.assertEqual(len(downstream_jobs), 1)
        self.assertEqual(downstream_jobs.pop().name, 'Test Transform Job')

    def test_upstream_jobs(self):
        upstream_jobs = self.config.downstream_job.get_upstream_jobs(include_self=False)
        self.assertEqual(len(upstream_jobs), 1)
        self.assertEqual(upstream_jobs.pop().name, 'Test Job')

    def test_downstream_jobs_include_self(self):
        downstream_jobs = self.config.job.get_downstream_jobs()
        self.assertEqual(len(downstream_jobs), 2)
        names = set()
        names.add(downstream_jobs.pop().name)
        names.add(downstream_jobs.pop().name)
        self.assertSetEqual(names, {'Test Job', 'Test Transform Job'})

    def test_upstream_jobs_include_self(self):
        upstream_jobs = self.config.downstream_job.get_upstream_jobs()
        self.assertEqual(len(upstream_jobs), 2)
        names = set()
        names.add(upstream_jobs.pop().name)
        names.add(upstream_jobs.pop().name)
        self.assertSetEqual(names, {'Test Job', 'Test Transform Job'})
