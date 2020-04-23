import json

from django.test import TestCase
from django.urls import reverse

from core.models import AnalysisJob
from tests.utils import TestConfiguration


class AnalysisTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_get_analysis(self):
        analysis_job = AnalysisJob(
            user=self.config.user,
            job_details={
                'job_name': 'Test Analysis Job',
                'job_note': '',
                'validation_scenarios': [],
                'input_job_ids': []
            }
        )
        response = self.client.get(reverse('analysis'))
        # TODO: test something real
        jobs = list(response.context['jobs'])
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].id, analysis_job.job.id)
        job_lineage = json.loads(response.context['job_lineage_json'])
        self.assertEqual(job_lineage['edges'], [])
        self.assertEqual(len(job_lineage['nodes']), 1)
        node = job_lineage['nodes'][0]
        self.assertEqual(node['id'], analysis_job.job.id)

    def test_get_job_analysis(self):
        self.config.job.publish('test_publish_set')
        response = self.client.get(reverse('job_analysis'))
        self.assertEqual(len(response.context['input_jobs']), 2)
        print(response.context['published'])

    def test_post_job_analysis(self):
        response = self.client.post(reverse('job_analysis'))
        self.assertRedirects(response, reverse('analysis'))
