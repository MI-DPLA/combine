from django.test import TestCase

from tests.utils import TestConfiguration

from core.models import HarvestTabularDataJob, HarvestStaticXMLJob, HarvestOAIJob,\
    MergeJob, TransformJob


class JobModelTestCase(TestCase):

    def setUp(self) -> None:
        self.config = TestConfiguration()

    def test_job_type_family_harvest_oai(self):
        job_details = {
            'job_name': 'Harvest McHarvestFace',
            'job_note': '',
            'validation_scenarios': []
        }
        cjob = HarvestOAIJob(job_details=job_details,
                             record_group=self.config.record_group,
                             user=self.config.user)
        self.assertEqual(cjob.job.job_type_family(), 'HarvestJob')