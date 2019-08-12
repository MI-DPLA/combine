from django.test import TestCase

from core.models import Record

class RecordModelTestCase(TestCase):

    def test_record_with_nonexistent_job(self):
        record = Record(job_id=2345789)
        self.assertEqual(record.job.name, 'No Job')
        self.assertEqual(record.job.record_group, 'No Record Group')
