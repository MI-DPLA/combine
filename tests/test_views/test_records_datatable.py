from django.test import TestCase

from core.views import DTRecordsJson
from core.models import Record
from tests.utils import TestConfiguration

class DTRecordsTestCase(TestCase):
    def test_get_initial_queryset(self):
        config = TestConfiguration()
        other_record = Record(job_id=9348275987)
        view = DTRecordsJson(kwargs={})
        records = view.get_initial_queryset()
        job_ids = set(map(lambda x: x.job.id, records))
        self.assertNotIn(0, job_ids)
