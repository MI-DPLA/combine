from core.models import User, Organization, RecordGroup, Job, Record


class TestConfiguration:
    def __init__(self):
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        self.record = None
        self.org = Organization.objects.create(name="Test Organization")
        self.record_group = RecordGroup.objects.create(organization=self.org,
                                                  name="Test Record Group")
        self.job = Job.objects.create(record_group=self.record_group,
                                 user=self.user,
                                 job_type="HarvestJob")
        self.record = Record.objects.create(job_id=self.job.id,
                                            record_id='testrecord',
                                            document='test document')
