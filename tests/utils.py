from django.contrib.auth.models import User

from core.models import Organization, RecordGroup, Job, Record, GlobalMessageClient,\
        JobInput


class TestConfiguration:
    def __init__(self):
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        self.org = Organization.objects.create(name="Test Organization")
        self.record_group = RecordGroup.objects.create(organization=self.org,
                                                       name="Test Record Group")
        self.job = Job.objects.create(record_group=self.record_group,
                                      user=self.user,
                                      job_type="HarvestJob",
                                      job_details='{"test_key": "test value"}',
                                      name="Test Job")
        self.downstream_job = Job.objects.create(record_group=self.record_group,
                                                 user=self.user,
                                                 job_type="TransformJob",
                                                 job_details='{"test_key": "test value"}',
                                                 name="Test Transform Job")
        JobInput.objects.create(job=self.downstream_job,
                                input_job=self.job)
        # TODO: the test framework should be clearing all the dbs, not just mysql
        old_records = Record.objects(job_id=self.job.id)
        for record in old_records:
            record.delete()
        self.record = Record.objects.create(job_id=self.job.id,
                                            record_id='testrecord',
                                            document='test document')
        self.job.update_record_count()

    def record_group_path(self):
        return f'/combine/organization/{self.org.id}/record_group/{self.record_group.id}'

    def job_path(self, job=None):
        if job is not None:
            job_id = job.id
        else:
            job_id = self.job.id

        return f'{self.record_group_path()}/job/{job_id}'

    def record_path(self):
        return f'{self.job_path()}/record/{self.record.id}'


def most_recent_global_message():
    gmc = GlobalMessageClient()
    gmc.load_most_recent_session()
    gm = gmc.session['gms'][0]
    return gm

