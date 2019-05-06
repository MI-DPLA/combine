from django.test import TestCase, Client

from core.models import CombineBackgroundTask, Job, Organization, RecordGroup, User


class BackgroundTaskTestCase(TestCase):
    def test_get_bg_tasks(self):
        c = Client()
        response = c.get('/combine/background_tasks')
        self.assertIn(b'Some tasks in Combine are long running and must be run in the background.', response.content)

    def test_get_bg_task(self):
        user = User.objects.create(username='combine', password='combine', is_superuser=True)
        c = Client()
        c.force_login(user)
        org = Organization.objects.create(name="Test Organization")
        record_group = RecordGroup.objects.create(organization=org,
                                                  name="Test Record Group")
        job = Job.objects.create(record_group=record_group,
                                 user=user,
                                 job_type="HarvestJob")
        task_params_json = {"job_id": job.id,
                            "record_group_id": record_group.id,
                            "org_id": org.id}
        bg_task = CombineBackgroundTask.objects.create(celery_task_id='test celery id',
                                                       task_type='job_reindex',
                                                       task_params_json=str(task_params_json).replace("\'", "\""))
        response = c.get(f'/combine/background_tasks/task/{bg_task.id}')
        self.assertIn(b'test celery id', response.content)
