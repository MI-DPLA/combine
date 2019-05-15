from django.test import TestCase, Client

from core.models import CombineBackgroundTask, Job, Organization, RecordGroup, User


class BackgroundTaskTestCase(TestCase):
    def setUp(self):
        self.c = Client()
        user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        org = Organization.objects.create(name="Test Organization")
        record_group = RecordGroup.objects.create(organization=org,
                                                  name="Test Record Group")
        job = Job.objects.create(record_group=record_group,
                                 user=user,
                                 job_type="HarvestJob")
        task_params_json = {"job_id": job.id,
                            "record_group_id": record_group.id,
                            "org_id": org.id}
        self.bg_task = CombineBackgroundTask.objects.create(celery_task_id='test celery id',
                                                            task_type='job_reindex',
                                                            task_params_json=str(task_params_json).replace("\'", "\""))

    def test_get_bg_tasks(self):
        response = self.c.get('/combine/background_tasks')
        self.assertIn(
            b'Some tasks in Combine are long running and must be run in the background.', response.content)

    def test_get_bg_task(self):
        response = self.c.get(f'/combine/background_tasks/task/{self.bg_task.id}')
        self.assertIn(b'test celery id', response.content)

    def test_delete_all_bg_tasks(self):
        response = self.c.get('/combine/background_tasks/delete_all')
        self.assertRedirects(response, '/combine/background_tasks')

    def test_delete_bg_task(self):
        response = self.c.get(f'/combine/background_tasks/task/{self.bg_task.id}/delete')
        self.assertRedirects(response, '/combine/background_tasks')
        response = self.c.get('/combine/background_tasks')
        self.assertNotIn(b'test celery id', response.content)

    def test_cancel_bg_task(self):
        response = self.c.get(f'/combine/background_tasks/task/{self.bg_task.id}/cancel')
        self.assertRedirects(response, '/combine/background_tasks')
        # TODO: Can we test that it got canceled?
