from django.test import TestCase, Client

from core.models import CombineBackgroundTask
from tests.utils import TestConfiguration


class BackgroundTaskTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        self.config = TestConfiguration()
        task_params_json = {"job_id": self.config.job.id,
                            "record_group_id": self.config.record_group.id,
                            "org_id": self.config.org.id}
        self.bg_task = CombineBackgroundTask.objects.create(celery_task_id='test celery id',
                                                            task_type='job_reindex',
                                                            task_params_json=str(task_params_json).replace("\'", "\""))

    def test_get_bg_tasks(self):
        response = self.client.get('/combine/background_tasks')
        self.assertIn(
            b'Some tasks in Combine are long running and must be run in the background.', response.content)

    def test_get_bg_task(self):
        response = self.client.get(f'/combine/background_tasks/task/{self.bg_task.id}')
        self.assertIn(b'test celery id', response.content)
        self.assertIn(b'View Indexed Fields', response.content)

    def test_get_bg_task_no_job(self):
        new_task_params = {"record_group_id": self.config.record_group.id,
                           "org_id": self.config.org.id}
        new_task = CombineBackgroundTask.objects.create(celery_task_id='new celery id',
                                                        task_type='export_documents',
                                                        task_params_json=str(new_task_params).replace("\'", "\""))
        response = self.client.get(f'/combine/background_tasks/task/{new_task.id}')
        self.assertIn(b'new celery id', response.content)
        self.assertIn(b'Download Documents as Archive', response.content)


    def test_delete_all_bg_tasks(self):
        response = self.client.get('/combine/background_tasks/delete_all')
        self.assertRedirects(response, '/combine/background_tasks')

    def test_delete_bg_task(self):
        response = self.client.get(f'/combine/background_tasks/task/{self.bg_task.id}/delete')
        self.assertRedirects(response, '/combine/background_tasks')
        response = self.client.get('/combine/background_tasks')
        self.assertNotIn(b'test celery id', response.content)

    def test_cancel_bg_task(self):
        response = self.client.get(f'/combine/background_tasks/task/{self.bg_task.id}/cancel')
        self.assertRedirects(response, '/combine/background_tasks')
        # TODO: Can we test that it got canceled?
