from django.test import Client, TestCase
from django.core.urlresolvers import reverse


from core.models import Job, RecordGroup
from tests.test_views.utils import TestConfiguration


class JobTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.c = Client()
        self.c.force_login(self.config.user)

    def test_job_harvest_static_xml_form(self):
        response = self.c.get(
            f'{self.config.record_group_path()}/job/harvest/static/xml/new')
        self.assertIn(b"Harvest records from static XML files",
                      response.content)

    def test_job_id_redirect(self):
        response = self.c.get(f'/combine/job/{self.config.job.id}')
        self.assertRedirects(response,
                             f'{self.config.job_path()}/details')

    def test_all_jobs(self):
        response = self.c.get('/combine/jobs/all')
        self.assertIn(b'All Jobs', response.content)

    def test_job_delete(self):
        response = self.c.post(f'{self.config.job_path()}/delete', {}, HTTP_REFERER=reverse('all_jobs'))
        self.assertRedirects(response, '/combine/jobs/all')

    def test_stop_jobs(self):
        response = self.c.post('/combine/jobs/stop_jobs')
        self.assertEqual(response.json()['results'], True)

    def test_delete_jobs(self):
        response = self.c.post('/combine/jobs/delete_jobs')
        self.assertEqual(response.json()['results'], True)

    def test_move_jobs(self):
        new_record_group = RecordGroup.objects.create(organization=self.config.org,
                                                      name='New Test Record Group')
        response = self.c.post('/combine/jobs/move_jobs',
                               {
                                   "job_ids[]": [ self.config.job.id ],
                                   "record_group_id": [ new_record_group.id ]
                               })
        self.assertEqual(response.json()['results'], True)
        job = Job.objects.get(id=self.config.job.id)
        self.assertEqual(job.record_group_id, new_record_group.id)

    def test_job_errors(self):
        new_job = Job.objects.create(record_group=self.config.record_group,
                                     user=self.config.user,
                                     job_type="HarvestStaticXMLJob")
        response = self.c.get(f'{self.config.job_path(new_job)}/errors')
        self.assertIn(b'Errors in Processing:', response.content)

    def test_job_update_note(self):
        response = self.c.post(f'{self.config.job_path()}/update_note',
                               {
                                   'job_note': 'test job note'
                               },
                               HTTP_REFERER=reverse('all_jobs'))
        self.assertRedirects(response, '/combine/jobs/all')
        job = Job.objects.get(id=self.config.job.id)
        self.assertEqual(job.note, 'test job note')

    def test_job_update_name(self):
        response = self.c.post(f'{self.config.job_path()}/update_name',
                               {
                                   'job_name': 'new name'
                               },
                               HTTP_REFERER=reverse('all_jobs'))
        self.assertRedirects(response, '/combine/jobs/all')
        job = Job.objects.get(id=self.config.job.id)
        self.assertEqual(job.name, 'new name')

    def test_job_publish(self):
        response = self.c.post(f'{self.config.job_path()}/publish',
                               {
                                   'publish_set_id': 'test publish set id'
                               })
        self.assertRedirects(response, self.config.record_group_path())
        # TODO: test that it actually published
