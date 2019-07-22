from django.test import Client, TestCase
from django.urls import reverse

from core.models import RecordGroup, Job
from tests.utils import TestConfiguration, most_recent_global_message


class RecordGroupViewTestCase(TestCase):
    def setUp(self) -> None:
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_new_record_group(self):
        response = self.client.post(reverse('record_group_new', args=[self.config.org.id]), {
            'organization': self.config.org.id,
            'name': 'New Test Record Group'
        })
        record_group = RecordGroup.objects.get(name='New Test Record Group')
        self.assertRedirects(response, reverse('record_group', args=[self.config.org.id, record_group.id]))
        redirect = self.client.get(response.url)
        self.assertIn('Test Record Group', str(redirect.content, 'utf-8'))

    def test_record_group_run_jobs(self):
        other_rg = RecordGroup.objects.create(organization=self.config.org,
                                              name="Other Record Group")
        Job.objects.create(record_group=other_rg,
                           user=self.config.user,
                           job_type='MergeJob',
                           job_details='{"test_key": "test value"}',
                           name="Other Job")
        response = self.client.get(reverse('record_group_run_jobs', args=[self.config.org.id,
                                                                          self.config.record_group.id]))
        self.assertRedirects(response, reverse('organization', args=[self.config.org.id]))
        gm = most_recent_global_message()
        self.assertEqual(gm['html'], '<strong>Preparing to Rerun Job(s):</strong><br>Test Job<br>Test Transform Job')
        self.assertEqual(gm['class'], 'success')

    def test_record_group_stop_jobs(self):
        other_rg = RecordGroup.objects.create(organization=self.config.org,
                                              name="Other Record Group")
        Job.objects.create(record_group=other_rg,
                           user=self.config.user,
                           job_type='MergeJob',
                           job_details='{"test_key": "test value"}',
                           name="Other Job")
        response = self.client.get(reverse('record_group_stop_jobs', args=[self.config.org.id,
                                                                           self.config.record_group.id]))
        self.assertRedirects(response, reverse('organization', args=[self.config.org.id]))
        gm = most_recent_global_message()
        self.assertEqual(gm['html'], '<p><strong>Stopped Job(s):</strong><br>Test Job<br>Test Transform Job</p>')
        self.assertEqual(gm['class'], 'danger')
