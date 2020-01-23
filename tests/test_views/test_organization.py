from django.test import TestCase
from django.urls import reverse

from core.models import Organization, RecordGroup, Job
from tests.utils import TestConfiguration, most_recent_global_message


class OrganizationViewTestCase(TestCase):

    def test_get_organizations(self):
        response = self.client.get(reverse('organizations'))
        self.assertIn("No Organizations found!  Create one below...",
                      str(response.content, 'utf-8'))

    def test_post_organizations(self):
        response = self.client.post(reverse('organizations'), {
            'name': 'Awesome Org',
            'description': 'Aw yeah'
        })
        new_org = Organization.objects.get(name='Awesome Org')
        self.assertRedirects(response, reverse('organization', args=[new_org.id]))
        self.assertEqual(new_org.description, 'Aw yeah')

    def test_get_organization(self):
        config = TestConfiguration()
        response = self.client.get(reverse('organization', args=[config.org.id]))
        self.assertIn(b'Test Organization', response.content)
        self.assertIn(b'Test Record Group', response.content)

    def test_delete_organization(self):
        # TODO: This actually kicks off a background job, so it's hard to test that it worked
        config = TestConfiguration()
        response = self.client.get(reverse('organization_delete', args=[config.org.id]))
        self.assertRedirects(response, reverse('organizations'))

    def test_organization_run_jobs(self):
        config = TestConfiguration()
        other_org = Organization.objects.create(name="Other Org")
        other_rg = RecordGroup.objects.create(organization=other_org,
                                              name="Other Record Group")
        Job.objects.create(record_group=other_rg,
                           user=config.user,
                           job_type='MergeJob',
                           job_details='{"test_key": "test value"}',
                           name="Other Job")
        response = self.client.get(reverse('organization_run_jobs', args=[config.org.id]))
        self.assertRedirects(response, reverse('organizations'))
        gm = most_recent_global_message()
        self.assertEqual(gm['html'], '<strong>Preparing to Rerun Job(s):</strong><br>Test Job<br>Test Transform Job')
        self.assertEqual(gm['class'], 'success')

    def test_organization_stop_jobs(self):
        config = TestConfiguration()
        other_org = Organization.objects.create(name="Other Org")
        other_rg = RecordGroup.objects.create(organization=other_org,
                                              name="Other Record Group")
        Job.objects.create(record_group=other_rg,
                           user=config.user,
                           job_type='MergeJob',
                           job_details='{"test_key": "test value"}',
                           name="Other Job")
        response = self.client.get(reverse('organization_stop_jobs', args=[config.org.id]))
        self.assertRedirects(response, reverse('organizations'))
        gm = most_recent_global_message()
        self.assertEqual(gm['html'], '<p><strong>Stopped Job(s):</strong><br>Test Job<br>Test Transform Job</p>')
        self.assertEqual(gm['class'], 'danger')

