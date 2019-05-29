from django.test import Client, TestCase
from django.urls import reverse

from core.models import Organization
from tests.test_views.utils import TestConfiguration


class OrganizationTestCase(TestCase):
    def setUp(self):
        self.client = Client()

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
        # This actually kicks off a background job, so it's hard to test that it worked
        config = TestConfiguration()
        response = self.client.get(reverse('organization_delete', args=[config.org.id]))
        self.assertRedirects(response, reverse('organizations'))
