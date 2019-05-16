from django.test import Client, TestCase

from core.models import Organization
from tests.test_views.utils import TestConfiguration


class OrganizationTestCase(TestCase):
    def setUp(self):
        self.c = Client()

    def test_get_organizations(self):
        response = self.c.get('/combine/organization/all')
        self.assertIn("No Organizations found!  Create one below...",
                      str(response.content, 'utf-8'))

    def test_post_organizations(self):
        response = self.c.post('/combine/organization/all',
                               {
                                   'name': 'Awesome Org',
                                   'description': 'Aw yeah'
                               })
        new_org = Organization.objects.get(name='Awesome Org')
        self.assertRedirects(response, f'/combine/organization/{new_org.id}')
        self.assertEqual(new_org.description, 'Aw yeah')

    def test_get_organization(self):
        config = TestConfiguration()
        response = self.c.get(f'/combine/organization/{config.org.id}')
        self.assertIn(b'Test Organization', response.content)
        self.assertIn(b'Test Record Group', response.content)

    def test_delete_organization(self):
        config = TestConfiguration()
        response = self.c.get(f'/combine/organization/{config.org.id}/delete')
        self.assertRedirects(response, '/combine/organization/all')
