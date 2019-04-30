from django.test import Client, TestCase


class OrganizationTestCase(TestCase):
    def test_get_organizations(self):
        c = Client()
        response = c.get('/combine/organization/all')
        self.assertIn("No Organizations found!  Create one below...", str(response.content, 'utf-8'))