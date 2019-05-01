from django.test import Client, TestCase

from core.models import Organization, User

class RecordGroupTestCase(TestCase):
    def test_new_record_group(self):
        org = Organization.objects.create(name="Test Organization")
        u = User.objects.create(username='combine', password='combine', is_superuser=True)
        c = Client()
        c.force_login(u)
        response = c.post(f'/combine/organization/{org.id}/record_group/new',
                          {'organization': org.id,
                           'name': 'Test Record Group'})
        self.assertEqual(response.status_code, 302)
        redirect = c.get(response.url)
        self.assertIn('Test Record Group', str(redirect.content, 'utf-8'))
