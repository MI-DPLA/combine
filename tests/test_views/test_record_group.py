from django.test import Client, TestCase
from django.urls import reverse

from core.models import RecordGroup, Organization, User


class RecordGroupTestCase(TestCase):
    def test_new_record_group(self):
        org = Organization.objects.create(name="Test Organization")
        user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        client = Client()
        client.force_login(user)
        response = client.post(reverse('record_group_new', args=[org.id]), {
            'organization': org.id,
            'name': 'Test Record Group'
        })
        record_group = RecordGroup.objects.get(name='Test Record Group')
        self.assertRedirects(response, reverse('record_group', args=[org.id, record_group.id]))
        redirect = client.get(response.url)
        self.assertIn('Test Record Group', str(redirect.content, 'utf-8'))
