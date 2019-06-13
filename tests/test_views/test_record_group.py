from django.test import Client, TestCase
from django.urls import reverse

from core.models import RecordGroup
from tests.utils import TestConfiguration


class RecordGroupTestCase(TestCase):
    def setUp(self) -> None:
        self.client = Client()
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
