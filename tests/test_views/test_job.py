from django.test import Client, TestCase

from core.models import User, Organization, RecordGroup


class JobTestCase(TestCase):
    def setUp(self):
        self.org = Organization.objects.create(name="Test Organization")
        self.user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        self.record_group = RecordGroup.objects.create(
            organization=self.org, name="Test Record Group")
        self.client = Client()
        self.client.force_login(self.user)

    def test_job_harvest_static_xml_form(self):
        response = self.client.get(
            f'/combine/organization/{self.org.id}/record_group/{self.record_group.id}/job/harvest/static/xml/new')
        self.assertIn(b"Harvest records from static XML files",
                      response.content)
