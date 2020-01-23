from django.test import TestCase

from core.models import PublishedRecords
from tests.utils import TestConfiguration


class PublishedTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_get_published(self):
        self.config.job.publish(publish_set_id='test publish id')
        publish_records = PublishedRecords().records
        # For some reason this accumulates records every time I run it
        # TODO: what the heck?
        print(publish_records.count())
        self.assertGreater(publish_records.count(), 0)
        published_page = self.client.get('/combine/published')
        self.assertIn('test publish id', str(published_page.content, 'utf-8'))
        self.assertIn(b'Published Records', published_page.content)
