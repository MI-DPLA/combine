from django.test import Client, TestCase

from core import models

from tests.test_views.utils import TestConfiguration


class PublishedTestCase(TestCase):
    def setUp(self):
        self.c = Client()
        self.config = TestConfiguration()

    def test_get_published(self):
        self.c.force_login(self.config.user)
        self.config.job.publish(publish_set_id='test publish id')
        publish_records = models.PublishedRecords().records
        # For some reason this accumulates records every time I run it
        # TODO: what the heck?
        print(publish_records.count())
        self.assertGreater(publish_records.count(), 0)
        published_page = self.c.get('/combine/published')
        self.assertIn('test publish id', str(published_page.content, 'utf-8'))
        self.assertIn(b'Published Records', published_page.content)

