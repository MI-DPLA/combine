from django.test import TestCase

from core.models import GlobalMessageClient
from tests.utils import TestConfiguration


class GlobalMessagesTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_gm_delete(self):
        gmc = GlobalMessageClient()
        gmc.load_most_recent_session()
        gm_dict = {"id": "test_msg", "msg": "test global message"}
        gmc.add_gm(gm_dict)
        response = self.client.post('/combine/gm/delete', {
            "gm_id": "test_msg"
        })
        json = response.json()
        self.assertEqual(json['gm_id'], 'test_msg')
        self.assertEqual(json['num_removed'], 1)
