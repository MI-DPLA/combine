from django.test import TestCase, Client

from core.models import GlobalMessageClient
from tests.test_views.utils import TestConfiguration


class GlobalMessagesTestCase(TestCase):
    def test_gm_delete(self):
        c = Client()
        config = TestConfiguration()
        c.force_login(config.user)
        gmc = GlobalMessageClient()
        gmc.load_most_recent_session()
        gm_dict = { "id": "test_msg", "msg": "test global message" }
        gmc.add_gm(gm_dict)
        response = c.post('/combine/gm/delete',
                          {
                              "gm_id": "test_msg"
                          })
        json = response.json()
        self.assertEqual(json['gm_id'], 'test_msg')
        self.assertEqual(json['num_removed'], 1)