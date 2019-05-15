from django.test import Client, TestCase

from core.models import User
from core import models


class ExternalBackgroundTaskTestCase(TestCase):
    def setUp(self):
        self.c = Client()
        user = User.objects.create(
            username='combine', password='combine', is_superuser=True)
        self.c.force_login(user)

    def test_get_system(self):
        response = self.c.get('/combine/system')
        self.assertIn(b'Livy/Spark Session', response.content)
        self.assertIn(b'Background Tasks', response.content)

    def test_livy_session_start(self):
        response = self.c.get('/combine/system/livy_sessions/start')
        self.assertRedirects(response, '/combine/system')
        response = self.c.get(response.url)
        self.assertIn(b'Livy Session, sessionId', response.content)
        self.assertIn(b'starting', response.content)

    def test_livy_session_stop(self):
        livy_session = models.LivySession()
        livy_session.start_session()
        livy_session.save()
        response = self.c.get(f'/combine/system/livy_sessions/{livy_session.session_id}/stop')
        self.assertRedirects(response, '/combine/system')

    def test_system_bg_status(self):
        response = self.c.get('/combine/system/bg_status')
        json = response.json()
        # TODO: why?
        self.assertEqual(json['celery_status'], 'unknown')
        self.assertEqual(json['livy_status'], 'stopped')
