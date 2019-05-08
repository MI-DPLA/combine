from django.test import Client, TestCase

from core.models import User


class IndexTestCase(TestCase):
    def test_index(self):
        user = User.objects.create(username='combine', password='combine', is_superuser=True)
        c = Client()
        c.force_login(user)
        response = c.get('/combine/')
        self.assertIn(b'Welcome to Combine!', response.content)
