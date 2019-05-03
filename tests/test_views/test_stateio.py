from django.test import Client, TestCase

from core.models import StateIO, User


class StateIOTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create(username='combine', password='combine', is_superuser=True)
        self.client = Client()
        self.client.force_login(self.user)

    def test_state_io(self):
        in_io = StateIO.objects.create(name="test import i/o",
                                       stateio_type='import')
        out_io = StateIO.objects.create(name='test export i/o',
                                        stateio_type='export')
        response = self.client.get('/combine/stateio')
        self.assertIn(str(in_io.id), str(response.content, 'utf-8'))
        self.assertIn(str(out_io.id), str(response.content, 'utf-8'))
