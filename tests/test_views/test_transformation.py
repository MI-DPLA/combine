from django.test import TestCase, Client

from core.models import Transformation


class TransformationTestCase(TestCase):
    def test_transformation_scenario_payload(self):
        c = Client()
        transformation = Transformation.objects.create(payload='test payload', transformation_type='python')
        response = c.get(f'/combine/configuration/transformation/{transformation.id}/payload')
        self.assertEqual(b'test payload', response.content)