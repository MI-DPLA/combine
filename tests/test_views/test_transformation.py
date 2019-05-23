from django.test import TestCase, Client
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from tests.test_views.utils import TestConfiguration
from core.models import Transformation


class TransformationTestCase(TestCase):
    def setUp(self):
        self.config = TestConfiguration()
        self.c = Client()
        self.c.force_login(self.config.user)  # The configuration page requires login

    def test_create_transformation_scenario_get(self):
        response = self.c.get(reverse('create_transformation_scenario'))
        self.assertIn(b'Create new Transformation Scenario', response.content)

    def test_create_transformation_scenario_post(self):
        post_body = {
            'name': 'Test Transform',
            'payload': 'test payload',
            'transformation_type': 'python'
        }
        response = self.c.post(reverse('create_transformation_scenario'), post_body)
        self.assertRedirects(response, '/combine/configuration')
        transform = Transformation.objects.get(name='Test Transform')
        self.assertIsNotNone(transform.id)
        transform_dict = transform.as_dict()
        for item in post_body:
            self.assertEqual(transform_dict[item], post_body[item])

    def test_transformation_scenario_get(self):
        transformation = Transformation.objects.create(
            name='Test Transform',
            payload='test payload',
            transformation_type='python')
        response = self.c.get(reverse('transformation_scenario', args=[transformation.id]))
        self.assertIn(b'Test Transform', response.content)

    def test_transformation_scenario_post(self):
        transformation = Transformation.objects.create(
            name='Test Transform',
            payload='test payload',
            transformation_type='python')
        response = self.c.post(reverse('transformation_scenario', args=[transformation.id]), {
            'payload': 'some other payload',
            'name': transformation.name,
            'transformation_type': transformation.transformation_type
        })
        self.assertRedirects(response, '/combine/configuration')
        transform =Transformation.objects.get(name='Test Transform')
        self.assertIsNotNone(transform.id)
        self.assertEqual(transform.payload, 'some other payload')
        self.assertEqual(transform.transformation_type, 'python')

    def test_transformation_scenario_delete(self):
        transformation = Transformation.objects.create(
            name='Test Transform',
            payload='test payload',
            transformation_type='python')
        response = self.c.delete(reverse('delete_transformation_scenario', args=[transformation.id]))
        self.assertRedirects(response, '/combine/configuration')
        try:
            Transformation.objects.get(pk=int(transformation.id))
            self.fail('Did not delete transformation')
        except ObjectDoesNotExist:
            pass

    def test_transformation_scenario_delete_nonexistent(self):
        response = self.c.delete(reverse('delete_transformation_scenario', args=[12345]))
        self.assertRedirects(response, '/combine/configuration')

    def test_transformation_scenario_payload(self):
        transformation = Transformation.objects.create(
            payload='test payload', transformation_type='python')
        response = self.c.get(reverse('transformation_scenario_payload', args=[transformation.id]))
        self.assertEqual(b'test payload', response.content)
