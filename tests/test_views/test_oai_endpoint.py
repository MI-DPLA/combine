from django.test import Client, TestCase
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from core.models import OAIEndpoint
from tests.test_views.utils import TestConfiguration


class OAIEndpointTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        self.config = TestConfiguration()
        self.client.force_login(self.config.user)

    def test_oai_endpoint_payload(self):
        endpoint = OAIEndpoint.objects.create(name='Test OAI',
                                              metadataPrefix='prefix')
        response = self.client.get(reverse('oai_endpoint_payload', args=[endpoint.id]))
        json = response.json()
        self.assertEqual(json['name'], 'Test OAI')

    def test_create_oai_endpoint_get(self):
        response = self.client.get(reverse('create_oai_endpoint'))
        self.assertIn(b'Create new OAI Endpoint', response.content)

    def test_create_oai_endpoint_post(self):
        post_body = {
            'name': 'Test OAI Endpoint',
            'endpoint': 'some endpoint',
            'verb': 'ListRecords',
            'metadataPrefix': 'prefix',
            'scope_type': 'setList',
            'scope_value': 'true'
        }
        response = self.client.post(reverse('create_oai_endpoint'), post_body)
        self.assertRedirects(response, reverse('configuration'))
        endpoint = OAIEndpoint.objects.get(name='Test OAI Endpoint')
        self.assertIsNotNone(endpoint.id)
        endpoint_dict = endpoint.as_dict()
        for item in post_body:
            self.assertEqual(endpoint_dict[item], post_body[item])

    def test_create_oai_endpoint_invalid(self):
        response = self.client.post(reverse('create_oai_endpoint'), {})
        self.assertIn(b'This field is required.', response.content)

    def test_edit_oai_endpoint_get(self):
        endpoint = OAIEndpoint.objects.create(name='Test OAI',
                                              metadataPrefix='prefix')
        response = self.client.get(reverse('edit_oai_endpoint', args=[endpoint.id]))
        self.assertIn(b'Test OAI', response.content)

    def test_edit_oai_endpoint_post(self):
        endpoint = OAIEndpoint.objects.create(name='Test OAI',
                                              metadataPrefix='prefix')
        endpoint_id = endpoint.id
        post_body = {
            'name': 'Test OAI Endpoint',
            'endpoint': 'some endpoint',
            'verb': 'ListRecords',
            'metadataPrefix': 'prefix',
            'scope_type': 'setList',
            'scope_value': 'true',
        }
        response = self.client.post(reverse('edit_oai_endpoint', args=[endpoint.id]), post_body)
        self.assertRedirects(response, reverse('configuration'))
        endpoint = OAIEndpoint.objects.get(pk=int(endpoint_id))
        endpoint_dict = endpoint.as_dict()
        for item in post_body:
            self.assertEqual(endpoint_dict[item], post_body[item])

    def test_edit_oai_endpoint_invalid(self):
        endpoint = OAIEndpoint.objects.create(name='Test OAI',
                                              metadataPrefix='prefix')
        post_body = {
            'name': 'Test OAI Endpoint',
        }
        response = self.client.post(reverse('edit_oai_endpoint', args=[endpoint.id]), post_body)
        self.assertIn(b'This field is required.', response.content)

    def test_delete_oai_endpoint(self):
        endpoint = OAIEndpoint.objects.create(name='Test OAI',
                                              metadataPrefix='prefix')
        response = self.client.delete(reverse('delete_oai_endpoint', args=[endpoint.id]))
        self.assertRedirects(response, reverse('configuration'))
        with self.assertRaises(ObjectDoesNotExist):
            OAIEndpoint.objects.get(pk=int(endpoint.id))

    def test_delete_oai_endpoint_nonexistent(self):
        response = self.client.delete(reverse('delete_oai_endpoint', args=[12345]))
        self.assertRedirects(response, reverse('configuration'))
