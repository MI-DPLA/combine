from django.test import Client, TestCase

from core.models import FieldMapper


class FieldMapperTestCase(TestCase):
    def test_field_mapper_payload_config_json(self):
        c = Client()
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  config_json='{}')
        response = c.get(f'/combine/configuration/field_mapper/{field_mapper.id}/payload')
        self.assertEqual(b'{}', response.content)

    def test_field_mapper_payload(self):
        c = Client()
        field_mapper = FieldMapper.objects.create(field_mapper_type='xml2kvp',
                                                  payload='test payload')
        response = c.get(f'/combine/configuration/field_mapper/{field_mapper.id}/payload?type=payload')
        self.assertEqual(b'test payload', response.content)
