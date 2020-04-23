import json
import jsonschema
from django.test import TestCase
from core.models import FieldMapper
from tests.utils import json_string

class FieldMapperTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.attributes = {
            'name': 'Test Field Mapper',
            'config_json': json_string({"add_literals":{"foo":"bar"}}),
            'field_mapper_type': 'xml2kvp'
        }
        cls.field_mapper = FieldMapper(**cls.attributes)

    def test_str(self):
        self.assertEqual('Test Field Mapper, FieldMapper: #{}'.format(FieldMapperTestCase.field_mapper.id),
            format(FieldMapperTestCase.field_mapper))

    def test_as_dict(self):
        as_dict = FieldMapperTestCase.field_mapper.as_dict()
        for k, v in FieldMapperTestCase.attributes.items():
            self.assertEqual(as_dict[k], v)

    def test_config(self):
        self.assertEqual(json.loads(FieldMapperTestCase.attributes['config_json']),
                         FieldMapperTestCase.field_mapper.config)

    def test_config_none(self):
        no_config_mapper = FieldMapper(name='new field mapper')
        self.assertIsNone(no_config_mapper.config)

    def test_validate_config_json(self):
        self.assertIsNone(FieldMapperTestCase.field_mapper.validate_config_json())

    def test_validate_config_json_invalid(self):
        invalid_config_mapper = FieldMapper(config_json=json_string({"add_literals": "invalid value"}))
        self.assertRaises(jsonschema.exceptions.ValidationError,
                          invalid_config_mapper.validate_config_json)

    def test_validate_config_json_provided(self):
        invalid_config_mapper = FieldMapper(config_json=json_string({"add_literals": "invalid value"}))
        self.assertIsNone(invalid_config_mapper.validate_config_json(json_string({"add_literals":{"foo":"bar"}})))
