from django.test import TestCase
from core.models import ValidationScenario
from tests.utils import TestConfiguration

SCHEMATRON_PAYLOAD = '''<?xml version="1.0" encoding="UTF-8"?>
<schema xmlns="http://purl.oclc.org/dsdl/schematron" xmlns:internet="http://internet.com">
	<ns prefix="internet" uri="http://internet.com"/>
	<!-- Required top level Elements for all records record -->
	<pattern>
		<title>Required Elements for Each MODS record</title>
		<rule context="root">
			<assert test="titleInfo">There must be a title element</assert>
		</rule>
	</pattern>
</schema>'''

class ValidationScenarioTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.schematron_attributes = {
            'name': 'Test Schematron Validation',
            'payload': SCHEMATRON_PAYLOAD,
            'validation_type': 'sch',
            'default_run': False
        }
        cls.schematron_validation_scenario = ValidationScenario(**cls.schematron_attributes)
        cls.schematron_validation_scenario.save()
        cls.config = TestConfiguration()

    def test_str(self):
        self.assertEqual('ValidationScenario: Test Schematron Validation, validation type: sch, default run: False',
                         format(ValidationScenarioTestCase.schematron_validation_scenario))

    def test_as_dict(self):
        as_dict = ValidationScenarioTestCase.schematron_validation_scenario.as_dict()
        for k, v in ValidationScenarioTestCase.schematron_attributes.items():
            self.assertEqual(as_dict[k], v)

    def test_validate_record_schematron(self):
        record = ValidationScenarioTestCase.config.record
        validation = ValidationScenarioTestCase.schematron_validation_scenario.validate_record(record)
        parsed = validation['parsed']
        self.assertEqual(parsed['passed'], [])
        self.assertEqual(parsed['fail_count'], 1)
        self.assertEqual(parsed['failed'], ['There must be a title element'])

    def test_validate_record_python(self):
        python_validation = '''from lxml import etree
def test_has_foo(row, test_message="There must be a foo"):
    doc_xml = etree.fromstring(row.document.encode('utf-8'))
    foo_elem_query = doc_xml.xpath('foo', namespaces=row.nsmap)
    return len(foo_elem_query) > 0
'''
        python_validation_scenario = ValidationScenario(
            name='Test Python Validation',
            payload=python_validation,
            validation_type='python'
        )
        validation = python_validation_scenario.validate_record(ValidationScenarioTestCase.config.record)
        parsed = validation['parsed']
        self.assertEqual(parsed['fail_count'], 0)
        self.assertEqual(parsed['passed'], ['There must be a foo'])

    def test_validate_record_es_query(self):
        # TODO: write a test :|
        pass

    def test_validate_record_xsd(self):
        # TODO: write a test :|
        pass
