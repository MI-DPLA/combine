# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import inspect
import json
import jsonschema
import logging
from lxml import etree, isoschematron
from types import ModuleType

# django imports
from django.db import models

# import xml2kvp
from core.xml2kvp import XML2kvp
from core.es import es_handle
from core.spark.utils import PythonUDFRecord

from elasticsearch_dsl import Search

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


class ValidationScenario(models.Model):

    '''
    Model to handle validation scenarios used to validate records.
    '''

    name = models.CharField(max_length=255, null=True)
    payload = models.TextField()
    validation_type = models.CharField(
        max_length=255,
        choices=[
            ('sch', 'Schematron'),
            ('python', 'Python Code Snippet'),
            ('es_query', 'ElasticSearch DSL Query'),
            ('xsd', 'XML Schema')
        ]
    )
    filepath = models.CharField(max_length=1024, null=True, default=None, blank=True) # HiddenInput
    default_run = models.BooleanField(default=1)


    def __str__(self):
        return 'ValidationScenario: %s, validation type: %s, default run: %s' % (
            self.name, self.validation_type, self.default_run)

    def as_dict(self):
        return self.__dict__

    def validate_record(self, row):

        '''
        Method to test validation against a single record.

        Note: The code for self._validate_schematron() and self._validate_python() are similar, if not identical,
        to staticmethods found in core.spark.record_validation.py.    However, because those are running on spark workers,
        in a spark context, it makes it difficult to define once, but use in multiple places.   As such, these
        validations are effectively defined twice.

        Args:
            row (core.models.Record): Record instance, called "row" here to mirror spark job iterating over DataFrame
        '''

        # run appropriate validation based on type
        if self.validation_type == 'sch':
            result = self._validate_schematron(row)
        if self.validation_type == 'python':
            result = self._validate_python(row)
        if self.validation_type == 'es_query':
            result = self._validate_es_query(row)
        if self.validation_type == 'xsd':
            result = self._validate_xsd(row)

        # return result
        return result


    def _validate_schematron(self, row):

        # parse schematron
        sct_doc = etree.parse(self.filepath)
        validator = isoschematron.Schematron(sct_doc, store_report=True)

        # get document xml
        record_xml = etree.fromstring(row.document.encode('utf-8'))

        # validate
        is_valid = validator.validate(record_xml)

        # prepare results_dict
        results_dict = {
            'fail_count':0,
            'passed':[],
            'failed':[]
        }

        # temporarily add all tests to successes
        sct_root = sct_doc.getroot()
        nsmap = sct_root.nsmap

        # if schematron namespace logged as None, fix
        try:
            schematron_ns = nsmap.pop(None)
            nsmap['schematron'] = schematron_ns
        except:
            pass

        # get all assertions
        assertions = sct_root.xpath('//schematron:assert', namespaces=nsmap)
        for assertion in assertions:
            results_dict['passed'].append(assertion.text)

        # record total tests
        results_dict['total_tests'] = len(results_dict['passed'])

        # if not valid, parse failed
        if not is_valid:

            # get failed
            report_root = validator.validation_report.getroot()
            fails = report_root.findall('svrl:failed-assert', namespaces=report_root.nsmap)

            # log count
            results_dict['fail_count'] = len(fails)

            # loop through fails
            for fail in fails:

                # get fail test name
                fail_text_elem = fail.find('svrl:text', namespaces=fail.nsmap)

                # if in successes, remove
                if fail_text_elem.text in results_dict['passed']:
                    results_dict['passed'].remove(fail_text_elem.text)

                # append to failed
                results_dict['failed'].append(fail_text_elem.text)

        # return
        return {
            'parsed':results_dict,
            'raw':etree.tostring(validator.validation_report).decode('utf-8')
        }


    def _validate_python(self, row):

        # parse user defined functions from validation scenario payload
        temp_pyvs = ModuleType('temp_pyvs')
        exec(self.payload, temp_pyvs.__dict__)

        # get defined functions
        pyvs_funcs = []
        test_labeled_attrs = [attr for attr in dir(temp_pyvs) if attr.lower().startswith('test')]
        for attr in test_labeled_attrs:
            attr = getattr(temp_pyvs, attr)
            if inspect.isfunction(attr):
                pyvs_funcs.append(attr)

        # instantiate prvb
        prvb = PythonUDFRecord(row)

        # prepare results_dict
        results_dict = {
            'fail_count':0,
            'passed':[],
            'failed':[]
        }

        # record total tests
        results_dict['total_tests'] = len(pyvs_funcs)

        # loop through functions
        for func in pyvs_funcs:

            # get func test message
            signature = inspect.signature(func)
            t_msg = signature.parameters['test_message'].default

            # attempt to run user-defined validation function
            try:

                # run test
                test_result = func(prvb)

                # if fail, append
                if test_result != True:
                    results_dict['fail_count'] += 1
                    # if custom message override provided, use
                    if test_result != False:
                        results_dict['failed'].append(test_result)
                    # else, default to test message
                    else:
                        results_dict['failed'].append(t_msg)

                # if success, append to passed
                else:
                    results_dict['passed'].append(t_msg)

            # if problem, report as failure with Exception string
            except Exception as err:
                results_dict['fail_count'] += 1
                results_dict['failed'].append("test '%s' had exception: %s" % (func.__name__, str(err)))

        # return
        return {
            'parsed':results_dict,
            'raw':json.dumps(results_dict)
        }


    def _validate_es_query(self, row):

        '''
        Method to test ElasticSearch DSL query validation against row
            - NOTE: unlike the schematron and python validations, which run as
            python UDF functions in spark, the mechanics are slightly different here
            where this will run with Hadoop ES queries and unions in Spark

        Proposed structure:
        [
                {
                    "test_name":"record has mods_subject_topic",
                    "matches":"valid",
                    "es_query":{
                        "query":{
                            "exists":{
                                "field":"mods_subject_topic"
                            }
                        }
                    }
                },
                {
                    "test_name":"record does not have subject of Fiction",
                    "matches":"invalid",
                    "es_query":{
                        "query":{
                            "match":{
                                "mods_subject_topic.keyword":"Fiction"
                            }
                        }
                    }
                }
            ]
        '''

        # core models imports
        from core.models.job import CombineJob

        # parse es validation payload
        es_payload = json.loads(self.payload)

        # prepare results_dict
        results_dict = {
            'fail_count':0,
            'passed':[],
            'failed':[],
            'total_tests':len(es_payload)
        }

        # loop through tests in ES validation
        for test in es_payload:

            # get row's cjob
            cjob = CombineJob.get_combine_job(row.job.id)

            # init query with es_handle and es index
            query = Search(using=es_handle, index=cjob.esi.es_index)

            # update query with search body
            query = query.update_from_dict(test['es_query'])

            # add row to query
            query = query.query("term", db_id=str(row.id))

            # debug
            LOGGER.debug(query.to_dict())

            # execute query
            query_results = query.execute()

            # if hits.total > 0, assume a hit and call success
            if test['matches'] == 'valid':
                if query_results.hits.total > 0:
                    results_dict['passed'].append(test['test_name'])
                else:
                    results_dict['failed'].append(test['test_name'])
                    results_dict['fail_count'] += 1
            elif test['matches'] == 'invalid':
                if query_results.hits.total == 0:
                    results_dict['passed'].append(test['test_name'])
                else:
                    results_dict['failed'].append(test['test_name'])
                    results_dict['fail_count'] += 1

        # return
        return {
            'parsed':results_dict,
            'raw':json.dumps(results_dict)
        }


    def _validate_xsd(self, row):

        # prepare results_dict
        results_dict = {
            'total_tests':1,
            'fail_count':0,
            'passed':[],
            'failed':[]
        }

        # parse xsd
        xmlschema_doc = etree.parse(self.filepath)
        xmlschema = etree.XMLSchema(xmlschema_doc)

        # get document xml
        record_xml = etree.fromstring(row.document.encode('utf-8'))

        # validate
        try:
            xmlschema.assertValid(record_xml)
            validation_msg = 'Document is valid'
            results_dict['passed'].append(validation_msg)

        except etree.DocumentInvalid as err:
            validation_msg = str(err)
            results_dict['failed'].append(validation_msg)
            results_dict['fail_count'] += 1

        # return
        return {
            'parsed':results_dict,
            'raw':validation_msg
        }


class FieldMapper(models.Model):

    '''
    Model to handle different Field Mappers
    '''

    name = models.CharField(max_length=128, null=True)
    payload = models.TextField(null=True, default=None, blank=True)
    config_json = models.TextField(null=True, default=None, blank=True)
    field_mapper_type = models.CharField(
        max_length=255,
        choices=[
            ('xml2kvp', 'XML to Key/Value Pair (XML2kvp)'),
            ('xslt', 'XSL Stylesheet'),
            ('python', 'Python Code Snippet')]
    )


    def __str__(self):
        return '%s, FieldMapper: #%s' % (self.name, self.id)

    def as_dict(self):
        return self.__dict__

    @property
    def config(self):

        if self.config_json:
            return json.loads(self.config_json)
        return None

    def validate_config_json(self, config_json=None):

        # if config_json not provided, assume use self
        if not config_json:
            config_json = self.config_json

        # load config_json as dictionary
        config_dict = json.loads(config_json)

        # validate against XML2kvp schema
        jsonschema.validate(config_dict, XML2kvp.schema)


class RecordIdentifierTransformationScenario(models.Model):

    '''
    Model to manage transformation scenarios for Record's record_ids (RITS)
    '''

    name = models.CharField(max_length=255)
    transformation_type = models.CharField(
        max_length=255,
        choices=[('regex', 'Regular Expression'), ('python', 'Python Code Snippet'), ('xpath', 'XPath Expression')]
    )
    transformation_target = models.CharField(
        max_length=255,
        choices=[('record_id', 'Record Identifier'), ('document', 'Record Document')]
    )
    regex_match_payload = models.CharField(null=True, default=None, max_length=4096, blank=True)
    regex_replace_payload = models.CharField(null=True, default=None, max_length=4096, blank=True)
    python_payload = models.TextField(null=True, default=None, blank=True)
    xpath_payload = models.CharField(null=True, default=None, max_length=4096, blank=True)

    def __str__(self):
        return '%s, RITS: #%s' % (self.name, self.id)

    def as_dict(self):
        return self.__dict__


class DPLABulkDataDownload(models.Model):

    '''
    Model to handle the management of DPLA bulk data downloads
    '''

    s3_key = models.CharField(max_length=255, null=True, blank=True)
    downloaded_timestamp = models.DateTimeField(null=True, auto_now_add=True) # HiddenInput
    filepath = models.CharField(max_length=255, null=True, default=None, blank=True) # HiddenInput
    es_index = models.CharField(max_length=255, null=True, default=None, blank=True) # HiddenInput
    uploaded_timestamp = models.DateTimeField(null=True, default=None, auto_now_add=False, blank=True) # HiddenInput
    status = models.CharField(
        max_length=255,
        choices=[
            ('init', 'Initiating'),
            ('downloading', 'Downloading'),
            ('indexing', 'Indexing'),
            ('finished', 'Downloaded and Indexed')
        ],
        default='init'
    ) # HiddenInput

    def __str__(self):
        return '%s, DPLABulkDataDownload: #%s' % (self.s3_key, self.id)


    # name shim
    @property
    def name(self):
        return self.s3_key
