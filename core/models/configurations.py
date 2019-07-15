# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import json
import jsonschema
import logging

# django imports
from django.db import models

# import xml2kvp
from core.xml2kvp import XML2kvp

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


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
