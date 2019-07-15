# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import logging

# django imports
from django.db import models

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


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
