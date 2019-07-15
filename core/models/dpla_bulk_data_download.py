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
