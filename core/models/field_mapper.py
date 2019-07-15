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
