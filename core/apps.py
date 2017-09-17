# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import logging

# combine
from django.apps import AppConfig


# Get an instance of a logger
logger = logging.getLogger(__name__)


class CoreConfig(AppConfig):
	
	name = 'core'

