# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from .models import Organization, RecordGroup, Job, Record, OAIEndpoint, LivySession, Transformation, ValidationScenario, RecordIdentifierTransformationScenario

# register models
admin.site.register([Organization, RecordGroup, Job, Record, OAIEndpoint, LivySession, Transformation, ValidationScenario, RecordIdentifierTransformationScenario])