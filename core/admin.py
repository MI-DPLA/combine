# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from .models import Organization, RecordGroup, Job, OAIEndpoint, LivySession, Transformation

# register models
admin.site.register([Organization, RecordGroup, Job, OAIEndpoint, LivySession, Transformation])