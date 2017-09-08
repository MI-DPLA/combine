# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from .models import RecordGroup, Job, OAIEndpoint, LivySession

# register models
admin.site.register([RecordGroup, Job, OAIEndpoint, LivySession])