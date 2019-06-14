# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import datetime
import logging
from itertools import chain

# django imports
from django.db import models

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


class Organization(models.Model):

    '''
    Model to manage Organizations in Combine.
    Organizations contain Record Groups, and are the highest level of organization in Combine.
    '''

    name = models.CharField(max_length=128)
    description = models.CharField(max_length=255, blank=True)
    timestamp = models.DateTimeField(null=True, auto_now_add=True)
    for_analysis = models.BooleanField(default=0)

    def __str__(self):
        return 'Organization: %s' % self.name

    def total_record_count(self):

        '''
        Method to determine total records under this Org
        '''

        total_record_count = 0

        # loop through record groups
        for record_group in self.recordgroup_set.all():

            # loop through jobs
            for job in record_group.job_set.all():

                total_record_count += job.record_count

        # return
        return total_record_count

    def all_jobs(self):
        groups = [group.all_jobs() for group in self.recordgroup_set.all()]
        jobs = list(chain.from_iterable(groups))
        ordered_jobs = sorted(jobs, key=lambda j: j.id)
        return ordered_jobs
