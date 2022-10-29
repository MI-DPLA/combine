# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import logging
import time

# django imports
from django.db import models

# core models imports
from core.models.organization import Organization

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)

class RecordGroup(models.Model):

    '''
    Model to manage Record Groups in Combine.
    Record Groups are members of Organizations, and contain Jobs
    '''

    organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
    name = models.CharField(max_length=128)
    description = models.CharField(max_length=255, null=True, default=None, blank=True)
    timestamp = models.DateTimeField(null=True, auto_now_add=True)
    # publish_set_id = models.CharField(max_length=128, null=True, default=None, blank=True)
    for_analysis = models.BooleanField(default=0)

    def __str__(self):
        return 'Record Group: %s' % self.name

    def get_jobs_lineage(self):

        '''
        Method to generate structured data outlining the lineage of jobs for this Record Group.
        Will use Combine DB ID as node identifiers.

        Args:
            None

        Returns:
            (dict): lineage dictionary of nodes (jobs) and edges (input jobs as edges)
        '''

        # debug
        stime = time.time()

        # create record group lineage dictionary
        lineage_dict = {'edges':[], 'nodes':[]}

        # get all jobs
        record_group_jobs = self.job_set.order_by('-id').all()

        # loop through jobs
        for job in record_group_jobs:
            job_ld = job.get_lineage()
            lineage_dict['edges'].extend(job_ld['edges'])
            lineage_dict['nodes'].extend(job_ld['nodes'])

        # filter for unique
        lineage_dict['nodes'] = list({node['id']:node for node in lineage_dict['nodes']}.values())
        lineage_dict['edges'] = list({edge['id']:edge for edge in lineage_dict['edges']}.values())

        # sort by id
        lineage_dict['nodes'].sort(key=lambda x: x['id'])
        lineage_dict['edges'].sort(key=lambda x: x['id'])

        # return
        LOGGER.debug('lineage calc time elapsed: %s', (time.time()-stime))
        return lineage_dict

    def published_jobs(self):

        # get published jobs for rg
        return self.job_set.filter(published=True)

    def is_published(self):

        '''
        Method to determine if a Job has been published for this RecordGroup

        Args:
            None

        Returns:
            (bool): if a job has been published for this RecordGroup, return True, else False
        '''

        # get jobs for rg
        published = self.published_jobs()

        # return True/False
        if published.count() == 0:
            return False
        return True

    def total_record_count(self):

        '''
        Method to count total records under this RG
        '''

        total_record_count = 0

        # loop through jobs
        for job in self.job_set.all():

            total_record_count += job.record_count

        # return
        return total_record_count

    def all_jobs(self):
        jobs = self.job_set.all()
        ordered_jobs = sorted(jobs, key=lambda j: j.id)
        return ordered_jobs

    @property
    def last_modified(self):
        jobs = self.job_set.all()
        if not jobs:
            return None
        else:
            timestamps = [job.timestamp for job in jobs]
            return max(timestamps)
