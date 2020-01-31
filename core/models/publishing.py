# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import logging

from core.models.elasticsearch import ESIndex
from core.models.job import Job, Record
from core.mongo import mc_handle

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)

class PublishedRecords():

    '''
    Model to manage the aggregation and retrieval of published records.
    '''

    def __init__(self, subset=None):

        '''
        Args:
            subset (str): published subset slug
        '''

        self.record_group = 0

        # get published jobs
        self.published_jobs = Job.objects.filter(published=True)

        # set subset and filter if need be
        self.subset = subset
        if self.subset != None:

            # retrieve published subset document in Mongo
            self.ps_doc = mc_handle.combine.misc.find_one({'type':'published_subset', 'name':self.subset})

            if self.ps_doc == None:
                LOGGER.warning('published subset could not be found for name: %s', self.subset)

            else:

                # build list of publish_set_ids to limit to
                publish_set_ids = self.ps_doc.get('publish_set_ids', [])

                # filter jobs to sets
                self.published_jobs = self.published_jobs.filter(publish_set_id__in=publish_set_ids)

                # if including non-set records, merge a Job queryset with all published Jobs, sans publish_set_id
                if self.ps_doc.get('include_non_set_records', False):
                    non_set_published_jobs = Job.objects.filter(published=True, publish_set_id='')
                    self.published_jobs = self.published_jobs | non_set_published_jobs

                # include any published Jobs from hierarchy, if present
                hierarchy = self.ps_doc.get('hierarchy', [])
                if len(hierarchy) > 0:

                    # collect job, record group, and org ids
                    org_ids = [int(_.split('|')[1]) for _ in hierarchy if _.startswith('org')]
                    record_group_ids = [int(_.split('|')[1]) for _ in hierarchy if _.startswith('record_group')]
                    job_ids = [int(_.split('|')[1]) for _ in hierarchy if _.startswith('job')]

                    # OR query to get set of Jobs that match
                    hierarchy_jobs = Job.objects.filter(published=True, pk__in=job_ids) |\
                        Job.objects.filter(published=True, record_group__in=record_group_ids) |\
                        Job.objects.filter(published=True, record_group__organization__in=org_ids)

                    # merge with published jobs
                    self.published_jobs = self.published_jobs | hierarchy_jobs

        # set Mongo document count id
        self.mongo_count_id = 'published_field_counts'
        if self.subset != None:
            self.mongo_count_id = '%s_%s' % (self.mongo_count_id, self.subset)

        # get set IDs from record group of published jobs
        sets = {}
        for job in self.published_jobs:

            if job.publish_set_id:

                # if set not seen, add as list
                if job.publish_set_id not in sets.keys():
                    sets[job.publish_set_id] = []

                # add publish job
                sets[job.publish_set_id].append(job)
        self.sets = sets

        # establish ElasticSearchIndex (esi) instance
        self.esi = ESIndex(['j%s' % job.id for job in self.published_jobs])


    @property
    def records(self):

        '''
        Property to return QuerySet of all published records
        '''

        return Record.objects.filter(job_id__in=[job.id for job in self.published_jobs])


    def get_record(self, record_id):

        '''
        Return single, published record by record.record_id

        Args:
            record_id (str): Record's record_id

        Returns:
            (core.model.Record): single Record instance
        '''

        record_query = self.records.filter(record_id=record_id)

        # if one, return
        if record_query.count() == 1:
            return record_query.first()

        LOGGER.debug('multiple records found for id %s - this is not allowed for published records', id)
        return False


    def count_indexed_fields(self, force_recount=False):

        '''
        Wrapper for ESIndex.count_indexed_fields
            - stores results in Mongo to avoid re-calculating every time
                - stored as misc/published_field_counts
            - checks Mongo for stored metrics, if not found, calculates and stores
            - when Jobs are published, this Mongo entry is removed forcing a re-calc

        Args:
            force_recount (boolean): force recount and update to stored doc in Mongo
        '''

        # check for stored field counts
        published_field_counts = mc_handle.combine.misc.find_one(self.mongo_count_id)

        # if present, return and use
        if published_field_counts and not force_recount:
            LOGGER.debug('saved published field counts found, using')
            return published_field_counts

        # else, calculate, store, and return
        LOGGER.debug('calculating published field counts, saving, and returning')

        # calc
        published_field_counts = self.esi.count_indexed_fields()

        # if published_field_counts
        if published_field_counts:

            # add id and replace (upsert if necessary)
            published_field_counts['_id'] = self.mongo_count_id
            _ = mc_handle.combine.misc.replace_one(
                {'_id':self.mongo_count_id},
                published_field_counts,
                upsert=True)

        # return
        return published_field_counts


    @staticmethod
    def get_publish_set_ids():

        '''
        Static method to return unique, not Null publish set ids

        Args:
            None

        Returns:
            (list): list of publish set ids
        '''

        publish_set_ids = Job.objects.exclude(publish_set_id=None).values('publish_set_id').distinct()
        return publish_set_ids


    @staticmethod
    def get_subsets(includes_publish_set_id=None):

        '''
        Static method to return published subsets
        '''

        if includes_publish_set_id:
            LOGGER.debug('filtering Published Subsets to those that include publish_set_id: %s', includes_publish_set_id)
            return list(mc_handle.combine.misc.find({'type':'published_subset', 'publish_set_ids':includes_publish_set_id}))

        return list(mc_handle.combine.misc.find({'type':'published_subset'}))


    def update_subset(self, update_dict):

        '''
        Method to update currently associated subset (self.subset and self.ps_doc) with new parameters
        '''

        # update mongo doc
        result = mc_handle.combine.misc.update_one(
            {'_id':self.ps_doc['_id']},
            {'$set':update_dict}, upsert=True)
        LOGGER.debug(result.raw_result)


    def add_publish_set_id_to_subset(self, publish_set_id):

        '''
        Method to add publish_set_id to Published Subset
        '''

        publish_set_ids = self.ps_doc['publish_set_ids']
        if publish_set_id != '' and publish_set_id not in publish_set_ids:
            update_dict = {
                'publish_set_ids':publish_set_ids + [publish_set_id]
            }

        elif publish_set_id == '':
            LOGGER.debug('adding ALL non-set Records to Published Subset')
            update_dict = {
                'include_non_set_records':True
            }

        # update mongo doc
        result = mc_handle.combine.misc.update_one(
            {'_id':self.ps_doc['_id']},
            {'$set':update_dict}, upsert=True)
        LOGGER.debug(result.raw_result)

        # remove pre-counts
        self.remove_subset_precounts()


    def remove_subset_precounts(self):

        '''
        Method to remove pre-counts for Published Subset
        '''

        mc_handle.combine.misc.delete_one({'_id':'published_field_counts_%s' % self.subset})
