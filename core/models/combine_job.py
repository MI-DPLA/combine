# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import datetime
import hashlib
import json
import logging
import os
import re
import uuid
import zipfile

from core.models.configurations import ValidationScenario, DPLABulkDataDownload, OAIEndpoint, Transformation
from core.models.organization import Organization
from core.models.record_group import RecordGroup
from core.models.livy_spark import LivySession, LivyClient

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.utils.datastructures import MultiValueDict

from core.xml2kvp import XML2kvp
from core import tasks, models as core_models
from core.models.elasticsearch import ESIndex
from core.models.job import Job, JobValidation, JobInput, Record,\
    IndexMappingFailure, JobTrack

LOGGER = logging.getLogger(__name__)


class CombineJob(object):

    '''
    Class to aggregate methods useful for managing and inspecting jobs.

    Additionally, some methods and workflows for loading a job, inspecting job.job_type, and loading as appropriate
    Combine job.

    Note: There is overlap with the core.models.Job class, but this not being a Django model, allows for a bit
    more flexibility with __init__.
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            parse_job_output=True
        ):

        self.user = user
        self.livy_session = LivySession.get_active_session()
        self.job_id = job_id

        # setup ESIndex instance
        self.esi = ESIndex('j%s' % self.job_id)

        # if job_id provided, attempt to retrieve and parse output
        if self.job_id:

            # retrieve job
            self.get_job(self.job_id)


    def __repr__(self):
        return '<Combine Job: #%s, %s, status %s>' % (self.job.id, self.job.job_type, self.job.status)


    def default_job_name(self):

        '''
        Method to provide default job name based on class type and date

        Args:
            None

        Returns:
            (str): formatted, default job name
        '''

        return '%s @ %s' % (type(self).__name__, datetime.datetime.now().strftime('%b. %d, %Y, %-I:%M:%S %p'))


    @staticmethod
    def init_combine_job(
            user=None,
            record_group=None,
            job_type_class=None,
            job_params={},
            job_details={},
            **kwargs
        ):

        '''
        Static method to initiate a CombineJob

        Args:
            user (django.auth.User): Instance of User
            record_group (core.models.RecordGroup): Record Group for Job to be run in
            job_type_class (CombineJob subclass): Type of Job to run
            job_params (dict, QueryDict): parameters for Job
                - accepts dictionary or Django QueryDict
                - is converted to Django MultiValueDict
            job_details (dict): optional, pre-loaded job_details dict

        Returns:
            - inititates core.models.Job instance
            - initiates job_details
                - parses *shared* parameters across all Job types
            - passes unsaved job instance and initiated job_details dictionary to job_type_class

        '''

        # prepare job_details
        job_details = {}

        # user
        if user != None:
            job_details['user_id'] = user.id

        # convert python dictionary or Django request object to Django MultiValueDict
        job_params = MultiValueDict(job_params)

        # init job_details by parsing job params shared across job types
        job_details = CombineJob._parse_shared_job_params(job_details, job_params, kwargs)

        # capture and mix in job type specific params
        job_details = job_type_class.parse_job_type_params(job_details, job_params, kwargs)

        # init job_type_class with record group and parsed job_details dict
        cjob = job_type_class(
            user=user,
            record_group=record_group,
            job_details=job_details)

        # return
        return cjob


    @staticmethod
    def _parse_shared_job_params(job_details, job_params, kwargs):

        '''
        Method to parse job parameters shared across all Job types

        Args:
            job_details (dict): dictionary to add parsed parameters to
            job_params (django.utils.datastructures.MultiValueDict): parameters provided for job
        '''

        # parse job name
        job_details['job_name'] = job_params.get('job_name')
        if job_details['job_name'] == '':
            job_details['job_name'] = None

        # get job note
        job_details['job_note'] = job_params.get('job_note')
        if job_details['job_note'] == '':
            job_details['job_note'] = None

        # get field mapper configurations
        job_details['field_mapper'] = job_params.get('field_mapper', None)
        if job_details['field_mapper'] != None and job_details['field_mapper'] != 'default':
            job_details['field_mapper'] = int(job_details['field_mapper'])
        fm_config_json = job_params.get('fm_config_json', None)
        if fm_config_json != None:
            job_details['field_mapper_config'] = json.loads(fm_config_json)
        else:
            job_details['field_mapper_config'] = json.loads(XML2kvp().config_json)

        # finish input filters
        job_details['input_filters'] = CombineJob._parse_input_filters(job_params)

        # get requested validation scenarios and convert to int
        job_details['validation_scenarios'] = [int(vs_id) for vs_id in job_params.getlist('validation_scenario', [])]

        # handle requested record_id transform
        job_details['rits'] = job_params.get('rits', None)
        if job_details['rits'] == '':
            job_details['rits'] = None
        if job_details['rits'] != None:
            job_details['rits'] = int(job_details['rits'])

        # handle DPLA bulk data compare
        job_details['dbdm'] = {
            'dbdd':None,
            'dbdd_s3_key':None,
            'matches':None,
            'misses':None
        }
        job_details['dbdm']['dbdd'] = job_params.get('dbdd', None)
        if job_details['dbdm']['dbdd'] == '':
            job_details['dbdm']['dbdd'] = None
        if job_details['dbdm']['dbdd'] != None:
            # set as int
            job_details['dbdm']['dbdd'] = int(job_details['dbdm']['dbdd'])
            # get dbdd instance
            dbdd = DPLABulkDataDownload.objects.get(pk=job_details['dbdm']['dbdd'])
            # set s3_key
            job_details['dbdm']['dbdd_s3_key'] = dbdd.s3_key

        # debug
        LOGGER.debug(job_details)

        # return
        return job_details


    @staticmethod
    def _parse_input_filters(job_params):

        '''
        Method to handle parsing input filters, including Job specific filters
        TODO: refactor duplicative parsing for global and job specific

        Args:
            job_params (dict): parameters passed

        Returns:
            (dict): dictionary to be set as job_details['input_filters']
        '''

        # capture input filters
        input_filters = {
            'job_specific':{}
        }

        # validity valve
        input_filters['input_validity_valve'] = job_params.get('input_validity_valve', 'all')

        # numerical valve
        input_numerical_valve = job_params.get('input_numerical_valve', None)
        if input_numerical_valve in ('', None):
            input_filters['input_numerical_valve'] = None
        else:
            input_filters['input_numerical_valve'] = int(input_numerical_valve)

        # es query valve
        input_es_query_valve = job_params.get('input_es_query_valve', None)
        if input_es_query_valve in ('', None):
            input_es_query_valve = None
        input_filters['input_es_query_valve'] = input_es_query_valve

        # duplicates valve
        filter_dupe_record_ids = job_params.get('filter_dupe_record_ids', 'true')
        if filter_dupe_record_ids == 'true':
            input_filters['filter_dupe_record_ids'] = True
        else:
            input_filters['filter_dupe_record_ids'] = False

        # check for any Job specific filters, and append
        job_specs = job_params.getlist('job_spec_json')
        for job_spec_json in job_specs:

            # parse JSON and make similar to global input filters structure
            job_spec_form_dict = {f['name'].lstrip('job_spec_'):f['value'] for f in json.loads(job_spec_json)}

            # prepare job_spec
            job_spec_dict = {}

            # validity valve
            job_spec_dict['input_validity_valve'] = job_spec_form_dict.get('input_validity_valve', 'all')

            # numerical valve
            input_numerical_valve = job_spec_form_dict.get('input_numerical_valve', None)
            if input_numerical_valve in ('', None):
                job_spec_dict['input_numerical_valve'] = None
            else:
                job_spec_dict['input_numerical_valve'] = int(input_numerical_valve)

            # es query valve
            input_es_query_valve = job_spec_form_dict.get('input_es_query_valve', None)
            if input_es_query_valve in ('', None):
                input_es_query_valve = None
            job_spec_dict['input_es_query_valve'] = input_es_query_valve

            # duplicates valve
            filter_dupe_record_ids = job_spec_form_dict.get('filter_dupe_record_ids', 'true')
            if filter_dupe_record_ids == 'true':
                job_spec_dict['filter_dupe_record_ids'] = True
            else:
                job_spec_dict['filter_dupe_record_ids'] = False

            # finally, append to job_specific dictionary for input_filters
            input_filters['job_specific'][job_spec_form_dict['input_job_id']] = job_spec_dict

        # finish input filters
        return input_filters


    def write_validation_job_links(self, job_details):

        '''
        Method to write links for all Validation Scenarios run
        '''

        # write validation links
        if len(job_details['validation_scenarios']) > 0:
            for vs_id in job_details['validation_scenarios']:
                val_job = JobValidation(
                    job=self.job,
                    validation_scenario=ValidationScenario.objects.get(pk=int(vs_id))
                )
                val_job.save()


    def write_input_job_links(self, job_details):

        '''
        Method to write links for all input Jobs used
        '''

        # get input_jobs
        input_jobs = [Job.objects.get(pk=int(job_id)) for job_id in job_details['input_job_ids']]

        # save input jobs to JobInput table
        for input_job in input_jobs:
            job_input_link = JobInput(
                job=self.job,
                input_job=input_job
            )
            job_input_link.save()


    @staticmethod
    def get_combine_job(job_id):

        '''
        Method to retrieve job, and load as appropriate Combine Job type.

        Args:
            job_id (int): Job ID in DB

        Returns:
            ([
                core.models.HarvestJob,
                core.models.TransformJob,
                core.models.MergeJob,
                core.models.PublishJob
           ])
        '''

        # get job from db
        try:
            j = Job.objects.get(pk=job_id)
        except ObjectDoesNotExist:
            LOGGER.debug('Job #%s was not found, returning False', job_id)
            return False

        # using job_type, return instance of appropriate job type
        return globals()[j.job_type](job_id=job_id)


    def start_job(self):

        '''
        Starts job, sends to prepare_job() for child classes

        Args:
            None

        Returns:
            None
        '''

        # if active livy session
        if self.livy_session:
            self.prepare_job()

        # no active livy session, creating
        else:
            livy_session_id = LivySession.ensure_active_session_id(None)
            self.livy_session = LivySession.get_active_session()
            self.prepare_job()


    def submit_job_to_livy(self, job_code):

        '''
        Using LivyClient, submit actual job code to Spark.    For the most part, Combine Jobs have the heavy lifting of
        their Spark code in core.models.spark.jobs, but this spark code is enough to fire those.

        Args:
            job_code (str): String of python code to submit to Spark

        Returns:
            None
                - sets attributes to self
        '''

        # if livy session provided
        if self.livy_session != None:

            # submit job
            submit = LivyClient().submit_job(self.livy_session.session_id, job_code)
            response = submit.json()
            headers = submit.headers

            # update job in DB
            self.job.response = json.dumps(response)
            self.job.spark_code = job_code
            self.job.job_id = int(response['id'])
            self.job.status = response['state']
            self.job.url = headers['Location']
            self.job.headers = headers
            self.job.save()

        else:

            LOGGER.debug('active livy session not found')

            # update job in DB
            self.job.spark_code = job_code
            self.job.status = 'failed'
            self.job.save()


    def get_job(self, job_id):

        '''
        Retrieve Job from DB

        Args:
            job_id (int): Job ID

        Returns:
            (core.models.Job)
        '''

        self.job = Job.objects.filter(id=job_id).first()


    def get_record(self, record_id, record_field='record_id'):

        '''
        Convenience method to return single record from job.

        Args:
            record_id (str): string of record ID
            record_field (str): field from Record to filter on, defaults to 'record_id'
        '''

        # query for record
        record_query = Record.objects.filter(job=self.job).filter(**{record_field:record_id})

        # if only one found
        if record_query.count() == 1:
            return record_query.first()

        # else, return all results
        return record_query


    def count_indexed_fields(self):

        '''
        Wrapper for ESIndex.count_indexed_fields
        '''

        # return count
        return self.esi.count_indexed_fields(job_record_count=self.job.record_count)


    def field_analysis(self, field_name):

        '''
        Wrapper for ESIndex.field_analysis
        '''

        # return field analysis
        return self.esi.field_analysis(field_name)


    def get_indexing_failures(self):

        '''
        Retrieve failures for job indexing process

        Args:
            None

        Returns:
            (django.db.models.query.QuerySet): from IndexMappingFailure model
        '''

        # load indexing failures for this job from DB
        index_failures = IndexMappingFailure.objects.filter(job_id=self.job.id)
        return index_failures


    def get_job_output_filename_hash(self):

        '''
        When avro files are saved to disk from Spark, they are given a unique hash for the outputted filenames.
        This method reads the avro files from a Job's output, and extracts this unique hash for use elsewhere.

        Args:
            None

        Returns:
            (str): hash shared by all avro files within a job's output
        '''

        # get list of avro files
        job_output_dir = self.job.job_output.split('file://')[-1]

        try:
            avros = [f for f in os.listdir(job_output_dir) if f.endswith('.avro')]

            if len(avros) > 0:
                job_output_filename_hash = re.match(r'part-[0-9]+-(.+?)\.avro', avros[0]).group(1)
                LOGGER.debug('job output filename hash: %s', job_output_filename_hash)
                return job_output_filename_hash

            if len(avros) == 0:
                LOGGER.debug('no avro files found in job output directory')
                return False
        except:
            LOGGER.debug('could not load job output to determine filename hash')
            return False


    def reindex_bg_task(self, fm_config_json=None):

        '''
        Method to reindex job as bg task

        Args:
            fm_config_json (dict|str): XML2kvp field mapper configurations, JSON or dictionary
                - if None, saved configurations for Job will be used
                - pass JSON to bg task for serialization
        '''

        # handle fm_config
        if not fm_config_json:
            fm_config_json = self.job.get_fm_config_json()
        else:
            if type(fm_config_json) == dict:
                fm_config_json = json.dumps(fm_config_json)

        # initiate Combine BG Task
        combine_task = core_models.CombineBackgroundTask(
            name='Re-Map and Index Job: %s' % self.job.name,
            task_type='job_reindex',
            task_params_json=json.dumps({
                'job_id':self.job.id,
                'fm_config_json':fm_config_json
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.job_reindex.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task


    def new_validations_bg_task(self, validation_scenarios):

        '''
        Method to run new validations for Job

        Args:
            validation_scenarios (list): List of Validation Scenarios ids
        '''

        # initiate Combine BG Task
        combine_task = core_models.CombineBackgroundTask(
            name='New Validations for Job: %s' % self.job.name,
            task_type='job_new_validations',
            task_params_json=json.dumps({
                'job_id':self.job.id,
                'validation_scenarios':validation_scenarios
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.job_new_validations.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task


    def remove_validation_bg_task(self, jv_id):

        '''
        Method to remove validations from Job based on Validation Job id
        '''

        # initiate Combine BG Task
        combine_task = core_models.CombineBackgroundTask(
            name='Remove Validation %s for Job: %s' % (jv_id, self.job.name),
            task_type='job_remove_validation',
            task_params_json=json.dumps({
                'job_id':self.job.id,
                'jv_id':jv_id
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.job_remove_validation.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task


    def publish_bg_task(self, publish_set_id=None, in_published_subsets=[]):

        '''
        Method to remove validations from Job based on Validation Job id
        '''

        # initiate Combine BG Task
        combine_task = core_models.CombineBackgroundTask(
            name='Publish Job: %s' % (self.job.name),
            task_type='job_publish',
            task_params_json=json.dumps({
                'job_id':self.job.id,
                'publish_set_id':publish_set_id,
                'in_published_subsets':in_published_subsets
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.job_publish.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task


    def unpublish_bg_task(self):

        '''
        Method to remove validations from Job based on Validation Job id
        '''

        # initiate Combine BG Task
        combine_task = core_models.CombineBackgroundTask(
            name='Unpublish Job: %s' % (self.job.name),
            task_type='job_unpublish',
            task_params_json=json.dumps({
                'job_id':self.job.id
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.job_unpublish.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task


    def dbdm_bg_task(self, dbdd_id):

        '''
        Method to run DPLA Bulk Data Match as bg task
        '''

        # initiate Combine BG Task
        combine_task = core_models.CombineBackgroundTask(
            name='Run DPLA Bulk Data Match for Job: %s' % (self.job.name),
            task_type='job_dbdm',
            task_params_json=json.dumps({
                'job_id':self.job.id,
                'dbdd_id':dbdd_id
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.job_dbdm.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task


    def rerun(self, rerun_downstream=True, set_gui_status=True):

        '''
        Method to re-run job, and if flagged, all downstream Jobs in lineage
        '''

        # get lineage
        rerun_jobs = self.job.get_downstream_jobs()

        # if not running downstream, select only this job
        if not rerun_downstream:
            rerun_jobs = [self.job]

        # loop through jobs
        for re_job in rerun_jobs:

            LOGGER.debug('re-running job: %s', re_job)

            # optionally, update status for GUI representation
            if set_gui_status:

                re_job.timestamp = datetime.datetime.now()
                re_job.status = 'initializing'
                re_job.record_count = 0
                re_job.finished = False
                re_job.elapsed = 0
                re_job.deleted = True
                re_job.save()

            # drop records
            re_job.remove_records_from_db()

            # drop es index
            re_job.drop_es_index()

            # remove previously run validations
            re_job.remove_validation_jobs()
            re_job.remove_validations_from_db()

            # remove mapping failures
            re_job.remove_mapping_failures_from_db()

            # remove from published subsets precounts
            re_job.remove_from_published_precounts()

            # where Job has Input Job, reset passed records
            parent_input_jobs = JobInput.objects.filter(job_id=re_job.id)
            for job_input in parent_input_jobs:
                job_input.passed_records = None
                job_input.save()

            # where Job is input for another, reset passed_records
            as_input_jobs = JobInput.objects.filter(input_job_id=re_job.id)
            for job_input in as_input_jobs:
                job_input.passed_records = None
                job_input.save()

            # get combine job
            re_cjob = CombineJob.get_combine_job(re_job.id)

            # write Validation links
            re_cjob.write_validation_job_links(re_cjob.job.job_details_dict)

            # remove old JobTrack instance
            JobTrack.objects.filter(job=self.job).delete()

            # re-submit to Livy
            if re_cjob.job.spark_code != None:
                re_cjob.submit_job_to_livy(eval(re_cjob.job.spark_code))
            else:
                LOGGER.debug('Spark code not set for Job, attempt to re-prepare...')
                re_cjob.job.spark_code = re_cjob.prepare_job(return_job_code=True)
                re_cjob.job.save()
                re_cjob.submit_job_to_livy(eval(re_cjob.job.spark_code))

            # set as undeleted
            re_cjob.job.deleted = False
            re_cjob.job.save()


    def clone(self, rerun=True, clone_downstream=True, skip_clones=[]):

        '''
        Method to clone Job

        Args:
            rerun (bool): If True, Job(s) are automatically rerun after cloning
            clone_downstream (bool): If True, downstream Jobs are cloned as well
        '''

        if clone_downstream:
            to_clone = self.job.get_downstream_jobs()
        else:
            to_clone = [self.job]

        # loop through jobs to clone
        clones = {} # dictionary of original:clone
        clones_ids = {} # dictionary of original.id:clone.id
        for job in to_clone:

            # confirm that job is itself not a clone made during lineage from another Job
            if job in skip_clones:
                continue

            # establish clone handle
            clone = CombineJob.get_combine_job(job.id)

            # drop PK
            clone.job.pk = None

            # update name
            clone.job.name = "%s (CLONE)" % job.name

            # save, cloning in ORM and generating new Job ID which is needed for clone.prepare_job()
            clone.job.save()
            LOGGER.debug('Cloned Job #%s --> #%s', job.id, clone.job.id)

            # update spark_code
            clone.job.spark_code = clone.prepare_job(return_job_code=True)
            clone.job.save()

            # save to clones and clones_ids dictionary
            clones[job] = clone.job
            clones_ids[job.id] = clone.job.id

            # recreate JobInput links
            for job_input in job.jobinput_set.all():
                LOGGER.debug('cloning input job link: %s', job_input.input_job)
                job_input.pk = None

                # if input job was parent clone, rewrite input jobs links
                if job_input.input_job in clones.keys():

                    # alter job.job_details
                    update_dict = {}

                    # handle input_job_ids
                    update_dict['input_job_ids'] = [clones[job_input.input_job].id if job_id == job_input.input_job.id else job_id for job_id in clone.job.job_details_dict['input_job_ids']]

                    # handle record input filters
                    update_dict['input_filters'] = clone.job.job_details_dict['input_filters']
                    for job_id in update_dict['input_filters']['job_specific'].keys():

                        if int(job_id) in clones_ids.keys():
                            job_spec_dict = update_dict['input_filters']['job_specific'].pop(job_id)
                            update_dict['input_filters']['job_specific'][str(clones_ids[job_input.input_job.id])] = job_spec_dict

                    # update
                    clone.job.update_job_details(update_dict)

                    # rewrite JobInput.input_job
                    job_input.input_job = clones[job_input.input_job]

                job_input.job = clone.job
                job_input.save()

            # recreate JobValidation links
            for job_validation in job.jobvalidation_set.all():
                LOGGER.debug('cloning validation link: %s', job_validation.validation_scenario.name)
                job_validation.pk = None
                job_validation.job = clone.job
                job_validation.save()

            # rerun clone
            if rerun:
                # reopen and run
                clone = CombineJob.get_combine_job(clone.job.id)
                clone.rerun(rerun_downstream=False)

        # return
        return clones

class HarvestJob(CombineJob):
    """
    Harvest records to Combine.

    This class represents a high-level "Harvest" job type, with more specific harvest types extending this class.
    In saved and associated core.models.Job instance, job_type will be "HarvestJob".

    Note: Unlike downstream jobs, Harvest does not require an input job
    """

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
    ):

        """
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent CombineJob init
                - captures args specific to Harvest jobs
        """

        # perform CombineJob initialization
        super().__init__(user=user, job_id=job_id)

        # if job_id not provided, assumed new Job
        if not job_id:

            # if job name not provided, provide default
            if not job_details['job_name']:
                job_details['job_name'] = self.default_job_name()

            # create Job entry in DB and save
            self.job = Job(
                record_group=record_group,
                job_type=type(self).__name__,  # selects this level of class inheritance hierarchy
                user=user,
                name=job_details['job_name'],
                note=job_details['job_note'],
                spark_code=None,
                job_id=None,
                status='initializing',
                url=None,
                headers=None,
                job_details=json.dumps(job_details)
            )
            self.job.save()


class HarvestOAIJob(HarvestJob):
    """
    Harvest records from OAI-PMH endpoint
    Extends core.models.HarvestJob
    """

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
    ):

        """
        Args:

            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent HarvestJob init
        """

        # perform HarvestJob initialization
        super().__init__(
            user=user,
            job_id=job_id,
            record_group=record_group,
            job_details=job_details)

        # if job_id not provided, assume new Job
        if not job_id:
            # write job details
            self.job.update_job_details(job_details)

            # write validation links
            self.write_validation_job_links(job_details)

    @staticmethod
    def parse_job_type_params(job_details, job_params, kwargs):

        """
        Method to parse job type specific parameters
        """

        # retrieve endpoint params
        oai_params = OAIEndpoint.objects.get(pk=int(job_params.get('oai_endpoint_id'))).__dict__.copy()

        # drop _state
        oai_params.pop('_state')

        # retrieve overrides
        overrides = {override: job_params.get(override) for override in
                     ['verb', 'metadataPrefix', 'scope_type', 'scope_value'] if job_params.get(override) != ''}

        # mix in overrides
        for param, value in overrides.items():
            oai_params[param] = value

        # ensure true value for harvestAllSets
        if oai_params['scope_type'] in ['harvestAllSets', 'harvestAllRecords']:
            oai_params['scope_value'] = 'true'

        # get include <header> toggle
        include_oai_record_header = job_params.get('include_oai_record_header', False)
        if include_oai_record_header == 'true':
            oai_params['include_oai_record_header'] = True
        elif include_oai_record_header == False:
            oai_params['include_oai_record_header'] = False

        # save to job_details
        job_details['oai_params'] = oai_params

        return job_details

    def prepare_job(self, return_job_code=False):

        """
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            return_job_code

        Returns:
            None
                - submits job to Livy
        """

        # prepare job code
        job_code = {
            'code': 'from jobs import HarvestOAISpark\nHarvestOAISpark(spark, job_id="%(job_id)s").spark_function()' %
                    {
                        'job_id': self.job.id
                    }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)

    def get_job_errors(self):

        """
        return harvest job specific errors
        NOTE: Currently, we are not saving errors from OAI harvest, and so, cannot retrieve...
        """

        LOGGER.debug(self.job.get_errors())

        return None


class HarvestStaticXMLJob(HarvestJob):
    """
    Harvest records from static XML files
    Extends core.models.HarvestJob
    """

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
    ):

        """
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent HarvestJob init
        """

        # perform HarvestJob initialization
        super().__init__(
            user=user,
            job_id=job_id,
            record_group=record_group,
            job_details=job_details)

        # if job_id not provided, assume new Job
        if not job_id:
            # write job details
            self.job.update_job_details(job_details)

            # write validation links
            self.write_validation_job_links(job_details)

    @staticmethod
    def parse_job_type_params(job_details, job_params, kwargs):

        """
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        """

        # use location on disk
        # When a location on disk is provided, set payload_dir as the location provided
        if job_params.get('static_filepath') != '':
            job_details['type'] = 'location'
            job_details['payload_dir'] = job_params.get('static_filepath')

        # use upload
        # When a payload is uploaded, create payload_dir and set
        else:
            job_details['type'] = 'upload'

            # get static file payload
            payload_file = kwargs['files']['static_payload']

            # grab content type
            job_details['content_type'] = payload_file.content_type

            # create payload dir
            job_details['payload_dir'] = '%s/static_uploads/%s' % (
                settings.BINARY_STORAGE.split('file://')[-1], str(uuid.uuid4()))
            os.makedirs(job_details['payload_dir'])

            # establish payload filename
            if kwargs['hash_payload_filename']:
                job_details['payload_filename'] = hashlib.md5(payload_file.name.encode('utf-8')).hexdigest()
            else:
                job_details['payload_filename'] = payload_file.name

            # write temporary Django file to disk
            with open(os.path.join(job_details['payload_dir'], job_details['payload_filename']), 'wb') as out_file:
                out_file.write(payload_file.read())
                payload_file.close()

            # handle zip files
            if job_details['content_type'] in ['application/zip', 'application/x-zip-compressed']:
                LOGGER.debug('handling zip file upload')
                zip_filepath = os.path.join(job_details['payload_dir'], job_details['payload_filename'])
                zip_ref = zipfile.ZipFile(zip_filepath, 'r')
                zip_ref.extractall(job_details['payload_dir'])
                zip_ref.close()
                os.remove(zip_filepath)

        # include other information for finding, parsing, and preparing identifiers
        job_details['xpath_document_root'] = job_params.get('xpath_document_root', None)
        job_details['document_element_root'] = job_params.get('document_element_root', None)
        job_details['additional_namespace_decs'] = job_params.get('additional_namespace_decs', None).replace("'", '"')
        job_details['xpath_record_id'] = job_params.get('xpath_record_id', None)

        return job_details

    def prepare_job(self, return_job_code=False):

        """
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            return_job_code

        Returns:
            None
                - submits job to Livy
        """

        # prepare job code
        job_code = {
            'code':'from jobs import HarvestStaticXMLSpark\nHarvestStaticXMLSpark(spark, job_id="%(job_id)s").spark_function()' %
                   {
                       'job_id':self.job.id
                   }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)

    def get_job_errors(self):

        """
        Currently not implemented for HarvestStaticXMLJob
        """

        return None


class HarvestTabularDataJob(HarvestJob):
    """
    Harvest records from tabular data
    Extends core.models.HarvestJob
    """

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
    ):

        """
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent HarvestJob init
        """

        # perform HarvestJob initialization
        super().__init__(
            user=user,
            job_id=job_id,
            record_group=record_group,
            job_details=job_details)

        # if job_id not provided, assume new Job
        if not job_id:
            # write job details
            self.job.update_job_details(job_details)

            # write validation links
            self.write_validation_job_links(job_details)

    @staticmethod
    def parse_job_type_params(job_details, job_params, kwargs):

        """
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        """

        # use location on disk
        if job_params.get('static_filepath') != '':
            job_details['payload_filepath'] = job_params.get('static_filepath')

        # use upload
        else:
            # get static file payload
            payload_file = kwargs['files']['static_payload']

            # grab content type
            job_details['content_type'] = payload_file.content_type

            # create payload dir
            job_details['payload_dir'] = '%s/static_uploads/%s' % (
                settings.BINARY_STORAGE.split('file://')[-1], str(uuid.uuid4()))
            os.makedirs(job_details['payload_dir'])

            # establish payload filename
            if kwargs['hash_payload_filename']:
                job_details['payload_filename'] = hashlib.md5(payload_file.name.encode('utf-8')).hexdigest()
            else:
                job_details['payload_filename'] = payload_file.name

            # filepath
            job_details['payload_filepath'] = os.path.join(job_details['payload_dir'], job_details['payload_filename'])

            # write temporary Django file to disk
            with open(job_details['payload_filepath'], 'wb') as out_file:
                out_file.write(payload_file.read())
                payload_file.close()

        # get fm_config_json
        job_details['fm_harvest_config_json'] = job_params.get('fm_harvest_config_json')

        return job_details

    def prepare_job(self, return_job_code=False):

        """
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            return_job_code

        Returns:
            None
                - submits job to Livy
        """

        # prepare job code
        job_code = {
            'code': 'from jobs import HarvestTabularDataSpark\nHarvestTabularDataSpark(spark, job_id="%(job_id)s").spark_function()' %
                    {
                        'job_id': self.job.id
                    }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)

    def get_job_errors(self):

        """
        Currently not implemented for HarvestStaticXMLJob
        """

        return None


class TransformJob(CombineJob):
    """
    Apply an XSLT transformation to a Job
    """

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
    ):

        """
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - sets multiple attributes for self.job
                - sets in motion the output of spark jobs from core.spark.jobs
        """

        # perform CombineJob initialization
        super().__init__(user=user, job_id=job_id)

        # if job_id not provided, assumed new Job
        if not job_id:

            # if job name not provided, provide default
            if not job_details['job_name']:
                job_details['job_name'] = self.default_job_name()

            # create Job entry in DB and save
            self.job = Job(
                record_group=record_group,
                job_type=type(self).__name__,  # selects this level of class inheritance hierarchy
                user=user,
                name=job_details['job_name'],
                note=job_details['job_note'],
                spark_code=None,
                job_id=None,
                status='initializing',
                url=None,
                headers=None,
                job_details=json.dumps(job_details)
            )
            self.job.save()

            # write job details
            self.job.update_job_details(job_details)

            # write validation links
            self.write_validation_job_links(job_details)

            # write validation links
            self.write_input_job_links(job_details)

    @staticmethod
    def parse_job_type_params(job_details, job_params, kwargs):

        """
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        """

        # retrieve input jobs
        job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id')]

        # retrieve transformation, add details to job details

        # reconstitute json and init job_details
        sel_trans = json.loads(job_params['sel_trans_json'])
        job_details['transformation'] = {
            'scenarios_json': job_params['sel_trans_json'],
            'scenarios': []
        }

        # loop through and add with name and type
        for trans in sel_trans:
            transformation = Transformation.objects.get(pk=int(trans['trans_id']))
            job_details['transformation']['scenarios'].append({
                'name': transformation.name,
                'type': transformation.transformation_type,
                'type_human': transformation.get_transformation_type_display(),
                'id': transformation.id,
                'index': trans['index']
            })

        return job_details

    def prepare_job(self, return_job_code=False):

        """
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            return_job_code

        Returns:
            None
                - submits job to Livy
        """

        # prepare job code
        job_code = {
            'code': 'from jobs import TransformSpark\nTransformSpark(spark, job_id="%(job_id)s").spark_function()' %
                    {
                        'job_id': self.job.id
                    }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)

    def get_job_errors(self):

        """
        Return errors from Job

        Args:
            None

        Returns:
            (django.db.models.query.QuerySet)
        """

        return self.job.get_errors()


class MergeJob(CombineJob):
    """
    Merge multiple jobs into a single job
    """

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
    ):

        """
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - sets multiple attributes for self.job
                - sets in motion the output of spark jobs from core.spark.jobs
        """

        # perform CombineJob initialization
        super().__init__(user=user, job_id=job_id)

        # if job_id not provided, assumed new Job
        if not job_id:

            # if job name not provided, provide default
            if not job_details['job_name']:
                job_details['job_name'] = self.default_job_name()

            # create Job entry in DB and save
            self.job = Job(
                record_group=record_group,
                job_type=type(self).__name__,  # selects this level of class inheritance hierarchy
                user=user,
                name=job_details['job_name'],
                note=job_details['job_note'],
                spark_code=None,
                job_id=None,
                status='initializing',
                url=None,
                headers=None,
                job_details=json.dumps(job_details)
            )
            self.job.save()

            # write job details
            self.job.update_job_details(job_details)

            # write validation links
            self.write_validation_job_links(job_details)

            # write validation links
            self.write_input_job_links(job_details)

    @staticmethod
    def parse_job_type_params(job_details, job_params, kwargs):

        """
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        """

        # retrieve input jobs
        job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id')]

        return job_details

    def prepare_job(self, return_job_code=False):

        """
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            return_job_code

        Returns:
            None
                - submits job to Livy
        """

        # prepare job code
        job_code = {
            'code': 'from jobs import MergeSpark\nMergeSpark(spark, job_id="%(job_id)s").spark_function()' % {
                'job_id': self.job.id
            }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)

    def get_job_errors(self):

        """
        Not current implemented from Merge jobs, as primarily just copying of successful records
        """


class AnalysisJob(CombineJob):
    """
    Analysis job
        - Analysis job are unique in name and some functionality, but closely mirror Merge Jobs in execution
        - Though Analysis jobs are very similar to most typical workflow jobs, they do not naturally
        belong to an Organization and Record Group like others. As such, they dynamically create their own Org and
        Record Group, configured in localsettings.py, that is hidden from most other views.
    """

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
    ):

        """
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - sets multiple attributes for self.job
                - sets in motion the output of spark jobs from core.spark.jobs
        """

        # perform CombineJob initialization
        super().__init__(user=user, job_id=job_id)

        # if job_id not provided, assumed new Job
        if not job_id:

            # if job name not provided, provide default
            if not job_details['job_name']:
                job_details['job_name'] = self.default_job_name()

            # get Record Group for Analysis jobs via AnalysisJob.get_analysis_hierarchy()
            analysis_hierarchy = self.get_analysis_hierarchy()

            # create Job entry in DB and save
            self.job = Job(
                record_group=analysis_hierarchy['record_group'],
                job_type=type(self).__name__,  # selects this level of class inheritance hierarchy
                user=user,
                name=job_details['job_name'],
                note=job_details['job_note'],
                spark_code=None,
                job_id=None,
                status='initializing',
                url=None,
                headers=None,
                job_details=json.dumps(job_details)
            )
            self.job.save()

            # write job details
            self.job.update_job_details(job_details)

            # write validation links
            self.write_validation_job_links(job_details)

            # write validation links
            self.write_input_job_links(job_details)

    @staticmethod
    def get_analysis_hierarchy():

        """
        Method to return organization and record_group for Analysis jobs
            - if do not exist, or name has changed, also create
            - reads from settings.ANALYSIS_JOBS_HIERARCHY for unique names for Organization and Record Group
        """

        # get Organization and Record Group name from settings
        org_name = settings.ANALYSIS_JOBS_HIERARCHY['organization']
        record_group_name = settings.ANALYSIS_JOBS_HIERARCHY['record_group']

        # check of Analysis jobs aggregating Organization exists
        analysis_org_search = Organization.objects.filter(name=org_name)
        if analysis_org_search.count() == 0:
            LOGGER.debug('creating Organization with name %s', org_name)
            analysis_org = Organization(
                name=org_name,
                description='For the explicit use of aggregating Analysis jobs',
                for_analysis=True
            )
            analysis_org.save()

        # if one found, use
        elif analysis_org_search.count() == 1:
            analysis_org = analysis_org_search.first()

        else:
            raise Exception('multiple Organizations found for explicit purpose of aggregating Analysis jobs')

        # check of Analysis jobs aggregating Record Group exists
        analysis_record_group_search = RecordGroup.objects.filter(name=record_group_name)
        if analysis_record_group_search.count() == 0:
            LOGGER.debug('creating RecordGroup with name %s', record_group_name)
            analysis_record_group = RecordGroup(
                organization=analysis_org,
                name=record_group_name,
                description='For the explicit use of aggregating Analysis jobs',
                for_analysis=True
            )
            analysis_record_group.save()

        # if one found, use
        elif analysis_record_group_search.count() == 1:
            analysis_record_group = analysis_record_group_search.first()

        else:
            raise Exception('multiple Record Groups found for explicit purpose of aggregating Analysis jobs')

        # return Org and Record Group
        return {
            'organization': analysis_org,
            'record_group': analysis_record_group
        }

    @staticmethod
    def parse_job_type_params(job_details, job_params, kwargs):

        """
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        """

        # retrieve input job
        job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id')]

        return job_details

    def prepare_job(self, return_job_code=False):

        """
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            return_job_code

        Returns:
            None
                - submits job to Livy
        """

        # prepare job code
        job_code = {
            'code': 'from jobs import MergeSpark\nMergeSpark(spark, job_id="%(job_id)s").spark_function()' % {
                'job_id': self.job.id
            }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)

    def get_job_errors(self):

        """
        Not current implemented from Analyze jobs, as primarily just copying of successful records
        """
