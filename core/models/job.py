# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
from collections import OrderedDict
from lxml import etree
import binascii
import datetime
import difflib
import hashlib
import inspect
import io
import json
import logging
import os
import re
import requests
import shutil
import time
import urllib.parse
import uuid
import zipfile

# django imports
from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.db import models, transaction
from django.utils.datastructures import MultiValueDict
from django.core.urlresolvers import reverse

from core.xml2kvp import XML2kvp
from core import tasks, models as core_models
from core.es import es_handle
from core.mongo import mongoengine, mc_handle
from core.models.configurations import OAIEndpoint, Transformation, ValidationScenario, DPLABulkDataDownload
from core.models.elasticsearch import ESIndex
from core.models.livy_spark import LivySession, LivyClient, SparkAppAPIClient
from core.models.organization import Organization
from core.models.record_group import RecordGroup

from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)

# sxsdiff
from sxsdiff import DiffCalculator
from sxsdiff.generators.github import GitHubStyledGenerator

# toposort
from toposort import toposort_flatten




class Job(models.Model):

    '''
    Model to manage jobs in Combine.
    Jobs are members of Record Groups, and contain Records.

    A Job can be considered a "stage" of records in Combine as they move through Harvest, Transformations, Merges, and
    eventually Publishing.
    '''

    record_group = models.ForeignKey(RecordGroup, on_delete=models.CASCADE)
    job_type = models.CharField(max_length=128, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    name = models.CharField(max_length=128, null=True)
    spark_code = models.TextField(null=True, default=None)
    job_id = models.IntegerField(null=True, default=None)
    status = models.CharField(max_length=30, null=True)
    finished = models.BooleanField(default=0)
    url = models.CharField(max_length=255, null=True)
    headers = models.CharField(max_length=255, null=True)
    response = models.TextField(null=True, default=None)
    job_output = models.TextField(null=True, default=None)
    record_count = models.IntegerField(null=True, default=0)
    published = models.BooleanField(default=0)
    publish_set_id = models.CharField(max_length=255, null=True, default=None, blank=True)
    job_details = models.TextField(null=True, default=None)
    timestamp = models.DateTimeField(null=True, auto_now_add=True)
    note = models.TextField(null=True, default=None)
    elapsed = models.IntegerField(null=True, default=0)
    deleted = models.BooleanField(default=0)

    def __str__(self):
        return '%s, Job #%s' % (self.name, self.id)

    def job_type_family(self):

        '''
        Method to return high-level job type from Harvest, Transform, Merge, Publish

        Args:
            None

        Returns:
            (str, ['HarvestJob', 'TransformJob', 'MergeJob', 'PublishJob']): String of high-level job type
        '''

        # get class hierarchy of job
        class_tree = inspect.getmro(globals()[self.job_type])

        # handle Harvest determination
        if HarvestJob in class_tree:
            return class_tree[-3].__name__

        # else, return job_type untouched
        return self.job_type

    def update_status(self):

        '''
        Method to udpate job information based on status from Livy.
        Jobs marked as deleted are not updated.

        Args:
            None

        Returns:
            None
                - updates status, record_count, elapsed (soon)
        '''

        # if not deleted
        if not self.deleted:

            # if job in various status, and not finished, ping livy
            if self.status in ['initializing', 'waiting', 'pending', 'starting', 'running', 'available', 'gone']\
            and self.url != None\
            and not self.finished:

                LOGGER.debug('pinging Livy for Job status: %s', self)
                self.refresh_from_livy(save=False)

            # udpate record count if not already calculated
            if self.record_count == 0:

                # if finished, count
                if self.finished:

                    # update record count
                    self.update_record_count(save=False)

            # update elapsed
            self.elapsed = self.calc_elapsed()

            # finally, save
            self.save()

    def calc_elapsed(self):

        '''
        Method to calculate how long a job has been running/ran.

        Args:
            None

        Returns:
            (int): elapsed time in seconds
        '''

        # if job_track exists, calc elapsed
        if self.jobtrack_set.count() > 0:

            # get start time
            job_track = self.jobtrack_set.first()

            # if not finished, determined elapsed until now
            if not self.finished:
                return (datetime.datetime.now() - job_track.start_timestamp.replace(tzinfo=None)).seconds

            # else, if finished, calc time between job_track start and finish
            try:
                return (job_track.finish_timestamp - job_track.start_timestamp).seconds
            except Exception as err:
                LOGGER.debug('error with calculating elapsed: %s', str(err))
                return 0

        # else, return zero
        else:
            return 0

    def elapsed_as_string(self):

        '''
        Method to return elapsed as string for Django templates
        '''

        minutes, seconds = divmod(self.elapsed, 60)
        hours, minutes = divmod(minutes, 60)
        return "%d:%02d:%02d" % (hours, minutes, seconds)

    def calc_records_per_second(self):

        '''
        Method to calculcate records per second, if total known.
        If running, use current elapsed, if finished, use total elapsed.

        Args:
            None

        Returns:
            (float): records per second, rounded to one dec.
        '''

        try:
            if self.record_count > 0:

                if not self.finished:
                    elapsed = self.calc_elapsed()
                else:
                    elapsed = self.elapsed
                return round((float(self.record_count) / float(elapsed)), 1)

            return None
        except:
            return None

    def refresh_from_livy(self, save=True):

        '''
        Update job status from Livy.

        Args:
            None

        Returns:
            None
                - sets attriutes of self
        '''

        # query Livy for statement status
        livy_response = LivyClient().job_status(self.url)

        # if status_code 400 or 404, set as gone
        if livy_response.status_code in [400, 404]:

            self.status = 'available'
            self.finished = True

            # update
            if save:
                self.save()

        elif livy_response.status_code == 200:

            # set response
            self.response = livy_response.content

            # parse response
            response = livy_response.json()
            headers = livy_response.headers

            # update Livy information
            self.status = response['state']

            # if state is available, assume finished
            if self.status == 'available':
                self.finished = True

            # update
            if save:
                self.save()

        else:

            LOGGER.debug('error retrieving information about Livy job/statement')
            LOGGER.debug(livy_response.status_code)
            LOGGER.debug(livy_response.json())

    def get_spark_jobs(self):

        '''
        Attempt to retrieve associated jobs from Spark Application API
        '''

        # get active livy session, and refresh, which contains spark_app_id as appId
        livy_session = LivySession.get_active_session()

        if livy_session and type(livy_session) == LivySession:

            # if appId not set, attempt to retrieve
            if not livy_session.appId:
                livy_session.refresh_from_livy()

            # get list of Jobs, filter by jobGroup for this Combine Job
            try:
                filtered_jobs = SparkAppAPIClient.get_spark_jobs_by_jobGroup(livy_session, livy_session.appId, self.id)
            except:
                LOGGER.warning('trouble retrieving Jobs from Spark App API')
                filtered_jobs = []
            if len(filtered_jobs) > 0:
                return filtered_jobs
            return None

        return False

    def has_spark_failures(self):

        '''
        Look for failure in spark jobs associated with this Combine Job
        '''

        # get spark jobs
        spark_jobs = self.get_spark_jobs()

        if spark_jobs:
            failed = [job for job in spark_jobs if job['status'] == 'FAILED']
            if len(failed) > 0:
                return failed
            return False

        return None

    def get_records(self, success=True):

        '''
        Retrieve records associated with this job from Mongo

        Args:
            success (boolean): filter records on success column by this arg
                - passing None will return unfiltered (success and failures)

        Returns:
            (django.db.models.query.QuerySet)
        '''

        if success == None:
            records = Record.objects(job_id=self.id)

        else:
            records = Record.objects(job_id=self.id, success=success)

        # return
        return records

    def get_errors(self):

        '''
        Retrieve records associated with this job if the error field is not blank.

        Args:
            None

        Returns:
            (django.db.models.query.QuerySet)
        '''

        errors = Record.objects(job_id=self.id, success=False)

        # return
        return errors

    def update_record_count(self, save=True):

        '''
        Get record count from Mongo from Record table, filtering by job_id

        Args:
            save (bool): Save instance on calculation

        Returns:
            None
        '''

        # det detailed record count
        drc = self.get_detailed_job_record_count(force_recount=True)

        # update Job record count
        self.record_count = drc['records'] + drc['errors']

        # if job has single input ID, and that is still None, set to record count
        if self.jobinput_set.count() == 1:
            job_input = self.jobinput_set.first()
            if job_input.passed_records == None:
                job_input.passed_records = self.record_count
                job_input.save()

        # if save, save
        if save:
            self.save()

    def get_total_input_job_record_count(self):

        '''
        Calc record count sum from all input jobs, factoring in whether record input validity was all, valid, or invalid

        Args:
            None

        Returns:
            (int): count of records
        '''

        if self.jobinput_set.count() > 0:

            # init dict
            input_jobs_dict = {
                'total_input_record_count':0
            }

            # loop through input jobs
            for input_job in self.jobinput_set.all():

                # bump count
                if input_job.passed_records != None:
                    input_jobs_dict['total_input_record_count'] += input_job.passed_records

            # return
            return input_jobs_dict

        return None

    def get_detailed_job_record_count(self, force_recount=False):

        '''
        Return details of record counts for input jobs, successes, and errors

        Args:
            force_recount (bool): If True, force recount from db

        Returns:
            (dict): Dictionary of record counts
        '''

        # debug
        stime = time.time()

        if 'detailed_record_count' in self.job_details_dict.keys() and not force_recount:
            LOGGER.debug('total detailed record count retrieve elapsed: %s', (time.time()-stime))
            return self.job_details_dict['detailed_record_count']

        r_count_dict = {}

        # get counts
        r_count_dict['records'] = self.get_records().count()
        r_count_dict['errors'] = self.get_errors().count()

        # include input jobs
        r_count_dict['input_jobs'] = self.get_total_input_job_record_count()

        # calc success percentages, based on records ratio to job record count (which includes both success and error)
        if r_count_dict['records'] != 0:
            r_count_dict['success_percentage'] = round((float(r_count_dict['records']) / float(r_count_dict['records'])), 4)
        else:
            r_count_dict['success_percentage'] = 0.0

        # saving to job_details
        self.update_job_details({'detailed_record_count':r_count_dict})

        # return
        LOGGER.debug('total detailed record count calc elapsed: %s', (time.time()-stime))
        return r_count_dict

    def job_output_as_filesystem(self):

        '''
        Not entirely removing the possibility of storing jobs on HDFS, this method returns self.job_output as
        filesystem location and strips any righthand slash

        Args:
            None

        Returns:
            (str): location of job output
        '''

        return self.job_output.replace('file://', '').rstrip('/')

    def get_output_files(self):

        '''
        Convenience method to return full path of all avro files in job output

        Args:
            None

        Returns:
            (list): list of strings of avro files locations on disk
        '''

        output_dir = self.job_output_as_filesystem()
        return [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.avro')]

    def index_results_save_path(self):

        '''
        Return index save path

        Args:
            None

        Returns:
            (str): location of saved indexing results
        '''

        # index results save path
        return '%s/organizations/%s/record_group/%s/jobs/indexing/%s' % (
            settings.BINARY_STORAGE.rstrip('/'), self.record_group.organization.id, self.record_group.id, self.id)

    def get_lineage(self):

        '''
        Method to retrieve lineage of self.
            - creates nodes and edges dictionary of all "upstream" Jobs
        '''

        lineage_dict = {'nodes':[], 'edges':[]}

        # get validation results for self
        validation_results = self.validation_results()

        # prepare node dictionary
        node_dict = {
            'id':self.id,
            'name':self.name,
            'record_group_id':None,
            'org_id':None,
            'job_type':self.job_type,
            'job_status':self.status,
            'is_valid':validation_results['verdict'],
            'deleted':self.deleted
        }

        # if not Analysis job, add org and record group
        if self.job_type != 'AnalysisJob':
            node_dict['record_group_id'] = self.record_group.id
            node_dict['org_id'] = self.record_group.organization.id

        # add self to lineage dictionary
        lineage_dict['nodes'].append(node_dict)

        # update lineage dictionary recursively
        self._get_parent_jobs(self, lineage_dict)

        # return
        return lineage_dict

    def _get_parent_jobs(self, job, lineage_dict):

        '''
        Method to recursively find parent jobs and add to lineage dictionary

        Args:
            job (core.models.Job): job to derive all upstream jobs from
            lineage_dict (dict): lineage dictionary

        Returns:
            (dict): lineage dictionary, updated with upstream parents
        '''

        # get parent job(s)
        parent_job_links = job.jobinput_set.all() # reverse many to one through JobInput model

        # if parent jobs found
        if parent_job_links.count() > 0:

            # loop through
            for link in parent_job_links:

                # get parent job proper
                parent_job = link.input_job

                # add as node, if not already added to nodes list
                if parent_job.id not in [node['id'] for node in lineage_dict['nodes']]:

                    # get validation results and add to node
                    validation_results = parent_job.validation_results()

                    # prepare node dictionary
                    node_dict = {
                        'id':parent_job.id,
                        'name':parent_job.name,
                        'record_group_id':None,
                        'org_id':None,
                        'job_type':parent_job.job_type,
                        'job_status':self.status,
                        'is_valid':validation_results['verdict'],
                        'deleted':parent_job.deleted
                        }

                    # if not Analysis job, add org and record group
                    if parent_job.job_type != 'AnalysisJob':
                        node_dict['record_group_id'] = parent_job.record_group.id
                        node_dict['org_id'] = parent_job.record_group.organization.id

                    # if from another Record Group, note in node
                    if parent_job.record_group != self.record_group:
                        node_dict['external_record_group'] = True

                    # append to nodes
                    lineage_dict['nodes'].append(node_dict)

                # set edge directionality
                from_node = parent_job.id
                to_node = job.id

                # add edge
                edge_id = '%s_to_%s' % (from_node, to_node)
                if edge_id not in [edge['id'] for edge in lineage_dict['edges']]:

                    if 'input_filters' in self.job_details_dict:

                        # check for job specific filters to use for edge
                        if 'job_specific' in self.job_details_dict['input_filters'].keys() and str(from_node) in self.job_details_dict['input_filters']['job_specific'].keys():

                            LOGGER.debug('found job type specifics for input job: %s, applying to edge', from_node)

                            # get job_spec_dict
                            job_spec_dict = self.job_details_dict['input_filters']['job_specific'][str(from_node)]

                            # prepare edge dictionary
                            try:
                                edge_dict = {
                                    'id':edge_id,
                                    'from':from_node,
                                    'to':to_node,
                                    'input_validity_valve':job_spec_dict['input_validity_valve'],
                                    'input_numerical_valve':job_spec_dict['input_numerical_valve'],
                                    'filter_dupe_record_ids':job_spec_dict['filter_dupe_record_ids'],
                                    'total_records_passed':link.passed_records
                                }
                                # add es query flag
                                if job_spec_dict['input_es_query_valve']:
                                    edge_dict['input_es_query_valve'] = True
                                else:
                                    edge_dict['input_es_query_valve'] = False

                            except:
                                edge_dict = {
                                    'id':edge_id,
                                    'from':from_node,
                                    'to':to_node,
                                    'input_validity_valve':'unknown',
                                    'input_numerical_valve':None,
                                    'filter_dupe_record_ids':False,
                                    'input_es_query_valve':False,
                                    'total_records_passed':link.passed_records
                                }

                        # else, use global input job filters
                        else:

                            # prepare edge dictionary
                            try:
                                edge_dict = {
                                    'id':edge_id,
                                    'from':from_node,
                                    'to':to_node,
                                    'input_validity_valve':self.job_details_dict['input_filters']['input_validity_valve'],
                                    'input_numerical_valve':self.job_details_dict['input_filters']['input_numerical_valve'],
                                    'filter_dupe_record_ids':self.job_details_dict['input_filters']['filter_dupe_record_ids'],
                                    'total_records_passed':link.passed_records
                                }
                                # add es query flag
                                if self.job_details_dict['input_filters']['input_es_query_valve']:
                                    edge_dict['input_es_query_valve'] = True
                                else:
                                    edge_dict['input_es_query_valve'] = False

                            except:
                                edge_dict = {
                                    'id':edge_id,
                                    'from':from_node,
                                    'to':to_node,
                                    'input_validity_valve':'unknown',
                                    'input_numerical_valve':None,
                                    'filter_dupe_record_ids':False,
                                    'input_es_query_valve':False,
                                    'total_records_passed':link.passed_records
                                }

                    else:

                        LOGGER.debug('no input filters were found for job: %s', self.id)
                        edge_dict = {
                            'id':edge_id,
                            'from':from_node,
                            'to':to_node,
                            'input_validity_valve':'unknown',
                            'input_numerical_valve':None,
                            'filter_dupe_record_ids':False,
                            'input_es_query_valve':False,
                            'total_records_passed':link.passed_records
                        }

                    lineage_dict['edges'].append(edge_dict)

                # recurse
                self._get_parent_jobs(parent_job, lineage_dict)

    @staticmethod
    def get_all_jobs_lineage(
            organization=None,
            record_group=None,
            jobs_query_set=None,
            exclude_analysis_jobs=True
        ):

        '''
        Static method to get lineage for all Jobs
            - used for all jobs and input select views

        Args:
            organization(core.models.Organization): Organization to filter results by
            record_group(core.models.RecordGroup): RecordGroup to filter results by
            jobs_query_set(django.db.models.query.QuerySet): optional pre-constructed Job model QuerySet

        Returns:
            (dict): lineage dictionary of Jobs
        '''

        # if Job QuerySet provided, use
        if jobs_query_set:
            jobs = jobs_query_set

        # else, construct Job QuerySet
        else:
            # get all jobs
            jobs = Job.objects.all()

            # if Org provided, filter
            if organization:
                jobs = jobs.filter(record_group__organization=organization)

            # if RecordGroup provided, filter
            if record_group:
                jobs = jobs.filter(record_group=record_group)

            # if excluding analysis jobs
            if exclude_analysis_jobs:
                jobs = jobs.exclude(job_type='AnalysisJob')

        # create record group lineage dictionary
        lineage_dict = {'edges':[], 'nodes':[]}

        # loop through jobs
        for job in jobs:
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
        return lineage_dict

    def validation_results(self, force_recount=False):

        '''
        Method to return boolean whether job passes all/any validation tests run

        Args:
            None

        Returns:
            (dict):
                verdict (boolean): True if all tests passed, or no tests performed, False is any fail
                failure_count (int): Total number of distinct Records with 1+ validation failures
                validation_scenarios (list): QuerySet of associated JobValidation
        '''

        # return dict
        results = {
            'verdict':True,
            'passed_count':self.record_count,
            'failure_count':0,
            'validation_scenarios':[]
        }

        # if not finished, don't bother counting at all
        if not self.finished:
            return results

        # no validation tests run, return True
        if self.jobvalidation_set.count() == 0:
            return results

        # check if already calculated, and use
        if 'validation_results' in self.job_details_dict.keys() and not force_recount:
            return self.job_details_dict['validation_results']

        # validation tests run, loop through
        # determine total number of distinct Records with 0+ validation failures
        results['failure_count'] = Record.objects(job_id=self.id, valid=False).count()

        # if failures found
        if results['failure_count'] > 0:
            # set result to False
            results['verdict'] = False
            # subtract failures from passed
            results['passed_count'] -= results['failure_count']

        # add validation scenario ids
        for job_validation in self.jobvalidation_set.all():
            results['validation_scenarios'].append(job_validation.validation_scenario_id)

        # save to job_details
        LOGGER.debug("saving validation results for %s to job_details", self)
        self.update_job_details({'validation_results':results})

        # return
        return results

    def get_dpla_bulk_data_matches(self):

        '''
        Method to update counts and return overview of results of DPLA Bulk Data matching
        '''

        if self.finished:

            # check job_details for dbdm key in job_details, indicating bulk data check
            if 'dbdm' in self.job_details_dict.keys() and 'dbdd' in self.job_details_dict['dbdm'].keys() and self.job_details_dict['dbdm']['dbdd'] != None:

                # get dbdm
                dbdm = self.job_details_dict.get('dbdm', False)

                try:

                    # retrieve DBDD
                    dbdd = DPLABulkDataDownload.objects.get(pk=dbdm['dbdd'])

                    # get misses and matches, counting if not yet done
                    if dbdm['matches'] == None and dbdm['misses'] == None:

                        # matches
                        dbdm['matches'] = self.get_records().filter(dbdm=True).count()

                        # misses
                        dbdm['misses'] = self.get_records().filter(dbdm=False).count()

                        # update job details
                        self.update_job_details({'dbdm':dbdm})

                    # return dict
                    return {
                        'dbdd':dbdd,
                        'matches':dbdm['matches'],
                        'misses': dbdm['misses']
                    }

                except Exception as err:
                    LOGGER.debug('Job claims to have dbdd, but encountered error while retrieving: %s', str(err))
                    return {}

            else:
                LOGGER.debug('DPLA Bulk comparison not run, or no matches found.')
                return False

        else:
            return False

    def drop_es_index(self, clear_mapped_field_analysis=True):

        '''
        Method to drop associated ES index
        '''

        # remove ES index if exists
        try:
            if es_handle.indices.exists('j%s' % self.id):
                LOGGER.debug('removing ES index: j%s', self.id)
                es_handle.indices.delete('j%s' % self.id)
                LOGGER.debug('ES index remove')
        except:
            LOGGER.debug('could not remove ES index: j%s', self.id)


        # remove saved mapped_field_analysis in job_details, if exists
        if clear_mapped_field_analysis:
            job_details = self.job_details_dict
            if 'mapped_field_analysis' in job_details.keys():
                job_details.pop('mapped_field_analysis')
                self.job_details = json.dumps(job_details)
                with transaction.atomic():
                    self.save()

    def get_fm_config_json(self, as_dict=False):

        '''
        Method to return used Field Mapper Configuration as JSON
        '''

        try:

            # get field mapper config as dictionary
            fm_dict = self.job_details_dict['field_mapper_config']

            # return as JSON
            if as_dict:
                return fm_dict
            return json.dumps(fm_dict)

        except Exception as err:
            LOGGER.debug('error retrieving fm_config_json: %s', str(err))
            return False

    @property
    def job_details_dict(self):

        '''
        Property to return job_details json as dictionary
        '''

        if self.job_details:
            return json.loads(self.job_details)
        return {}

    def update_job_details(self, update_dict, save=True):

        '''
        Method to update job_details by providing a dictionary to update with, optiontally saving

        Args:
            update_dict (dict): dictionary of key/value pairs to update job_details JSON with
            save (bool): if True, save Job instance
        '''

        # parse job details
        try:
            if self.job_details:
                job_details = json.loads(self.job_details)
            elif not self.job_details:
                job_details = {}
        except:
            LOGGER.debug('could not parse job details')
            raise Exception('could not parse job details')

        # update details with update_dict
        job_details.update(update_dict)

        # if saving
        if save:
            self.job_details = json.dumps(job_details)
            self.save()

        # return
        return job_details

    def publish(self, publish_set_id=None):

        '''
        Method to publish Job
            - remove 'published_field_counts' doc from combine.misc Mongo collection

        Args:
            publish_set_id (str): identifier to group published Records
        '''

        # debug
        LOGGER.debug('publishing job #%s, with publish_set_id %s', self.id, publish_set_id)

        # remove previously saved published field counts
        mc_handle.combine.misc.delete_one({'_id':'published_field_counts'})

        # mongo db command
        result = mc_handle.combine.record.update_many({'job_id':self.id}, {'$set':{'published':True, 'publish_set_id':publish_set_id}}, upsert=False)
        LOGGER.debug('Matched %s, marked as published %s', result.matched_count, result.modified_count)

        # set self as published
        self.refresh_from_db()
        self.publish_set_id = publish_set_id
        self.published = True
        self.save()

        # add to job details
        self.update_job_details({
            'published':{
                'status':True,
                'publish_set_id':self.publish_set_id
            }
        })

        # return
        return True

    def unpublish(self):

        '''
        Method to unpublish Job
            - remove 'published_field_counts' doc from combine.misc Mongo collection
        '''

        # debug
        LOGGER.debug('unpublishing job #%s', (self.id))

        # remove previously saved published field counts
        mc_handle.combine.misc.delete_one({'_id':'published_field_counts'})

        # mongo db command
        result = mc_handle.combine.record.update_many({'job_id':self.id}, {'$set':{'published':False, 'publish_set_id':None}}, upsert=False)
        LOGGER.debug('Matched %s, marked as unpublished %s', result.matched_count, result.modified_count)

        # set self as publish
        self.refresh_from_db()
        self.publish_set_id = None
        self.published = False
        self.save()

        # add to job details
        self.update_job_details({
            'published':{
                'status':False,
                'publish_set_id':None
            }
        })

        # return
        return True

    def remove_records_from_db(self):

        '''
        Method to remove records from DB, fired as pre_delete signal
        '''

        LOGGER.debug('removing records from db')
        mc_handle.combine.record.delete_many({'job_id':self.id})
        LOGGER.debug('removed records from db')
        return True

    def remove_validations_from_db(self):

        '''
        Method to remove validations from DB, fired as pre_delete signal
            - usually handled by signals, but method left as convenience
        '''

        LOGGER.debug('removing validations from db')
        mc_handle.combine.record_validation.delete_many({'job_id':self.id})
        LOGGER.debug('removed validations from db')
        return True

    def remove_mapping_failures_from_db(self):

        '''
        Method to remove mapping failures from DB, fired as pre_delete signal
        '''

        LOGGER.debug('removing mapping failures from db')
        mc_handle.combine.index_mapping_failure.delete_many({'job_id':self.id})
        LOGGER.debug('removed mapping failures from db')
        return True

    def remove_validation_jobs(self, validation_scenarios=None):

        '''
        Method to remove validation jobs that match validation scenarios provided
            - NOTE: only one validation job should exist per validation scenario per Job
        '''

        # if validation scenarios provided
        if validation_scenarios != None:

            for vs_id in validation_scenarios:

                LOGGER.debug('removing JobValidations for Job %s, using vs_id %s', self, vs_id)

                # attempt to retrieve JobValidation
                jvs = JobValidation.objects.filter(validation_scenario=ValidationScenario.objects.get(pk=int(vs_id)), job=self)

                # if any found, delete all
                if jvs.count() > 0:
                    for job_validation in jvs:
                        job_validation.delete()

        # loop through and delete all
        else:
            for job_validation in self.jobvalidation_set.all():
                job_validation.delete()

        # return
        return True

    def get_downstream_jobs(
            self,
            include_self=True,
            topographic_sort=True,
            depth=None
        ):

        '''
        Method to retrieve downstream jobs, ordered by order required for re-running

        Args:
            include_self (bool): Boolean to include self as first in returned set
            topographic_sort (bool): Boolean to topographically sort returned set
            depth (None, int): None or int depth to recurse
        '''

        def _job_recurse(job_node, rec_depth):

            # bump recurse levels
            rec_depth += 1

            # get children
            downstream_jobs = JobInput.objects.filter(input_job=job_node)

            # if children, re-run
            if downstream_jobs.count() > 0:

                # recurse
                for downstream_job in downstream_jobs:

                    # add to sets
                    job_set.add(downstream_job.job)

                    # recurse
                    if depth == None or (depth != None and rec_depth < depth):
                        if downstream_job.job not in visited:
                            _job_recurse(downstream_job.job, rec_depth)
                            visited.add(downstream_job.job)

        # set lists and seed
        visited = set()
        if include_self:
            job_set = {self}
        else:
            job_set = set()

        # recurse
        _job_recurse(self, 0)

        # return topographically sorted
        if topographic_sort:
            return Job._topographic_sort_jobs(job_set)
        return list(job_set)

    def get_upstream_jobs(self, topographic_sort=True):

        '''
        Method to retrieve upstream jobs
            - placeholder for now
        '''

    @staticmethod
    def _topographic_sort_jobs(job_set):

        '''
        Method to topographically sort set of Jobs,
        using toposort (https://bitbucket.org/ericvsmith/toposort)

            - informed by cartesian JobInput links that exist between all Jobs in job_set

        Args:
            job_set (set): set of unordered jobs
        '''

        # if single job, return
        if len(job_set) == 1:
            return job_set

        # else, topographically sort
        # get all lineage edges given Job set
        lineage_edges = JobInput.objects.filter(job__in=job_set, input_job__in=job_set)

        # loop through and build graph
        edge_hash = {}
        for edge in lineage_edges:

            # if not in hash, add
            if edge.job not in edge_hash:
                edge_hash[edge.job] = set()

            # append required input job
            edge_hash[edge.job].add(edge.input_job)

        # topographically sort and return
        topo_sorted_jobs = list(toposort_flatten(edge_hash, sort=False))
        return topo_sorted_jobs

    def prepare_for_rerunning(self):
        self.timestamp = datetime.datetime.now()
        self.status = 'initializing'
        self.record_count = 0
        self.finished = False
        self.elapsed = 0
        self.deleted = True
        self.save()

    def stop_job(self, cancel_livy_statement=True, kill_spark_jobs=True):

        '''
        Stop running Job in Livy/Spark
            - cancel Livy statement
                - unnecessary if running Spark app, but helpful if queued by Livy
            - issue "kill" commands to Spark app
                - Livy statement cancels do not stop Spark jobs, but we can kill them manually
        '''

        LOGGER.debug('Stopping Job: %s', self)

        # get active livy session
        livy_session = LivySession.get_active_session()

        # if active session
        if livy_session:

            # send cancel to Livy
            if cancel_livy_statement:
                try:
                    livy_response = LivyClient().stop_job(self.url).json()
                    LOGGER.debug('Livy statement cancel result: %s', livy_response)
                except:
                    LOGGER.debug('error canceling Livy statement: %s', self.url)

            # retrieve list of spark jobs, killing two most recent
            # note: repeate x3 if job turnover so quick enough that kill is missed
            if kill_spark_jobs:
                try:
                    for _ in range(0, 3):
                        sjs = self.get_spark_jobs()
                        spark_job = sjs[0]
                        LOGGER.debug('Killing Spark Job: #%s, %s', spark_job['jobId'], spark_job.get('description', 'DESCRIPTION NOT FOUND'))
                        kill_result = SparkAppAPIClient.kill_job(livy_session, spark_job['jobId'])
                        LOGGER.debug('kill result: %s', kill_result)
                except:
                    LOGGER.debug('error killing Spark jobs')

        else:
            LOGGER.debug('active Livy session not found, unable to cancel Livy statement or kill Spark application jobs')

    def add_input_job(self, input_job, job_spec_input_filters=None):

        '''
        Method to add input Job to self
            - add JobInput instance
            - modify self.job_details_dict.input_job_ids

        Args:
            input_job (int, str, core.models.Job): Input Job to add
            job_spec_input_filters (dict): dictionary of Job specific input filters
        '''

        if self.job_type_family() not in ['HarvestJob']:

            # handle types
            if type(input_job) in [int, str]:
                input_job = Job.objects.get(pk=int(input_job))

            # check that inverse relationship does not exist,
            #  resulting in circular dependency
            if JobInput.objects.filter(job=input_job, input_job=self).count() > 0:
                LOGGER.debug('cannot add %s as Input Job, would result in circular dependency', input_job)
                return False

            # create JobInput instance
            job_input = JobInput(job=self, input_job=input_job)
            job_input.save()

            # add input_job.id to input_job_ids
            input_job_ids = self.job_details_dict['input_job_ids']
            input_job_ids.append(input_job.id)
            self.update_job_details({'input_job_ids':input_job_ids})

            # if job spec input filteres provided, add
            if job_spec_input_filters != None:
                input_filters = self.job_details_dict['input_filters']
                input_filters['job_specific'][str(input_job.id)] = job_spec_input_filters
                self.update_job_details({'input_filters':input_filters})

            # return
            LOGGER.debug('%s added as input job', input_job)
            return True

        LOGGER.debug('cannot add Input Job to Job type: %s', self.job_type_family())
        return False

    def remove_input_job(self, input_job):

        '''
        Method to remove input Job from self
            - remove JobInput instance
            - modify self.job_details_dict.input_job_ids

        Args:
            input_job (int, str, core.models.Job): Input Job to remove
        '''

        if self.job_type_family() not in ['HarvestJob']:

            # handle types
            if type(input_job) in [int, str]:
                input_job = Job.objects.get(pk=int(input_job))

            # create JobInput instance
            job_input = JobInput.objects.filter(job=self, input_job=input_job)
            job_input.delete()

            # add input_job.id to input_job_ids
            input_job_ids = self.job_details_dict['input_job_ids']
            try:
                input_job_ids.remove(input_job.id)
            except:
                LOGGER.debug('input job_id not found in input_job_ids: %s', input_job.id)
            self.update_job_details({'input_job_ids':input_job_ids})

            # check for job in job spec filters, remove if found
            if str(input_job.id) in self.job_details_dict['input_filters']['job_specific'].keys():
                input_filters = self.job_details_dict['input_filters']
                input_filters['job_specific'].pop(str(input_job.id))
                self.update_job_details({'input_filters':input_filters})

            # return
            LOGGER.debug('%s removed as input job', input_job)
            return True

        LOGGER.debug('cannot add Input Job to Job type: %s', self.job_type_family())
        return False

    def remove_as_input_job(self):

        '''
        Remove self from other Job's job_details that see it as input_job
        '''

        LOGGER.debug('removing self from child jobs input_job_ids')

        # get all JobInputs where self is input job
        immediate_children = self.get_downstream_jobs(include_self=False, depth=1)

        # loop through children and remove from job_details.input_job_ids
        for child in immediate_children:
            input_job_ids = child.job_details_dict['input_job_ids']
            input_job_ids.remove(self.id)
            child.update_job_details({'input_job_ids':input_job_ids})

    def remove_from_published_precounts(self):

        '''
        Method to remove Job from Published -- global and subsets -- precounts of mapped fields
            - remove global published pre-counts
            - if job's publish_set_id part of Published Subset, remove that pre-count as well
        '''

        # import
        from core.models.publishing import PublishedRecords

        # remove global published pre-count
        if self.published:
            delete = mc_handle.combine.misc.delete_one({'_id':'published_field_counts'})
            LOGGER.debug(delete.raw_result)

        # check if Job's publish_set_id in Published Subsets
        if self.publish_set_id != '':

            # check subsets and delete pre-counts if present
            subsets = PublishedRecords.get_subsets(includes_publish_set_id=self.publish_set_id)
            if len(subsets) > 0:
                for subset in subsets:
                    delete = mc_handle.combine.misc.delete_one({'_id':'published_field_counts_%s' % subset['name']})
                    LOGGER.debug(delete.raw_result)

    def remove_temporary_files(self):

        '''
        Method to remove temporary files when Job is deleted
        '''

        # get combine job
        cjob = CombineJob.get_combine_job(self.id)

        # if remove_temporary_files() method present, use
        if hasattr(cjob, 'remove_temporary_files'):
            cjob.remove_temporary_files()

        # else, skip
        else:
            LOGGER.debug('Job type %s, does not have remove_temporary_files() method, skipping', cjob.job.job_type)



class IndexMappingFailure(mongoengine.Document):

    db_id = mongoengine.StringField()
    record_id = mongoengine.StringField()
    job_id = mongoengine.IntField()
    mapping_error = mongoengine.StringField()

    # meta
    meta = {
        'index_options': {},
        'index_background': False,
        'auto_create_index': False,
        'index_drop_dups': False,
        'indexes': [
            {'fields': ['job_id']},
            {'fields': ['db_id']},
        ]
    }

    def __str__(self):
        return 'Index Mapping Failure: #%s' % (self.id)


    # cache
    _job = None
    _record = None


    # define job property
    @property
    def job(self):

        '''
        Method to retrieve Job from Django ORM via job_id
        '''

        if self._job is None:
            job = Job.objects.get(pk=self.job_id)
            self._job = job
        return self._job


    # convenience method
    @property
    def record(self):

        '''
        Method to retrieve Record from Django ORM via job_id
        '''

        if self._record is None:
            record = Record.objects.get(id=self.db_id)
            self._record = record
        return self._record



class JobValidation(models.Model):

    '''
    Model to record one-to-many relationship between jobs and validation scenarios run against its records
    '''

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    validation_scenario = models.ForeignKey(ValidationScenario, on_delete=models.CASCADE)
    failure_count = models.IntegerField(null=True, default=None)

    def __str__(self):
        return 'JobValidation: #%s, Job: #%s, ValidationScenario: #%s, failure count: %s' % (
            self.id, self.job.id, self.validation_scenario.id, self.failure_count)


    def get_record_validation_failures(self):

        '''
        Method to return records, for this job, with validation errors

        Args:
            None

        Returns:
            (django.db.models.query.QuerySet): RecordValidation queryset of records from self.job and self.validation_scenario
        '''

        rvfs = RecordValidation.objects\
            .filter(validation_scenario_id=self.validation_scenario.id)\
            .filter(job_id=self.job.id)
        return rvfs


    def validation_failure_count(self, force_recount=False):

        '''
        Method to count, set, and return failure count for this job validation
            - set self.failure_count if not set

        Args:
            None

        Returns:
            (int): count of records that did not pass validation (Note: each record may have failed 1+ assertions)
                - sets self.failure_count and saves model
        '''

        if (self.failure_count is None and self.job.finished) or force_recount:
            LOGGER.debug("calculating failure count for validation job: %s", self)
            rvfs = self.get_record_validation_failures()
            self.failure_count = rvfs.count()
            self.save()

        # return count
        return self.failure_count


    def delete_record_validation_failures(self):

        '''
        Method to delete record validations associated with this validation job
        '''

        rvfs = RecordValidation.objects\
            .filter(validation_scenario_id=self.validation_scenario.id)\
            .filter(job_id=self.job.id)
        del_results = rvfs.delete()
        LOGGER.debug('%s validations removed', del_results)
        return del_results



class JobTrack(models.Model):

    '''
    Model to record information about jobs from Spark context, as not to interfere with model `Job` transactions
    '''

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    start_timestamp = models.DateTimeField(null=True, auto_now_add=True)
    finish_timestamp = models.DateTimeField(null=True, auto_now_add=True)


    def __str__(self):
        return 'JobTrack: job_id #%s' % self.job_id



class JobInput(models.Model):

    '''
    Model to manage input jobs for other jobs.
    Provides a one-to-many relationship for a job and potential multiple input jobs
    '''

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    input_job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='input_job')
    passed_records = models.IntegerField(null=True, default=None)


    def __str__(self):
        return 'JobInputLink: input job #%s for job #%s' % (self.input_job.id, self.job.id)



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

        # using job_type, return instance of approriate job type
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

    '''
    Harvest records to Combine.

    This class represents a high-level "Harvest" job type, with more specific harvest types extending this class.
    In saved and associated core.models.Job instance, job_type will be "HarvestJob".

    Note: Unlike downstream jobs, Harvest does not require an input job
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
        ):

        '''
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent CombineJob init
                - captures args specific to Harvest jobs
        '''

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
                job_type=type(self).__name__, # selects this level of class inheritance hierarchy
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

    '''
    Harvest records from OAI-PMH endpoint
    Extends core.models.HarvestJob
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
        ):

        '''
        Args:

            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent HarvestJob init
        '''

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

        '''
        Method to parse job type specific parameters
        '''

        # retrieve endpoint params
        oai_params = OAIEndpoint.objects.get(pk=int(job_params.get('oai_endpoint_id'))).__dict__.copy()

        # drop _state
        oai_params.pop('_state')

        # retrieve overrides
        overrides = {override:job_params.get(override) for override in ['verb', 'metadataPrefix', 'scope_type', 'scope_value'] if job_params.get(override) != ''}

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

        '''
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            None

        Returns:
            None
                - submits job to Livy
        '''

        # prepare job code
        job_code = {
            'code':'from jobs import HarvestOAISpark\nHarvestOAISpark(spark, job_id="%(job_id)s").spark_function()' %
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

        '''
        return harvest job specific errors
        NOTE: Currently, we are not saving errors from OAI harveset, and so, cannot retrieve...
        '''

        return None



class HarvestStaticXMLJob(HarvestJob):

    '''
    Harvest records from static XML files
    Extends core.models.HarvestJob
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
        ):

        '''
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent HarvestJob init
        '''

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

        '''
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        '''

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
            job_details['payload_dir'] = '%s/static_uploads/%s' % (settings.BINARY_STORAGE.split('file://')[-1], str(uuid.uuid4()))
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

        '''
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            None

        Returns:
            None
                - submits job to Livy
        '''

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

        '''
        Currently not implemented for HarvestStaticXMLJob
        '''

        return None


    def remove_temporary_files(self):

        '''
        Method to remove temporary files associated with Job
        '''

        LOGGER.debug('investigating removal of temporary files')

        # check for existence of payload dir
        payload_dir = self.job.job_details_dict.get('payload_dir', False)
        if payload_dir and os.path.exists(payload_dir):

            # retrieve all Jobs that share payload_dir
            payload_deps = [_job for _job in Job.objects.all()
                            if _job.job_details_dict.get('payload_dir', False) == payload_dir
                            and _job != self.job]

            # if only this job, remove
            if len(payload_deps) == 0:

                try:
                    LOGGER.debug('removing payload_dir: %s', payload_dir)
                    shutil.rmtree(payload_dir)
                except Exception as err:
                    LOGGER.debug('trouble removing payload_dir: %s', str(err))

            else:

                LOGGER.debug('NOT removing payload_dir, shared by other Jobs: %s', payload_deps)



class HarvestTabularDataJob(HarvestJob):

    '''
    Harvest records from tabular data
    Extends core.models.HarvestJob
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
        ):

        '''
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - fires parent HarvestJob init
        '''

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

        '''
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        '''

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
            job_details['payload_dir'] = '%s/static_uploads/%s' % (settings.BINARY_STORAGE.split('file://')[-1], str(uuid.uuid4()))
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

        '''
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            None

        Returns:
            None
                - submits job to Livy
        '''

        # prepare job code
        job_code = {
            'code':'from jobs import HarvestTabularDataSpark\nHarvestTabularDataSpark(spark, job_id="%(job_id)s").spark_function()' %
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

        '''
        Currently not implemented for HarvestStaticXMLJob
        '''

        return None



class TransformJob(CombineJob):

    '''
    Apply an XSLT transformation to a Job
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
        ):

        '''
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - sets multiple attributes for self.job
                - sets in motion the output of spark jobs from core.spark.jobs
        '''

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
                job_type=type(self).__name__, # selects this level of class inheritance hierarchy
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

        '''
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        '''

        # retrieve input jobs
        job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id')]

        # retrieve transformation, add details to job details

        # reconstitute json and init job_details
        sel_trans = json.loads(job_params['sel_trans_json'])
        job_details['transformation'] = {
            'scenarios_json':job_params['sel_trans_json'],
            'scenarios':[]
        }

        # loop through and add with name and type
        for trans in sel_trans:
            transformation = Transformation.objects.get(pk=int(trans['trans_id']))
            job_details['transformation']['scenarios'].append({
                'name':transformation.name,
                'type':transformation.transformation_type,
                'type_human':transformation.get_transformation_type_display(),
                'id':transformation.id,
                'index':trans['index']
            })

        return job_details


    def prepare_job(self, return_job_code=False):

        '''
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            None

        Returns:
            None
                - submits job to Livy
        '''

        # prepare job code
        job_code = {
            'code':'from jobs import TransformSpark\nTransformSpark(spark, job_id="%(job_id)s").spark_function()' %
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

        '''
        Return errors from Job

        Args:
            None

        Returns:
            (django.db.models.query.QuerySet)
        '''

        return self.job.get_errors()



class MergeJob(CombineJob):

    '''
    Merge multiple jobs into a single job
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
        ):

        '''
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - sets multiple attributes for self.job
                - sets in motion the output of spark jobs from core.spark.jobs
        '''

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
                job_type=type(self).__name__, # selects this level of class inheritance hierarchy
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

        '''
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        '''

        # retrieve input jobs
        job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id')]

        return job_details


    def prepare_job(self, return_job_code=False):

        '''
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            None

        Returns:
            None
                - submits job to Livy
        '''

        # prepare job code
        job_code = {
            'code':'from jobs import MergeSpark\nMergeSpark(spark, job_id="%(job_id)s").spark_function()' % {
                'job_id':self.job.id
            }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)


    def get_job_errors(self):

        '''
        Not current implemented from Merge jobs, as primarily just copying of successful records
        '''



class AnalysisJob(CombineJob):

    '''
    Analysis job
        - Analysis job are unique in name and some functionality, but closely mirror Merge Jobs in execution
        - Though Analysis jobs are very similar to most typical workflow jobs, they do not naturally
        belong to an Organization and Record Group like others. As such, they dynamically create their own Org and
        Record Group, configured in localsettings.py, that is hidden from most other views.
    '''

    def __init__(
            self,
            user=None,
            job_id=None,
            record_group=None,
            job_details=None
        ):

        '''
        Args:
            user (django.auth.User): user account
            job_id (int): Job ID
            record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
            job_details (dict): dictionary for all Job parameters

        Returns:
            None
                - sets multiple attributes for self.job
                - sets in motion the output of spark jobs from core.spark.jobs
        '''

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
                job_type=type(self).__name__, # selects this level of class inheritance hierarchy
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

        '''
        Method to return organization and record_group for Analysis jobs
            - if do not exist, or name has changed, also create
            - reads from settings.ANALYSIS_JOBS_HIERARCHY for unique names for Organization and Record Group
        '''

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
            'organization':analysis_org,
            'record_group':analysis_record_group
        }



    @staticmethod
    def parse_job_type_params(job_details, job_params, kwargs):

        '''
        Method to parse job type specific parameters

        Args:
            job_details (dict): in-process job_details dictionary
            job_params (dict): original parameters passed to Job
            kwargs (dict): optional, named args for Jobs
        '''

        # retrieve input job
        job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id')]

        return job_details


    def prepare_job(self, return_job_code=False):

        '''
        Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

        Args:
            None

        Returns:
            None
                - submits job to Livy
        '''

        # prepare job code
        job_code = {
            'code':'from jobs import MergeSpark\nMergeSpark(spark, job_id="%(job_id)s").spark_function()' % {
                'job_id':self.job.id
            }
        }

        # return job code if requested
        if return_job_code:
            return job_code

        # submit job
        self.submit_job_to_livy(job_code)


    def get_job_errors(self):

        '''
        Not current implemented from Analyze jobs, as primarily just copying of successful records
        '''



class Record(mongoengine.Document):

    # fields
    combine_id = mongoengine.StringField()
    document = mongoengine.StringField()
    error = mongoengine.StringField()
    fingerprint = mongoengine.IntField()
    job_id = mongoengine.IntField()
    oai_set = mongoengine.StringField()
    publish_set_id = mongoengine.StringField()
    published = mongoengine.BooleanField(default=False)
    record_id = mongoengine.StringField()
    success = mongoengine.BooleanField(default=True)
    transformed = mongoengine.BooleanField(default=False)
    unique = mongoengine.BooleanField(default=True)
    unique_published = mongoengine.BooleanField(default=True)
    valid = mongoengine.BooleanField(default=True)
    dbdm = mongoengine.BooleanField(default=False)
    orig_id = mongoengine.StringField()

    # meta
    meta = {
        'index_options': {},
        'index_background': False,
        'auto_create_index': False,
        'index_drop_dups': False,
        'indexes': [
            {'fields': ['job_id']},
            {'fields': ['record_id']},
            {'fields': ['combine_id']},
            {'fields': ['success']},
            {'fields': ['valid']},
            {'fields': ['published']},
            {'fields': ['publish_set_id']},
            {'fields': ['dbdm']}
        ]
    }

    # cached attributes
    _job = None


    def __str__(self):
        return 'Record: %s, record_id: %s, Job: %s' % (self.id, self.record_id, self.job.name)


    # _id shim property
    @property
    def _id(self):
        return self.id


    # define job property
    @property
    def job(self):

        '''
        Method to retrieve Job from Django ORM via job_id
        '''
        if self._job is None:
            try:
                job = Job.objects.get(pk=self.job_id)
            except:
                job = False
            self._job = job
        return self._job


    def get_record_stages(self, input_record_only=False, remove_duplicates=True):

        '''
        Method to return all upstream and downstreams stages of this record

        Args:
            input_record_only (bool): If True, return only immediate record that served as input for this record.
            remove_duplicates (bool): Removes duplicates - handy for flat list of stages,
            but use False to create lineage

        Returns:
            (list): ordered list of Record instances from first created (e.g. Harvest), to last (e.g. Publish).
            This record is included in the list.
        '''

        record_stages = []

        def get_upstream(record, input_record_only):

            # check for upstream job
            upstream_job_query = record.job.jobinput_set

            # if upstream jobs found, continue
            if upstream_job_query.count() > 0:

                LOGGER.debug('upstream jobs found, checking for combine_id')

                # loop through upstream jobs, look for record id
                for upstream_job in upstream_job_query.all():
                    upstream_record_query = Record.objects.filter(
                        job_id=upstream_job.input_job.id,
                        combine_id=self.combine_id
                        )

                    # if count found, save record to record_stages and re-run
                    if upstream_record_query.count() > 0:
                        upstream_record = upstream_record_query.first()
                        record_stages.insert(0, upstream_record)
                        if not input_record_only:
                            get_upstream(upstream_record, input_record_only)


        def get_downstream(record):

            # check for downstream job
            downstream_job_query = JobInput.objects.filter(input_job=record.job)

            # if downstream jobs found, continue
            if downstream_job_query.count() > 0:

                LOGGER.debug('downstream jobs found, checking for combine_id')

                # loop through downstream jobs
                for downstream_job in downstream_job_query.all():

                    downstream_record_query = Record.objects.filter(
                        job_id=downstream_job.job.id,
                        combine_id=self.combine_id
                    )

                    # if count found, save record to record_stages and re-run
                    if downstream_record_query.count() > 0:
                        downstream_record = downstream_record_query.first()
                        record_stages.append(downstream_record)
                        get_downstream(downstream_record)

        # run
        get_upstream(self, input_record_only)
        if not input_record_only:
            record_stages.append(self)
            get_downstream(self)

        # remove duplicate
        if remove_duplicates:
            record_stages = list(OrderedDict.fromkeys(record_stages))

        # return
        return record_stages


    def get_es_doc(self, drop_combine_fields=False):

        '''
        Return indexed ElasticSearch document as dictionary.
        Search is limited by ES index (Job associated) and combine_id

        Args:
            None

        Returns:
            (dict): ES document
        '''

        # init search
        search = Search(using=es_handle, index='j%s' % self.job_id)
        search = search.query('match', _id=str(self.id))

        # drop combine fields if flagged
        if drop_combine_fields:
            search = search.source(exclude=['combine_id', 'db_id', 'fingerprint', 'publish_set_id', 'record_id'])

        # execute search and capture as dictionary
        try:
            search_result = search.execute()
            sr_dict = search_result.to_dict()
        except NotFoundError:
            LOGGER.debug('mapped fields for record not found in ElasticSearch')
            return {}

        # return
        try:
            return sr_dict['hits']['hits'][0]['_source']
        except:
            return {}


    @property
    def mapped_fields(self):

        '''
        Return mapped fields as property
        '''

        return self.get_es_doc()


    @property
    def document_mapped_fields(self):

        '''
        Method to return mapped fields, dropping internal Combine fields
        '''

        return self.get_es_doc(drop_combine_fields=True)


    def get_dpla_mapped_fields(self):

        '''
        Method to return DPLA specific mapped fields from Record's mapped fields
        '''

        # get mapped fields and return filtered
        return {f:v for f, v in self.get_es_doc().items() if f.startswith('dpla_')}


    def parse_document_xml(self):

        '''
        Parse self.document as XML node with etree

        Args:
            None

        Returns:
            (tuple): ((bool) result of XML parsing, (lxml.etree._Element) parsed document)
        '''
        try:
            return (True, etree.fromstring(self.document.encode('utf-8')))
        except Exception as err:
            LOGGER.debug(str(err))
            return (False, str(err))


    def dpla_api_record_match(self, search_string=None):

        '''
        Method to query DPLA API for match against mapped fields
            - querying is an ranked list of fields to consecutively search
            - this method is recursive such that a preformatted search string can be fed back into it

        Args:
            search_string(str): Optional search_string override

        Returns:
            (dict): If match found, return dictionary of DPLA API response
        '''

        # check for DPLA_API_KEY, else return None
        if settings.DPLA_API_KEY:

            # check for any mapped DPLA fields, skipping altogether if none
            mapped_dpla_fields = self.get_dpla_mapped_fields()
            if len(mapped_dpla_fields) > 0:

                # attempt search if mapped fields present and search_string not provided
                if not search_string:

                    # ranked search fields
                    opinionated_search_fields = [
                        ('dpla_isShownAt', 'isShownAt'),
                        ('dpla_title', 'sourceResource.title'),
                        ('dpla_description', 'sourceResource.description')
                    ]

                    # loop through ranked search fields
                    for local_mapped_field, target_dpla_field in opinionated_search_fields:

                        # if local_mapped_field in keys
                        if local_mapped_field in mapped_dpla_fields.keys():

                            # get value for mapped field
                            field_value = mapped_dpla_fields[local_mapped_field]

                            # if list, loop through and attempt searches
                            if type(field_value) == list:

                                for val in field_value:
                                    search_string = urllib.parse.urlencode({target_dpla_field:'"%s"' % val})
                                    match_results = self.dpla_api_record_match(search_string=search_string)

                            # else if string, perform search
                            else:
                                search_string = urllib.parse.urlencode({target_dpla_field:'"%s"' % field_value})
                                match_results = self.dpla_api_record_match(search_string=search_string)


                    # parse results
                    # count instances of isShownAt, a single one is good enough
                    if 'isShownAt' in self.dpla_api_matches.keys() and len(self.dpla_api_matches['isShownAt']) == 1:
                        self.dpla_api_doc = self.dpla_api_matches['isShownAt'][0]['hit']

                    # otherwise, count all, and if only one, use
                    else:
                        matches = []
                        for field, field_matches in self.dpla_api_matches.items():
                            matches.extend(field_matches)

                        if len(matches) == 1:
                            self.dpla_api_doc = matches[0]['hit']

                        else:
                            self.dpla_api_doc = None

                    # return
                    return self.dpla_api_doc

                #else, if search_string
                # prepare search query
                api_q = requests.get(
                    'https://api.dp.la/v2/items?%s&api_key=%s' % (search_string, settings.DPLA_API_KEY))

                # attempt to parse response as JSON
                try:
                    api_r = api_q.json()
                except:
                    LOGGER.debug('DPLA API call unsuccessful: code: %s, response: %s', api_q.status_code, api_q.content)
                    self.dpla_api_doc = None
                    return self.dpla_api_doc

                # if count present
                if type(api_r) == dict:
                    if 'count' in api_r.keys():

                        # response
                        if api_r['count'] >= 1:

                            # add matches to matches
                            field, value = search_string.split('=')
                            value = urllib.parse.unquote(value)

                            # check for matches attr
                            if not hasattr(self, "dpla_api_matches"):
                                self.dpla_api_matches = {}

                            # add mapped field used for searching
                            if field not in self.dpla_api_matches.keys():
                                self.dpla_api_matches[field] = []

                            # add matches for values searched
                            for doc in api_r['docs']:
                                self.dpla_api_matches[field].append({
                                    "search_term":value,
                                    "hit":doc
                                    })

                        else:
                            if not hasattr(self, "dpla_api_matches"):
                                self.dpla_api_matches = {}
                    else:
                        LOGGER.debug('non-JSON response from DPLA API: %s', api_r)
                        if not hasattr(self, "dpla_api_matches"):
                            self.dpla_api_matches = {}

                else:
                    LOGGER.debug(api_r)

        # return None by default
        self.dpla_api_doc = None
        return self.dpla_api_doc


    def get_validation_errors(self):

        '''
        Return validation errors associated with this record
        '''

        vfs = RecordValidation.objects.filter(record_id=self.id)
        return vfs


    def document_pretty_print(self):

        '''
        Method to return document as pretty printed (indented) XML
        '''

        # return as pretty printed string
        parsed_doc = self.parse_document_xml()
        if parsed_doc[0]:
            return etree.tostring(parsed_doc[1], pretty_print=True)
        raise Exception(parsed_doc[1])


    def get_lineage_url_paths(self):

        '''
        get paths of Record, Record Group, and Organzation
        '''

        record_lineage_urls = {
            'record':{
                'name':self.record_id,
                'path':reverse('record', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id, 'job_id':self.job.id, 'record_id':self.id})
                },
            'job':{
                'name':self.job.name,
                'path':reverse('job_details', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id, 'job_id':self.job.id})
                },
            'record_group':{
                'name':self.job.record_group.name,
                'path':reverse('record_group', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id})
                },
            'organization':{
                'name':self.job.record_group.organization.name,
                'path':reverse('organization', kwargs={'org_id':self.job.record_group.organization.id})
                }
        }

        return record_lineage_urls


    def get_input_record_diff(self, output='all', combined_as_html=False):

        '''
        Method to return a string diff of this record versus the input record
            - this is primarily helpful for Records from Transform Jobs
            - use self.get_record_stages(input_record_only=True)[0]

        Returns:
            (str|list): results of Record documents diff, line-by-line
        '''

        # check if Record has input Record
        irq = self.get_record_stages(input_record_only=True)
        if len(irq) == 1:
            LOGGER.debug('single, input Record found: %s', irq[0])

            # get input record
            input_record = irq[0]

            # check if fingerprints the same
            if self.fingerprint != input_record.fingerprint:

                LOGGER.debug('fingerprint mismatch, returning diffs')
                return self.get_record_diff(
                    input_record=input_record,
                    output=output,
                    combined_as_html=combined_as_html
                )

            # else, return None
            LOGGER.debug('fingerprint match, returning None')
            return None

        return False


    def get_record_diff(
            self,
            input_record=None,
            xml_string=None,
            output='all',
            combined_as_html=False,
            reverse_direction=False
        ):

        '''
        Method to return diff of document XML strings

        Args;
            input_record (core.models.Record): use another Record instance to compare diff
            xml_string (str): provide XML string to provide diff on

        Returns:
            (dict): {
                'combined_gen' : generator of diflibb
                'side_by_side_html' : html output of sxsdiff lib
            }

        '''

        if input_record:
            input_xml_string = input_record.document

        elif xml_string:
            input_xml_string = xml_string

        else:
            LOGGER.debug('input record or XML string required, returning false')
            return False

        # prepare input / result
        docs = [input_xml_string, self.document]
        if reverse_direction:
            docs.reverse()

        # include combine generator in output
        if output in ['all', 'combined_gen']:

            # get generator of differences
            combined_gen = difflib.unified_diff(
                docs[0].splitlines(),
                docs[1].splitlines()
            )

            # return as HTML
            if combined_as_html:
                combined_gen = self._return_combined_diff_gen_as_html(combined_gen)

        else:
            combined_gen = None

        # include side_by_side html in output
        if output in ['all', 'side_by_side_html']:

            sxsdiff_result = DiffCalculator().run(docs[0], docs[1])
            sio = io.StringIO()
            GitHubStyledGenerator(file=sio).run(sxsdiff_result)
            sio.seek(0)
            side_by_side_html = sio.read()

        else:
            side_by_side_html = None

        return {
            'combined_gen':combined_gen,
            'side_by_side_html':side_by_side_html
        }


    @staticmethod
    def _return_combined_diff_gen_as_html(combined_gen):

        '''
        Small method to return combined diff generated as pre-compiled HTML
        '''

        html = '<pre><code>'
        for line in combined_gen:
            if line.startswith('-'):
                html += '<span style="background-color:#ffeef0;">'
            elif line.startswith('+'):
                html += '<span style="background-color:#e6ffed;">'
            else:
                html += '<span>'
            html += line.replace('<', '&lt;').replace('>', '&gt;')
            html += '</span><br>'
        html += '</code></pre>'

        return html


    def calc_fingerprint(self, update_db=False):

        '''
        Generate fingerprint hash with binascii.crc32()
        '''

        fingerprint = binascii.crc32(self.document.encode('utf-8'))

        if update_db:
            self.fingerprint = fingerprint
            self.save()

        return fingerprint


    def map_fields_for_es(self, mapper):

        '''
        Method for testing how a Record will map given an instance
        of a mapper from core.spark.es
        '''

        stime = time.time()
        mapped_fields = mapper.map_record(record_string=self.document)
        LOGGER.debug('mapping elapsed: %s', (time.time()-stime))
        return mapped_fields



class RecordValidation(mongoengine.Document):

    # fields
    record_id = mongoengine.ReferenceField(Record, reverse_delete_rule=mongoengine.CASCADE)
    record_identifier = mongoengine.StringField()
    job_id = mongoengine.IntField()
    validation_scenario_id = mongoengine.IntField()
    validation_scenario_name = mongoengine.StringField()
    valid = mongoengine.BooleanField(default=True)
    results_payload = mongoengine.StringField()
    fail_count = mongoengine.IntField()

    # meta
    meta = {
        'index_options': {},
        'index_background': False,
        'auto_create_index': False,
        'index_drop_dups': False,
        'indexes': [
            {'fields': ['record_id']},
            {'fields': ['job_id']},
            {'fields': ['validation_scenario_id']}
        ]
    }

    # cache
    _validation_scenario = None
    _job = None

    # define Validation Scenario property
    @property
    def validation_scenario(self):

        '''
        Method to retrieve Job from Django ORM via job_id
        '''
        if self._validation_scenario is None:
            validation_scenario = ValidationScenario.objects.get(pk=self.validation_scenario_id)
            self._validation_scenario = validation_scenario
        return self._validation_scenario


    # define job property
    @property
    def job(self):

        '''
        Method to retrieve Job from Django ORM via job_id
        '''
        if self._job is None:
            job = Job.objects.get(pk=self.job_id)
            self._job = job
        return self._job


    # convenience method
    @property
    def record(self):
        return self.record_id


    # failed tests as property
    @property
    def failed(self):
        return json.loads(self.results_payload)['failed']
