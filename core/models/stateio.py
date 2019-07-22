# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import datetime
import json
import logging
import os
import polling
import shutil
import time
import uuid

# django imports
from django.conf import settings
from django.core import serializers


# core models imports
from core import tasks
from core.mongo import mongoengine, mc_handle
from core.models.transformation import Transformation
from core.models.validation_scenario import ValidationScenario
from core.models.field_mapper import FieldMapper
from core.models.record_identifier_transformation_scenario import RecordIdentifierTransformationScenario
from core.models.dpla_bulk_data_download import DPLABulkDataDownload
from core.models.oai_endpoint import OAIEndpoint
from core.models.job import Job, JobValidation, JobInput, CombineJob
from core.models.livy_spark import LivySession, LivyClient
from core.models.organization import Organization
from core.models.publishing import PublishedRecords
from core.models.record_group import RecordGroup
from core.models.tasks import CombineBackgroundTask

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


class StateIO(mongoengine.Document):

    '''
    Model to facilitate the recording of State Exports and Imports in Combine
        - flexible to both, defined by stateio_type
        - identifiers and manifests may be null
    '''


    name = mongoengine.StringField()
    stateio_type = mongoengine.StringField(
        required=True,
        choices=[('export', 'Export'), ('import', 'Import'), ('generic', 'Generic')],
        default='generic'
    )
    status = mongoengine.StringField(
        default='initializing'
    )
    export_id = mongoengine.StringField()
    export_manifest = mongoengine.DictField()
    export_path = mongoengine.StringField()
    import_id = mongoengine.StringField()
    import_manifest = mongoengine.DictField()
    import_path = mongoengine.StringField()
    bg_task_id = mongoengine.IntField()
    finished = mongoengine.BooleanField(default=False)


    # meta
    meta = {
        'index_options': {},
        'index_drop_dups': False,
        'indexes': []
    }

    # custom save
    def save(self, *args, **kwargs):

        if self.name == None:
            self.name = 'StateIO %s @ %s' % (self.stateio_type.title(), datetime.datetime.now().strftime('%b. %d, %Y, %-I:%M:%S %p'))
        return super(StateIO, self).save(*args, **kwargs)


    def __str__(self):
        return 'StateIO: %s, %s, %s' % (self.id, self.name, self.stateio_type)


    # _id shim property
    @property
    def _id(self):
        return self.id


    # timestamp property
    @property
    def timestamp(self):
        return self.id.generation_time if self.id else None

    # statuses: initializing,running,finished
    @property
    def progress_bar_motion(self):
        return self.status in 'initializing,running'

    @property
    def progress_bar_color(self):
        if self.status in 'initializing':
            return 'warning'
        if self.status in 'finished':
            return 'success'
        return ''

    @property
    def progress_bar_status(self):
        return self.status


    # pre_delete method
    @classmethod
    def pre_delete(cls, sender, document, **kwargs):

        LOGGER.debug('preparing to delete %s', document)

        try:

            # if export
            if document.stateio_type == 'export':

                # check for export_path and delete
                LOGGER.debug('removing export_path: %s', document.export_path)
                if os.path.isfile(document.export_path):
                    LOGGER.debug('export is filetype, removing')
                    os.remove(document.export_path)
                elif os.path.isdir(document.export_path):
                    LOGGER.debug('export is dir, removing')
                    shutil.rmtree(document.export_path)
                else:
                    LOGGER.debug('Could not remove %s', document.export_path)

            # if import
            elif document.stateio_type == 'import':

                # check for export_path and delete
                LOGGER.debug('removing import_path: %s', document.import_manifest['import_path'])
                if os.path.isdir(document.import_path):
                    LOGGER.debug('removing import dir')
                    shutil.rmtree(document.import_path)
                else:
                    LOGGER.debug('Could not remove %s', document.import_path)

        except:
            LOGGER.warning('Could not remove import/export directory')


    def export_all(self, skip_dict=None):

        '''
        Method to export all exportable objects in Combine

        Args:

            skip_dict (dict): Dictionary of model instances to skip
                - currently only supporting Organizations to skip, primarily for debugging purposes

        Todo:
            - currently only filtering org_ids from skip_dict, but could expand to skip
            any model type
        '''

        # if not yet initialized with id, do that now
        if not self.id:
            self.save()

        # select all Orgs
        LOGGER.debug('retrieving all Organizations')
        org_ids = [org.id for org in Organization.objects.filter(for_analysis=False)]

        # capture all config scenarios
        conf_model_instances = []

        # hash for conversion
        config_prefix_hash = {
            ValidationScenario:'validations',
            Transformation:'transformations',
            OAIEndpoint:'oai_endpoints',
            RecordIdentifierTransformationScenario:'rits',
            FieldMapper:'field_mapper_configs',
            DPLABulkDataDownload:'dbdd'
        }

        # loop through types
        for conf_model in [
                ValidationScenario,
                Transformation,
                OAIEndpoint,
                RecordIdentifierTransformationScenario,
                FieldMapper,
                DPLABulkDataDownload
            ]:

            LOGGER.debug('retrieving all config scenarios for: %s', conf_model.__name__)
            conf_model_instances.extend(['%s|%s' % (config_prefix_hash[conf_model], conf_instance.id) for conf_instance in conf_model.objects.all()])


        # using skip_dict to filter orgs
        if 'orgs' in skip_dict:
            LOGGER.debug('found orgs to skip in skip_dict')
            for org_id in skip_dict['orgs']:
                org_ids.remove(org_id)

        # fire StateIOClient with id
        sioc = StateIOClient()
        sioc.export_state(
            orgs=org_ids,
            config_scenarios=conf_model_instances,
            export_name=self.name,
            stateio_id=self.id
        )


    @property
    def bg_task(self):

        '''
        Method to return CombineBackgroundTask instance
        '''

        if self.bg_task_id != None:
            # run query
            ct_task_query = CombineBackgroundTask.objects.filter(id=int(self.bg_task_id))
            if ct_task_query.count() == 1:
                return ct_task_query.first()
            return False
        return False



class StateIOClient():

    '''
    Client to facilitate export and import of states in Combine, including:
        - Organizations, Record Groups, and Jobs
        - Records, Validations, and Mapped Fields associated with Jobs
        - configuration Scenarios
            - Transformation
            - Validation
            - RITS
            - DPLA Bulk Data Downloads (?)
    '''


    # model translation for serializable strings for models
    model_translation = {
        'jobs':Job,
        'record_groups':RecordGroup,
        'orgs':Organization,
        'dbdd':DPLABulkDataDownload,
        'oai_endpoints':OAIEndpoint,
        'rits':RecordIdentifierTransformationScenario,
        'transformations':Transformation,
        'validations':ValidationScenario
    }


    def __init__(self):

        # ensure working directories exist
        self._confirm_working_dirs()

    @staticmethod
    def _confirm_working_dirs():

        '''
        Method to ensure working dirs exist, and are writable
        '''

        # check for exports directory
        if not os.path.isdir(settings.STATEIO_EXPORT_DIR):
            LOGGER.debug('creating StateIO working directory: %s', settings.STATEIO_EXPORT_DIR)
            os.makedirs(settings.STATEIO_EXPORT_DIR)

        # check for imports directory
        if not os.path.isdir(settings.STATEIO_IMPORT_DIR):
            LOGGER.debug('creating StateIO working directory: %s', settings.STATEIO_IMPORT_DIR)
            os.makedirs(settings.STATEIO_IMPORT_DIR)


    def export_state(
            self,
            jobs=[],
            record_groups=[],
            orgs=[],
            config_scenarios=[],
            export_name=None,
            compress=True,
            compression_format='zip',
            stateio_id=None
        ):

        '''
        Method to export state of passed Organization, Record Group, and/or Jobs

        Args:
            jobs (list): List of Jobs to export, accept instance or id
            record_groups (list): List of Record Groups to export, accept instance or id
            orgs (list): List of Organizations to export, accept instance or id
            config_scenarios (list): List of configuration-related model *instances*
                - sorted based on type(instance) and added to self.export_dict
            compress (bool): If True, compress to zip file and delete directory
            compression_format (str): Possible values here: https://docs.python.org/3.5/library/shutil.html#shutil.make_archive
            stateio (int): ID of pre-existing, associated StateIO instance
        '''

        # DEBUG
        stime = time.time()

        # location of export created, or imported
        self.export_path = None

        # dictionary used to aggregate components for export
        self.export_dict = {

            # job related
            'jobs':set(),
            'record_groups':set(),
            'orgs':set(),
            'job_inputs':set(),
            'job_validations':set(),

            # config scenarios
            'validations':set(),
            'transformations':set(),
            'oai_endpoints':set(),
            'rits':set(),
            'field_mapper_configs':set(),
            'dbdd':set(),

        }

        # save compression flag
        self.compress = compress
        self.compression_format = compression_format

        # set export_roots with model instances
        self.export_roots = {
            'jobs':[Job.objects.get(pk=int(job)) if isinstance(job, (int, str)) else job for job in jobs],
            'record_groups':[RecordGroup.objects.get(pk=int(record_group)) if isinstance(record_group, (int, str)) else record_group for record_group in record_groups],
            'orgs':[Organization.objects.get(pk=int(org)) if isinstance(org, (int, str)) else org for org in orgs],
        }
        # set export_root_ids with model ids
        self.export_roots_ids = {
            'jobs':[job.id for job in self.export_roots['jobs']],
            'record_groups': [record_group.id for record_group in self.export_roots['record_groups']],
            'orgs': [org.id for org in self.export_roots['orgs']]
        }

        # unique hash for export, used for filepath and manifest
        export_id = uuid.uuid4().hex

        # prepare tmp location for export
        self.export_path = '%s/%s' % (settings.STATEIO_EXPORT_DIR, export_id)
        os.mkdir(self.export_path)

        # init export manifest dictionary

        self.export_manifest = {
            'combine_version':getattr(settings, 'COMBINE_VERSION', None),
            'export_id':export_id,
            'export_path':self.export_path,
            'export_name':export_name,
            'export_root_ids':self.export_roots_ids,
            'jobs':[]
        }

        # init/update associated StateIO instance
        sio_dict = {
            'stateio_type':'export',
            'name':self.export_manifest['export_name'],
            'export_id':self.export_manifest['export_id'],
            'export_path':self.export_manifest['export_path'],
            'export_manifest':self.export_manifest,
            'status':'running'
        }
        if stateio_id == None:
            LOGGER.debug('initializing StateIO object')
            self.stateio = StateIO(**sio_dict)
        else:
            LOGGER.debug('retrieving and updating StateIO object')
            self.stateio = StateIO.objects.get(id=stateio_id)
            self.stateio.update(**sio_dict)
            self.stateio.reload()
        # save
        self.stateio.save()

        # generate unique set of Jobs
        self.export_job_set = self._get_unique_roots_job_set()

        # if jobs present from export_roots
        if len(self.export_job_set) > 0:

            # based on Jobs identified from export_roots, collect all connected Jobs
            self._collect_related_jobs()

            # based on network of Jobs, collection associated components
            self._collect_related_components()

        # handle non-Job related configuration scenarios passed
        if len(config_scenarios) > 0:
            self._sort_discrete_config_scenarios(config_scenarios)

        # serialize for export
        self.package_export()

        # update associated StateIO instance
        self.stateio.update(**{
            'export_manifest':self.export_manifest,
            'export_path':self.export_manifest['export_path'],
            'status':'finished',
            'finished':True
        })
        self.stateio.reload()
        self.stateio.save()

        # debug
        LOGGER.debug("total time for state export: %s", (time.time() - stime))


    def _get_unique_roots_job_set(self):

        '''
        Method to get unique set of Jobs from self.export_roots
            - export_roots may contain Organizations, Record Groups, and Jobs
        '''

        # extend self.export_roots['record_groups'] with Record Groups from Orgs
        for org in self.export_roots['orgs']:
            self.export_roots['record_groups'].extend(org.recordgroup_set.all())

        # extend self.export_roots['jobs'] with Jobs from Record Groups
        for record_group in self.export_roots['record_groups']:
            self.export_roots['jobs'].extend(record_group.job_set.all())

        # ensure all Jobs unique and return
        self.export_roots['jobs'] = list(set(self.export_roots['jobs']))
        return self.export_roots['jobs']


    def _collect_related_jobs(self, include_upstream=True):

        '''
        Method to collect all connected Jobs based on Jobs identified
        in export_roots['jobs'].

        Topographically sorts all Jobs, to determine order.

        First:
            - Connected Jobs from lineage
                - upstream, downstream

        Second:
            - Related scenarios (mostly, to facilitate re-running once re-imported)
                - OAI endpoint
                - transformation
                - validation
                - RITS <------- waiting
                - DBDD <------- waiting
                - field mapper configurations <------- waiting, might need to store ID in job_details?
            - ElasticSearch index for each Job
            - Records from Mongo
            - Validations from Mongo

        TODO:
            - break pieces into sub-methods

        Args:
            include_upstream (bool): Boolean to also collect "upstream" Jobs
                - defaults to True, to safely ensure that Jobs exported/imported have all
                supporting infrastructure

        Returns:
            None
                - sets final list of Jobs to export at self.export_dict['jobs'],
                topographically sorted and unique
        '''

        ############################
        # JOBS
        ############################

        # loop through Jobs identified from roots and queue
        for job in self.export_roots['jobs']:

            # add job
            self.export_dict['jobs'].add(job)

            # get downstream
            downstream_jobs = job.get_downstream_jobs()
            self.export_dict['jobs'].update(downstream_jobs)

            if include_upstream:

                # get upstream for all Jobs currently queued in export_dict
                upstream_jobs = []
                for export_job in self.export_dict['jobs']:

                    # get lineage
                    # TODO: replace with new upstream lineage method that's coming
                    lineage = export_job.get_lineage()

                    # loop through nodes and add to set
                    for lineage_job_dict in lineage['nodes']:
                        upstream_jobs.append(Job.objects.get(pk=int(lineage_job_dict['id'])))

                # update set with upstream
                self.export_dict['jobs'].update(upstream_jobs)


        # topopgraphically sort all queued export jobs, and write to manifest
        self.export_dict['jobs'] = Job._topographic_sort_jobs(self.export_dict['jobs'])
        self.export_manifest['jobs'] = [job.id for job in self.export_dict['jobs']]


    def _collect_related_components(self):

        '''
        Method to collect related components based on self.export_dict['jobs'],
        and items from self.

        All operate over self.export_dict['jobs'], updating other sections of
        self.export_dict

        TODO:
            - these would benefit for more error and existence checking
                - even if dependencies are not found, exports should continue
        '''

        ###################################
        # ORGANIZATIONS and RECORD GROUPS
        ###################################

        # extend self.export_dict['orgs'] and self.export_dict['record_groups']
        for job in self.export_dict['jobs']:
            self.export_dict['orgs'].add(job.record_group.organization)
            self.export_dict['record_groups'].add(job.record_group)


        ############################
        # JOBS: Job Input
        ############################

        # get all related Job Inputs
        job_inputs = JobInput.objects.filter(
            job__in=self.export_dict['jobs'],
            input_job__in=self.export_dict['jobs'])

        # write to serialize set
        self.export_dict['job_inputs'].update(job_inputs)


        ############################
        # JOBS: Job Validation
        ############################

        # get all related Job Inputs
        job_validations = JobValidation.objects.filter(job__in=self.export_dict['jobs'])

        # write to serialize set
        self.export_dict['job_validations'].update(job_validations)


        ############################
        # TRANSFORMATION SCENARIOS
        ############################

        # loop through Jobs, looking for Transformation Scenarios
        for job in self.export_dict['jobs']:

            # check job details for transformation used
            if 'transformation' in job.job_details_dict.keys():

                try:
                    for trans in job.job_details_dict['transformation']['scenarios']:
                        self.export_dict['transformations'].add(Transformation.objects.get(pk=int(trans['id'])))
                except Exception as err:
                    LOGGER.warning('Could not export Transformations for job %s: %s', job, str(err))


        ############################
        # VALIDATION SCENARIOS
        ############################

        # loop through Jobs, looking for Validations applied Scenarios
        for job in self.export_dict['jobs']:

            # check for JobValidation instances
            jvs = JobValidation.objects.filter(job=job)

            # loop through and add to set
            for job_validation in jvs:
                self.export_dict['validations'].add(job_validation.validation_scenario)


        ############################
        # OAI ENDPOINTS
        ############################

        # loop through Jobs, looking for OAI endpoints that need exporting
        for job in self.export_dict['jobs']:

            if job.job_type == 'HarvestOAIJob':

                try:
                    # read OAI endpoint from params
                    self.export_dict['oai_endpoints'].add(OAIEndpoint.objects.get(pk=job.job_details_dict['oai_params']['id']))
                except Exception as err:
                    LOGGER.warning('Could not export OAIEndpoint for job %s: %s', job, str(err))


        ############################
        # RITS
        ############################

        # loop through Jobs, looking for RITS Scenarios
        for job in self.export_dict['jobs']:

            # check job details for rits used
            if 'rits' in job.job_details_dict.keys() and job.job_details_dict['rits'] != None:
                try:
                    self.export_dict['rits'].add(RecordIdentifierTransformationScenario.objects.get(pk=(job.job_details_dict['rits'])))
                except Exception as err:
                    LOGGER.warning('Could not export Record Identifier Transformation Scenario for job %s: %s', job, str(err))


        ############################
        # DBDD
        ############################

        # loop through Jobs, looking for DBDD used
        for job in self.export_dict['jobs']:

            # check job details for DBDD used
            if 'dbdm' in job.job_details_dict.keys() and job.job_details_dict['dbdm']['dbdd'] != None:

                LOGGER.debug('attempting to export dbdd_id %s for %s', job.job_details_dict['dbdm']['dbdd'], job)

                try:
                    # get dbdd
                    dbdd = DPLABulkDataDownload.objects.get(pk=(job.job_details_dict['dbdm']['dbdd']))

                    # add to export_dict
                    self.export_dict['dbdd'].add(dbdd)

                    # export DBDD index from ElaticSearch
                    # prepare dbdd export dir
                    dbdd_export_path = '%s/dbdd' % self.export_path
                    if not os.path.isdir(dbdd_export_path):
                        os.mkdir(dbdd_export_path)

                    # build command list
                    cmd = [
                        "elasticdump",
                        "--input=http://%s:9200/%s" % (settings.ES_HOST, dbdd.es_index),
                        "--output=%(dbdd_export_path)s/dbdd%(dbdd_id)s.json" % {'dbdd_export_path':dbdd_export_path, 'dbdd_id':dbdd.id},
                        "--type=data",
                        "--ignore-errors",
                        "--noRefresh"
                    ]

                    LOGGER.debug("elasticdump cmd: %s", cmd)

                    # run cmd
                    os.system(" ".join(cmd))

                except Exception as err:
                    LOGGER.debug('could not export DBDD for job %s: %s', job, str(err))


        ############################
        # JOB RECORDS (Mongo)
        ############################

        # prepare records export dir
        record_exports_path = '%s/record_exports' % self.export_path
        os.mkdir(record_exports_path)

        # loop through jobs and export
        for job in self.export_dict['jobs']:

            # prepare command
            cmd = 'mongoexport --host %(mongo_host)s:27017 --db combine --collection record --out %(record_exports_path)s/j%(job_id)s_mongo_records.json --type=json -v --query \'{"job_id":%(job_id)s}\'' % {
                'job_id':job.id,
                'record_exports_path':record_exports_path,
                'mongo_host':settings.MONGO_HOST
            }

            LOGGER.debug("mongoexport cmd: %s", cmd)

            # run
            os.system(cmd)


        ############################
        # JOB VALIDATIONS (Mongo)
        ############################

        # prepare records export dir
        validation_exports_path = '%s/validation_exports' % self.export_path
        os.mkdir(validation_exports_path)

        # loop through jobs and export
        for job in self.export_dict['jobs']:

            # prepare command
            cmd = 'mongoexport --host %(mongo_host)s:27017 --db combine --collection record_validation --out %(validation_exports_path)s/j%(job_id)s_mongo_validations.json --type=json -v --query \'{"job_id":%(job_id)s}\'' % {
                'job_id':job.id,
                'validation_exports_path':validation_exports_path,
                'mongo_host':settings.MONGO_HOST
            }

            LOGGER.debug("mongoexport cmd: %s", cmd)

            # run
            os.system(cmd)


        ############################
        # JOB MAPPED FIELDS (ES)
        ############################
        '''
        Consider: elasticdump
        '''

        # prepare records export dir
        es_export_path = '%s/mapped_fields_exports' % self.export_path
        os.mkdir(es_export_path)

        # loop through jobs and export
        for job in self.export_dict['jobs']:

            # build command list
            cmd = [
                "elasticdump",
                "--input=http://%s:9200/j%s" % (settings.ES_HOST, job.id),
                "--output=%(es_export_path)s/j%(job_id)s_mapped_fields.json" % {'es_export_path':es_export_path, 'job_id':job.id},
                "--type=data",
                "--sourceOnly",
                "--ignore-errors",
                "--noRefresh"
            ]

            LOGGER.debug("elasticdump cmd: %s", cmd)

            # run cmd
            os.system(" ".join(cmd))


    def _sort_discrete_config_scenarios(self, config_scenarios):

        '''
        Method to sort and add configuration scenarios to list of model instances
        in self.export_dict for eventual serialization

        Sorting to these:
            'validations':set(),
            'transformations':set(),
            'oai_endpoints':set(),
            'rits':set(),
            'field_mapper_configs':set(),
            'dbdd':set()

        Args:
            config_scenarios (list): List of model instances, or specially prefixed ids

        '''

        LOGGER.debug('sorting passed discrete configuration scenarios')

        # establish sort hash
        sorting_hash = {
            ValidationScenario:'validations',
            Transformation:'transformations',
            OAIEndpoint:'oai_endpoints',
            RecordIdentifierTransformationScenario:'rits',
            FieldMapper:'field_mapper_configs',
            DPLABulkDataDownload:'dbdd'
        }

        # invert sorting_hash for id prefixes
        id_prefix_hash = {v:k for k, v in sorting_hash.items()}

        # loop through passed model instances
        for config_scenario in config_scenarios:

            LOGGER.debug('adding to export_dict for serialization: %s', config_scenario)

            # if prefixed string passed, retrieve model instance
            if isinstance(config_scenario, str) and '|' in config_scenario:
                model_type, model_id = config_scenario.split('|')
                config_scenario = id_prefix_hash[model_type].objects.get(pk=int(model_id))

            # slot to export dict through hash
            self.export_dict[sorting_hash[type(config_scenario)]].add(config_scenario)


    def package_export(self):

        '''
        Method to serialize model instances, and combine with serializations already on disk
        '''

        # serialize Django model instances
        with open('%s/django_objects.json' % self.export_path, 'w') as out_file:

            # combine all model instances, across model types
            to_serialize = []
            for _key, val in self.export_dict.items():
                to_serialize.extend(val)

            # write as single JSON file
            out_file.write(serializers.serialize('json', to_serialize))

        # finalize export_manifest
        self._finalize_export_manifest()

        # write export_manifest
        with open('%s/export_manifest.json' % self.export_path, 'w') as out_file:
            out_file.write(json.dumps(self.export_manifest))

        # if compressing, zip up directory, and remove originals after archive created
        if self.compress:

            LOGGER.debug("compressiong exported state at %s", self.export_path)

            # establish output archive file
            export_filename = '%s/%s' % (settings.STATEIO_EXPORT_DIR, self.export_manifest['export_id'])

            # use shutil to zip up
            compress_stime = time.time()
            new_export_path = shutil.make_archive(
                export_filename,
                self.compression_format,
                settings.STATEIO_EXPORT_DIR,
                self.export_manifest['export_id']
            )
            LOGGER.debug('archive %s created in %ss', new_export_path, (time.time() - compress_stime))

            # remove originals
            shutil.rmtree(self.export_path)

            # update export path and manifeset
            self.export_path = new_export_path
            self.export_manifest['export_path'] = self.export_path

        # return export_path
        return self.export_path


    def _finalize_export_manifest(self):

        '''
        Method to finalize export_manifest before writing to export
            - loop through self.export_dict for export types that are human meaningful
        '''

        # establish section in export_manifest
        self.export_manifest['exports'] = {
            'jobs':[],
            'record_groups':[],
            'orgs':[],
            'dbdd':[],
            'oai_endpoints':[],
            'rits':[],
            'transformations':[],
            'validations':[],
        }

        # loop through export Django model instance exports
        export_count = 0
        for export_type in self.export_manifest['exports'].keys():

            # loop through exports for type
            for export in self.export_dict[export_type]:

                LOGGER.debug('writing %s to export_manifest', export)

                # bump counter
                export_count += 1

                # write
                self.export_manifest['exports'][export_type].append({
                    'name':export.name,
                    'id':export.id
                })

        # write Published Subests to export manifest
        self._collect_published_subsets()

        # write count to exports
        self.export_manifest['exports']['count'] = export_count


    def _collect_published_subsets(self):

        '''
        Method to include associated Published Subsets with export
        '''

        self.export_manifest['published_subsets'] = []

        # add all subsets if include non-set Records AND > 1 Jobs do not have publish_set_id
        non_publish_set_jobs = [job for job in self.export_dict['jobs'] if job.publish_set_id == '']
        non_publish_set_subsets = [subset for subset in PublishedRecords.get_subsets() if subset['include_non_set_records']]
        if len(non_publish_set_jobs) > 0 and len(non_publish_set_subsets) > 0:
            self.export_manifest['published_subsets'].extend(non_publish_set_subsets)

        # loop through Jobs and add Published Subsets if publish_set_id is included in any
        for job in self.export_dict['jobs']:

            # if Job published and has publish_set_ids, update export_dict set
            if job.published and job.publish_set_id != '':
                self.export_manifest['published_subsets'].extend(PublishedRecords.get_subsets(includes_publish_set_id=job.publish_set_id))

        # finally, dedupe published_subsets and remove IDs
        sanitized_subsets = []
        for subset in self.export_manifest['published_subsets']:
            subset.pop('_id')
            if subset not in sanitized_subsets:
                sanitized_subsets.append(subset)
        self.export_manifest['published_subsets'] = sanitized_subsets


    def import_state(
            self,
            export_path,
            import_name=None,
            load_only=False,
            import_records=True,
            stateio_id=None
        ):

        '''
        Import exported state

        Args:
            export_path (str): location on disk of unzipped export directory
            import_name (str): Human name for import task
            load_only (bool): If True, will only parse export but will not import anything
            import_records (bool): If True, will import Mongo and ElasticSearch records
            stateio_id (int): ID of pre-existing StateIO object

        Returns:


        '''

        #debug
        import_stime = time.time()

        # mint new import id
        self.import_id = uuid.uuid4().hex

        # init import_manifest
        self.import_manifest = {
            'combine_version':getattr(settings, 'COMBINE_VERSION', None),
            'import_id':self.import_id,
            'import_name':import_name,
            'export_path':export_path,
            'pk_hash':{
                'jobs':{},
                'record_groups':{},
                'orgs':{},
                'transformations':{},
                'validations':{},
                'oai_endpoints':{},
                'rits':{},
                'field_mapper_configs':{},
                'dbdd':{}
            }
        }

        # init/update associated StateIO instance
        update_dict = {
            'stateio_type':'import',
            'name':self.import_manifest['import_name'],
            'import_id':self.import_manifest['import_id'],
            'export_path':self.import_manifest['export_path'],
            'import_manifest':{k:v for k, v in self.import_manifest.items() if k not in ['pk_hash', 'export_manifest']},
            'status':'running'
        }
        if stateio_id == None:
            LOGGER.debug('initializing StateIO object')
            self.stateio = StateIO(**update_dict)
        else:
            LOGGER.debug('retrieving and updating StateIO object')
            self.stateio = StateIO.objects.get(id=stateio_id)
            self.stateio.update(**update_dict)
            self.stateio.reload()
        # save
        self.stateio.save()

        # load state, deserializing and export_manifest
        self._load_state(export_path)

        # if not load only, continue
        if not load_only:

            # load configuration dependencies
            self._import_config_instances()

            # if Jobs present
            if len(self.export_manifest['jobs']) > 0:

                # load structural hierarchy
                self._import_hierarchy()

                # load model instances
                self._import_job_related_instances()

                if import_records:
                    # load Mongo and ES DB records
                    self._import_db_records()

                # import published subsets
                self._import_published_subsets()

            # update import_manifest
            self._finalize_import_manifest()

        # update associated StateIO instance
        self.stateio.update(**{
            'export_path':self.export_path,
            'export_manifest':self.export_manifest,
            'import_manifest':{k:v for k, v in self.import_manifest.items() if k not in ['pk_hash', 'export_manifest']},
            'import_path':self.import_path,
            'status':'finished',
            'finished':True
        })
        self.stateio.reload()
        self.stateio.save()

        LOGGER.debug('state %s imported in %ss', self.import_id, (time.time()-import_stime))


    def _load_state(self, export_path):

        '''
        Method to load state data in preparation for import

        Args:
            export_path (str): location on disk of unzipped export directory

        Returns:
            None
        '''

        # set export path
        self.export_path = export_path

        # unpack if compressed, move to imports
        self._prepare_files()

        # parse export_manifest
        with open('%s/export_manifest.json' % self.import_path, 'r') as in_file:
            self.export_manifest = json.loads(in_file.read())
            self.import_manifest['export_manifest'] = self.export_manifest

        # deserialize django objects
        self.deser_django_objects = []
        with open('%s/django_objects.json' % self.import_path, 'r') as in_file:
            django_objects_json = in_file.read()
        for obj in serializers.deserialize('json', django_objects_json):
            self.deser_django_objects.append(obj)


    def _prepare_files(self):

        '''
        Method to handle unpacking of exported state, and prepare self.import_path
        '''

        # create import dir based on self.import_id
        self.import_path = '%s/%s' % (settings.STATEIO_IMPORT_DIR, self.import_id)
        self.import_manifest['import_path'] = self.import_path

        # handle URL
        if self.export_path.startswith('http'):
            LOGGER.debug('exported state determined to be URL, downloading and processing')
            raise Exception('not yet handling remote URL locations for importing states')

        # handle archives
        if os.path.isfile(self.export_path) and self.export_path.endswith(('.zip', '.tar', '.tar.gz')):
            LOGGER.debug('exported state determined to be archive, decompressing')
            shutil.unpack_archive(self.export_path, self.import_path)

        # handle dir
        elif os.path.isdir(self.export_path):
            LOGGER.debug('exported state is directory, copying to import directory')
            os.system('cp -r %s %s' % (self.export_path, self.import_path))

        # else, raise Exception
        else:
            raise Exception('cannot handle export_path %s, aborting' % self.export_path)

        # next, look for export_manifest.json, indicating base directory
        import_base_dir = False
        for root, _dirs, files in os.walk(self.import_path):
            if 'export_manifest.json' in files:
                import_base_dir = root
        # if not found, raise Exception
        if not import_base_dir:
            raise Exception('could not find export_manfiest.json, aborting')

        # if import_base_dir != self.import_path, move everything to self.import_path
        if import_base_dir != self.import_path:

            # mv everything to import dir
            os.system('mv %s/* %s' % (import_base_dir, self.import_path))

            # remove now empty base_dir
            shutil.rmtree(import_base_dir)

        LOGGER.debug('confirmed import path at %s', self.import_path)


    def _import_config_instances(self):

        '''
        Method to import configurations and scenarios instances before all else
        '''

        #################################
        # TRANSFORMATION SCENARIOS
        #################################
        # loop through and create
        for transformation in self._get_django_model_type(Transformation):
            LOGGER.debug('rehydrating %s', transformation)

            # check for identical name and payload
            ts_match = Transformation.objects.filter(name=transformation.object.name, payload=transformation.object.payload).order_by('id')

            # matching scenario found
            if ts_match.count() > 0:
                LOGGER.debug('found identical Transformation, skipping creation, adding to hash')
                self.import_manifest['pk_hash']['transformations'][transformation.object.id] = ts_match.first().id

            # not found, creating
            else:
                LOGGER.debug('Transformation not found, creating')
                ts_orig_id = transformation.object.id
                transformation.object.id = None
                transformation.save()
                self.import_manifest['pk_hash']['transformations'][ts_orig_id] = transformation.object.id


        #################################
        # VALIDATION SCENARIOS
        #################################
        # loop through and create
        for validation_scenario in self._get_django_model_type(ValidationScenario):
            LOGGER.debug('rehydrating %s', validation_scenario)

            # check for identical name and payload
            vs_match = ValidationScenario.objects.filter(name=validation_scenario.object.name, payload=validation_scenario.object.payload).order_by('id')

            # matching scenario found
            if vs_match.count() > 0:
                LOGGER.debug('found identical ValidationScenario, skipping creation, adding to hash')
                self.import_manifest['pk_hash']['validations'][validation_scenario.object.id] = vs_match.first().id

            # not found, creating
            else:
                LOGGER.debug('ValidationScenario not found, creating')
                vs_orig_id = validation_scenario.object.id
                validation_scenario.object.id = None
                validation_scenario.save()
                self.import_manifest['pk_hash']['validations'][vs_orig_id] = validation_scenario.object.id


        #################################
        # OAI ENDPOINTS
        #################################

        # loop through and create
        for oai in self._get_django_model_type(OAIEndpoint):
            LOGGER.debug('rehydrating %s', oai)

            # check for identical name and payload
            oai_match = OAIEndpoint.objects.filter(name=oai.object.name, endpoint=oai.object.endpoint).order_by('id')

            # matching scenario found
            if oai_match.count() > 0:
                LOGGER.debug('found identical OAIEndpoint, skipping creation, adding to hash')
                self.import_manifest['pk_hash']['oai_endpoints'][oai.object.id] = oai_match.first().id

            # not found, creating
            else:
                LOGGER.debug('OAIEndpoint not found, creating')
                oai_orig_id = oai.object.id
                oai.object.id = None
                oai.save()
                self.import_manifest['pk_hash']['oai_endpoints'][oai_orig_id] = oai.object.id


        #################################
        # FIELD MAPPER CONFIGS
        #################################

        # loop through and create
        for scenario in self._get_django_model_type(FieldMapper):
            LOGGER.debug('rehydrating %s', scenario)

            # check for identical name and payload
            scenario_match = FieldMapper.objects.filter(
                name=scenario.object.name,
                payload=scenario.object.payload,
                config_json=scenario.object.config_json,
                field_mapper_type=scenario.object.field_mapper_type
            ).order_by('id')

            # matching scenario found
            if scenario_match.count() > 0:
                LOGGER.debug('found identical FieldMapper, skipping creation, adding to hash')
                self.import_manifest['pk_hash']['field_mapper_configs'][scenario.object.id] = scenario_match.first().id

            # not found, creating
            else:
                LOGGER.debug('FieldMapper not found, creating')
                orig_id = scenario.object.id
                scenario.object.id = None
                scenario.save()
                self.import_manifest['pk_hash']['field_mapper_configs'][orig_id] = scenario.object.id


        #################################
        # RITS
        #################################

        # loop through and create
        for scenario in self._get_django_model_type(RecordIdentifierTransformationScenario):
            LOGGER.debug('rehydrating %s', scenario)

            # check for identical name and payload
            scenario_match = RecordIdentifierTransformationScenario.objects.filter(
                name=scenario.object.name,
                transformation_type=scenario.object.transformation_type,
                transformation_target=scenario.object.transformation_target,
                regex_match_payload=scenario.object.regex_match_payload,
                regex_replace_payload=scenario.object.regex_replace_payload,
                python_payload=scenario.object.python_payload,
                xpath_payload=scenario.object.xpath_payload,
            ).order_by('id')

            # matching scenario found
            if scenario_match.count() > 0:
                LOGGER.debug('found identical RITS, skipping creation, adding to hash')
                self.import_manifest['pk_hash']['rits'][scenario.object.id] = scenario_match.first().id

            # not found, creating
            else:
                LOGGER.debug('RITS not found, creating')
                orig_id = scenario.object.id
                scenario.object.id = None
                scenario.save()
                self.import_manifest['pk_hash']['rits'][orig_id] = scenario.object.id


        ###########################################
        # DBDD
        ###########################################

        '''
        Requires import to ElasticSearch
            - but because not modifying, can use elasticdump to import
        '''

        # loop through and create
        for scenario in self._get_django_model_type(DPLABulkDataDownload):
            LOGGER.debug('rehydrating %s', scenario)

            # check for identical name and payload
            scenario_match = DPLABulkDataDownload.objects.filter(
                s3_key=scenario.object.s3_key,
                es_index=scenario.object.es_index,
            ).order_by('id')

            # matching scenario found
            if scenario_match.count() > 0:
                LOGGER.debug('found identical DPLA Bulk Data Download, skipping creation, adding to hash')
                self.import_manifest['pk_hash']['dbdd'][scenario.object.id] = scenario_match.first().id

            # not found, creating
            else:
                LOGGER.debug('DPLA Bulk Data Download not found, creating')
                orig_id = scenario.object.id
                scenario.object.id = None
                # drop filepath
                scenario.object.filepath = None
                scenario.save()
                self.import_manifest['pk_hash']['dbdd'][orig_id] = scenario.object.id

                # re-hydrating es index

                # get dbdd
                dbdd = DPLABulkDataDownload.objects.get(pk=(scenario.object.id))

                # import DBDD index to ElaticSearch
                dbdd_export_path = '%s/dbdd/dbdd%s.json' % (self.import_path, orig_id)

                # build command list
                cmd = [
                    "elasticdump",
                    "--input=%(dbdd_export_path)s" % {'dbdd_export_path':dbdd_export_path},
                    "--output=http://localhost:9200/%s" % dbdd.es_index,
                    "--ignore-errors",
                    "--noRefresh"
                ]

                LOGGER.debug("elasticdump cmd: %s", cmd)

                # run cmd
                os.system(" ".join(cmd))


    def _import_hierarchy(self):

        '''
        Import Organizations and Record Groups

        Note: If same name is found, but duplicates exist, will be aligned with first instance
        sorted by id
        '''

        # loop through deserialized Organizations
        for org in self._get_django_model_type(Organization):
            LOGGER.debug('rehydrating %s', org)

            # get org_orig_id
            org_orig_id = org.object.id

            # check Org name
            org_match = Organization.objects.filter(name=org.object.name).order_by('id')

            # matching Organization found
            if org_match.count() > 0:
                LOGGER.debug('found same Organization name, skipping creation, adding to hash')
                self.import_manifest['pk_hash']['orgs'][org.object.id] = org_match.first().id

            # not found, creating
            else:
                LOGGER.debug('Organization not found, creating')
                org.object.id = None
                org.save()
                self.import_manifest['pk_hash']['orgs'][org_orig_id] = org.object.id


            # loop through deserialized Record Groups for this Org
            for record_group in self._get_django_model_type(RecordGroup):

                if record_group.object.organization_id == org_orig_id:

                    LOGGER.debug('rehydrating %s', record_group)

                    # checking parent org exists, and contains record group with same name
                    rg_match = RecordGroup.objects\
                        .filter(name=record_group.object.name, organization__name=self._get_django_model_instance(org.object.id, Organization).object.name).order_by('id')

                    # matching Record Group found
                    if rg_match.count() > 0:
                        LOGGER.debug('found Organization/Record Group name combination, skipping creation, adding to hash')
                        self.import_manifest['pk_hash']['record_groups'][record_group.object.id] = rg_match.first().id

                    # not found, creating
                    else:
                        LOGGER.debug('Record Group not found, creating')
                        rg_orig_id = record_group.object.id
                        record_group.object.id = None
                        # update org id
                        org_orig_id = record_group.object.organization_id
                        record_group.object.organization = None
                        record_group.object.organization_id = self.import_manifest['pk_hash']['orgs'][org_orig_id]
                        record_group.save()
                        self.import_manifest['pk_hash']['record_groups'][rg_orig_id] = record_group.object.id


    def _import_job_related_instances(self):

        '''
        Method to import Organization, Record Group, and Job model instances
        '''

        ############################
        # DJANGO OBJECTS: JOBS
        ############################
        # loop through ORDERED job ids, and rehydrate, capturing new PK in pk_hash
        for job_id in self.export_manifest['jobs']:

            # get deserialized Job
            job = self._get_django_model_instance(job_id, Job)
            LOGGER.debug('rehydrating %s', job.object.name)

            # run record_group id through pk_hash
            rg_orig_id = job.object.record_group_id
            job.object.record_group_id = self.import_manifest['pk_hash']['record_groups'][rg_orig_id]

            # drop pk, save, and note new id
            job_orig_id = job.object.id
            job.object.id = None
            job.save()

            # update pk_hash
            self.import_manifest['pk_hash']['jobs'][job_orig_id] = job.object.id

            # Job, update job_details
            self._update_job_details(job.object)

            # update any Published pre-counts this Job may affect
            job.object.remove_from_published_precounts()


        #############################
        # DJANGO OBJECTS: JOB INPUTS
        #############################
        # loop through and create
        for job_input in self._get_django_model_type(JobInput):
            LOGGER.debug('rehydrating %s', job_input)
            job_input.object.id = None
            job_input.object.job_id = self.import_manifest['pk_hash']['jobs'][job_input.object.job_id]
            job_input.object.input_job_id = self.import_manifest['pk_hash']['jobs'][job_input.object.input_job_id]
            job_input.save()


        #################################
        # DJANGO OBJECTS: JOB VALIDATIONS
        #################################
        # loop through and create
        for job_validation in self._get_django_model_type(JobValidation):
            LOGGER.debug('rehydrating %s', job_validation)

            # update validation_scenario_id
            vs_orig_id = job_validation.object.validation_scenario_id
            job_validation.object.validation_scenario_id = self.import_manifest['pk_hash']['validations'][vs_orig_id]

            # update job_id
            job_validation.object.id = None
            job_validation.object.job_id = self.import_manifest['pk_hash']['jobs'][job_validation.object.job_id]
            job_validation.save()


    def _import_db_records(self):

        '''
        Method to import DB records from:
            - Mongo
            - ElasticSearch
        '''

        # generate spark code
        self.spark_code = 'from jobs import CombineStateIOImport\nCombineStateIOImport(spark, import_path="%(import_path)s", import_manifest=%(import_manifest)s).spark_function()' % {
            'import_path':'file://%s' % self.import_path,
            'import_manifest':self.import_manifest
        }

        # submit to livy and poll
        submit = LivyClient().submit_job(LivySession.get_active_session().session_id, {'code':self.spark_code})
        self.spark_results = polling.poll(lambda: LivyClient().job_status(submit.headers['Location']).json()['state'] == 'available', step=5, poll_forever=True)

        return self.spark_results


    def _import_published_subsets(self):

        '''
        Method to import Published Subsets from export_manifest
        '''

        for subset in self.export_manifest['published_subsets']:

            LOGGER.debug('CHECKING ON SUBSET: %s', subset)

            # check to see if subset exists
            subset_q = mc_handle.combine.misc.find_one({'type':'published_subset', 'name':subset['name']})
            LOGGER.debug(subset_q)

            # does not exist, create
            if subset_q == None:
                LOGGER.debug('Published Subset %s not found, creating', subset['name'])

                # insert subset to Mongo
                mc_handle.combine.misc.insert_one(subset)

                # remove pre-count
                mc_handle.combine.misc.delete_one({'_id':'published_field_counts_%s' % subset['name']})

            # if found, check if identical
            elif isinstance(subset_q, dict):

                # pop id
                subset_q.pop('_id')

                # check if identical
                if subset == subset_q:
                    LOGGER.debug('found identical Published Subset %s, skipping import', subset['name'])

                # if different, rename and import
                else:
                    LOGGER.debug('found subset with same name, but different properties, creating new')
                    subset['name'] = '%s_stateio_import_created' % subset['name']

                    # insert subset to Mongo
                    mc_handle.combine.misc.insert_one(subset)

                    # remove pre-count
                    mc_handle.combine.misc.delete_one({'_id':'published_field_counts_%s' % subset['name']})


    def _finalize_import_manifest(self):

        '''
        Method to finalize import_manifest before writing to export
            - loop through self.export_dict for export types that are human meaningful
        '''

        # establish section in export_manifest
        self.import_manifest['imports'] = {
            'jobs':[],
            'record_groups':[],
            'orgs':[],
            'dbdd':[],
            'oai_endpoints':[],
            'rits':[],
            'transformations':[],
            'validations':[]
        }

        # loop through deserialized objects
        import_count = 0
        for import_type in self.import_manifest['imports'].keys():

            # invert pk_hash for type
            inv_pk_hash = {v:k for k, v in self.import_manifest['pk_hash'][import_type].items()}

            # loop through imports for type
            for obj in self._get_django_model_type(self.model_translation[import_type]):

                # confirm that id has changed, indicating newly created and not mapped from pre-existing
                if obj.object.id in inv_pk_hash.keys() and obj.object.id != inv_pk_hash[obj.object.id]:

                    LOGGER.debug('writing %s to import_manifest', obj)

                    # bump count
                    import_count += 1

                    # write name, and UPDATED id of imported object
                    self.import_manifest['imports'][import_type].append({
                        'name':obj.object.name,
                        'id':obj.object.id
                    })

        # calc total number of imports
        self.import_manifest['imports']['count'] = import_count

        # write pk_hash as json string (int keys are not valid in Mongo)
        self.import_manifest['pk_hash_json'] = json.dumps(self.import_manifest['pk_hash'])


    def _update_job_details(self, job):

        '''
        Handle the various ways in which Job.job_details must
        be updated in light of changing model instance PKs.

            - Generic
                - validation_scenarios: list of integers
                - input_job_ids: list of integers
                - input_filters.job_specific: dictionary with string of job_id as key

            - OAI Harvest
                - oai_params

            - Transformation
                - transformation

        Args:
            job (core.models.Job): newly minted Job instance to update

        Returns:
            None
        '''

        LOGGER.debug('updating job_details for %s', job)

        # pk_hash
        pk_hash = self.import_manifest['pk_hash']

        # update dictionary
        update_dict = {}

        # update validation_scenarios
        if 'validation_scenarios' in job.job_details_dict.keys():
            try:
                validation_scenarios = job.job_details_dict['validation_scenarios']
                validation_scenarios_updated = [pk_hash['validations'].get(vs_id, vs_id) for vs_id in validation_scenarios]
                update_dict['validation_scenarios'] = validation_scenarios_updated
            except:
                LOGGER.debug('error with updating job_details: validation_scenarios')

        # update input_job_ids
        if 'input_job_ids' in job.job_details_dict.keys():
            try:
                input_job_ids = job.job_details_dict['input_job_ids']
                input_job_ids_updated = [pk_hash['jobs'].get(job_id, job_id) for job_id in input_job_ids]
                update_dict['input_job_ids'] = input_job_ids_updated
            except:
                LOGGER.debug('error with updating job_details: input_job_ids')

        # update input_filters.job_specific
        if 'input_filters' in job.job_details_dict.keys():
            try:
                input_filters = job.job_details_dict['input_filters']
                job_specific_updated = {str(pk_hash['jobs'].get(int(k), int(k))):v for k, v in input_filters['job_specific'].items()}
                input_filters['job_specific'] = job_specific_updated
                update_dict['input_filters'] = input_filters
            except:
                LOGGER.debug('error with updating job_details: input_filters')

        # update rits
        if 'rits' in job.job_details_dict.keys():
            try:
                orig_rits_id = job.job_details_dict['rits']
                update_dict['rits'] = pk_hash['rits'][orig_rits_id]
            except:
                LOGGER.debug('error with updating job_details: rits')

        # update dbdd
        if 'dbdm' in job.job_details_dict.keys():
            try:
                dbdm = job.job_details_dict['dbdm']
                dbdd_orig_id = dbdm['dbdd']
                dbdm['dbdd'] = pk_hash['dbdd'][dbdd_orig_id]
                update_dict['dbdm'] = dbdm
            except:
                LOGGER.debug('error with updating job_details: dbdd')

        # if OAIHarvest, update oai_params
        if job.job_type == 'HarvestOAIJob':
            try:
                LOGGER.debug('HarvestOAIJob encountered, updating job details')
                oai_params = job.job_details_dict['oai_params']
                oai_params['id'] = pk_hash['oai_endpoints'].get(oai_params['id'], oai_params['id'])
                update_dict['oai_params'] = oai_params
            except:
                LOGGER.debug('error with updating job_details: oai_params')

        # if Transform, update transformation
        if job.job_type == 'TransformJob':
            try:
                LOGGER.debug('TransformJob encountered, updating job details')
                transformation = job.job_details_dict['transformation']
                transformation['id'] = pk_hash['transformations'].get(transformation['id'], transformation['id'])
                update_dict['transformation'] = transformation
            except:
                LOGGER.debug('error with updating job_details: transformation')

        # update job details
        job.update_job_details(update_dict)

        # update spark code
        cjob = CombineJob.get_combine_job(job.id)
        cjob.job.spark_code = cjob.prepare_job(return_job_code=True)
        cjob.job.save()


    def _get_django_model_instance(self, instance_id, instance_type, instances=None):

        '''
        Method to retrieve model instance from deserialized "bag",
        by instance type and id.

        Could be more performant, but length of deserialized objects
        makes it relatively negligable.

        Args:
            instance_id (int): model instance PK
            intsance_type (django.models.model): model instance type

        Returns:
            (core.models.model): Model instance based on matching id and type
        '''

        # if instances not provided, assumed self.deser_django_objects
        if instances == None:
            instances = self.deser_django_objects

        # loop through deserialized objects
        for obj in instances:
            if obj.object.id == instance_id and isinstance(obj.object, instance_type):
                LOGGER.debug('deserialized object found: %s', obj.object)
                return obj


    def _get_django_model_type(self, instance_type, instances=None):

        '''
        Method to retrieve types from deserialized objects

        Args:
            instance_type (core.models.model): Model type to return

        Return:
            (list): List of model instances that match type
        '''

        # if instances not provided, assumed self.deser_django_objects
        if instances == None:
            instances = self.deser_django_objects

        # return list
        return [obj for obj in instances if isinstance(obj.object, instance_type)]


    @staticmethod
    def export_state_bg_task(
            export_name=None,
            jobs=[],
            record_groups=[],
            orgs=[],
            config_scenarios=[]
        ):

        '''
        Method to init export state as bg task
        '''

        # init StateIO
        stateio = StateIO(
            name=export_name,
            stateio_type='export',
            status='initializing')
        stateio.save()

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Export State',
            task_type='stateio_export',
            task_params_json=json.dumps({
                'jobs':jobs,
                'record_groups':record_groups,
                'orgs':orgs,
                'config_scenarios':config_scenarios,
                'export_name':stateio.name,
                'stateio_id':str(stateio.id),
            })
        )
        combine_task.save()
        LOGGER.debug(combine_task)

        # add combine_task.id to stateio
        stateio.bg_task_id = combine_task.id
        stateio.save()

        # run celery task
        bg_task = tasks.stateio_export.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task


    @staticmethod
    def import_state_bg_task(
            import_name=None,
            export_path=None
        ):

        '''
        Method to init state import as bg task
        '''

        # init StateIO
        stateio = StateIO(
            name=import_name,
            stateio_type='import',
            status='initializing')
        stateio.save()

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Import State',
            task_type='stateio_import',
            task_params_json=json.dumps({
                'import_name':stateio.name,
                'export_path':export_path,
                'stateio_id':str(stateio.id),
            })
        )
        combine_task.save()
        LOGGER.debug(combine_task)

        # add combine_task.id to stateio
        stateio.bg_task_id = combine_task.id
        stateio.save()

        # run celery task
        bg_task = tasks.stateio_import.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

        return combine_task
