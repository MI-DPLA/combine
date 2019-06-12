# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import logging
import os
import shutil
import uuid

# django imports
from django.conf import settings
from django.contrib.auth import signals
from django.db import models
from django.dispatch import receiver

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)

from core.es import es_handle
from core.mongo import mongoengine
from core.models.configurations import Transformation, ValidationScenario, DPLABulkDataDownload
from core.models.job import Job, JobValidation
from core.models.livy_spark import LivySession
from core.models.organization import Organization
from core.models.record_group import RecordGroup
from core.models.stateio import StateIO
from core.models.tasks import CombineBackgroundTask



@receiver(signals.user_logged_in)
def user_login_handle_livy_sessions(sender, user, **kwargs):

    '''
    When user logs in, handle check for pre-existing sessions or creating

    Args:
        sender (auth.models.User): class
        user (auth.models.User): instance
        kwargs: not used
    '''

    # if superuser, skip
    if user.is_superuser:
        LOGGER.debug("superuser detected, not initiating Livy session")
        return False

    # else, continune with user sessions
    LOGGER.debug('Checking for pre-existing livy sessions')

    # get "active" user sessions
    livy_sessions = LivySession.objects.filter(status__in=['starting', 'running', 'idle'])
    LOGGER.debug(livy_sessions)

    # none found
    if livy_sessions.count() == 0:
        LOGGER.debug('no Livy sessions found, creating')
        livy_session = LivySession()
        livy_session.start_session()

    # if sessions present
    elif livy_sessions.count() == 1:
        LOGGER.debug('single, active Livy session found, using')

    elif livy_sessions.count() > 1:
        LOGGER.debug('multiple Livy sessions found, sending to sessions page to select one')


@receiver(models.signals.pre_delete, sender=Organization)
def delete_org_pre_delete(sender, instance, **kwargs):

    # mark child record groups as deleted
    LOGGER.debug('marking all child Record Groups as deleting')
    for record_group in instance.recordgroup_set.all():

        record_group.name = "%s (DELETING)" % record_group.name
        record_group.save()

        # mark child jobs as deleted
        LOGGER.debug('marking all child Jobs as deleting')
        for job in record_group.job_set.all():

            job.name = "%s (DELETING)" % job.name
            job.deleted = True
            job.status = 'deleting'
            job.save()


@receiver(models.signals.pre_delete, sender=RecordGroup)
def delete_record_group_pre_delete(sender, instance, **kwargs):

    # mark child jobs as deleted
    LOGGER.debug('marking all child Jobs as deleting')
    for job in instance.job_set.all():

        job.name = "%s (DELETING)" % job.name
        job.deleted = True
        job.status = 'deleting'
        job.save()


@receiver(models.signals.post_save, sender=Job)
def save_job_post_save(sender, instance, created, **kwargs):

    '''
    After job is saved, update job output

    Args:
        sender (auth.models.Job): class
        user (auth.models.Job): instance
        created (bool): indicates if newly created, or just save/update
        kwargs: not used
    '''

    # if the record was just created, then update job output (ensures this only runs once)
    if created and instance.job_type != 'AnalysisJob':

        # set output based on job type
        LOGGER.debug('setting job output for job')
        instance.job_output = '%s/organizations/%s/record_group/%s/jobs/%s/%s' % (
            settings.BINARY_STORAGE.rstrip('/'),
            instance.record_group.organization.id,
            instance.record_group.id,
            instance.job_type,
            instance.id)
        instance.save()


@receiver(models.signals.pre_delete, sender=Job)
def delete_job_pre_delete(sender, instance, **kwargs):

    '''
    When jobs are removed, some actions are performed:
        - if job is queued or running, stop
        - remove avro files from disk
        - delete ES indexes (if present)
        - delete from Mongo

    Args:
        sender (auth.models.Job): class
        user (auth.models.Job): instance
        kwargs: not used
    '''

    LOGGER.debug('pre-delete sequence for Job: #%s', instance.id)

    # stop Job
    instance.stop_job()

    # remove avro files from disk
    if instance.job_output and instance.job_output.startswith('file://'):

        try:
            output_dir = instance.job_output.split('file://')[-1]
            shutil.rmtree(output_dir)

        except:
            LOGGER.debug('could not remove job output directory at: %s', instance.job_output)

    # remove ES index if exists
    instance.drop_es_index()

    # remove Records from Mongo
    instance.remove_records_from_db()

    # remove Validations from Mongo
    instance.remove_validations_from_db()

    # remove Validations from Mongo
    instance.remove_mapping_failures_from_db()

    # remove Job as input for other Jobs
    instance.remove_as_input_job()

    # if Job published, remove pre-counts where necessary
    instance.remove_from_published_precounts()

    # remove any temporary files
    instance.remove_temporary_files()


@receiver(models.signals.pre_delete, sender=JobValidation)
def delete_job_validation_pre_delete(sender, instance, **kwargs):

    '''
    Signal to remove RecordValidations from DB if JobValidation removed
    '''

    _ = instance.delete_record_validation_failures()


@receiver(models.signals.post_delete, sender=Job)
def delete_job_post_delete(sender, instance, **kwargs):

    LOGGER.debug('job %s was deleted successfully', instance)


@receiver(models.signals.pre_save, sender=Transformation)
def save_transformation_to_disk(sender, instance, **kwargs):

    '''
    Pre-save work for Transformations

    Args:
        sender (auth.models.Transformation): class
        user (auth.models.Transformation): instance
        kwargs: not used
    '''

    # check that transformation directory exists
    transformations_dir = '%s/transformations' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]
    if not os.path.exists(transformations_dir):
        os.mkdir(transformations_dir)

    # if previously written to disk, remove
    if instance.filepath:
        try:
            os.remove(instance.filepath)
        except:
            LOGGER.debug('could not remove transformation file: %s', instance.filepath)

    # fire transformation method to rewrite external HTTP includes for XSLT
    if instance.transformation_type == 'xslt':
        instance._rewrite_xsl_http_includes()

    # write XSLT type transformation to disk
    if instance.transformation_type == 'xslt':
        filename = uuid.uuid4().hex

        filepath = '%s/%s.xsl' % (transformations_dir, filename)
        with open(filepath, 'w') as outfile:
            outfile.write(instance.payload)

        # update filepath
        instance.filepath = filepath


@receiver(models.signals.pre_save, sender=ValidationScenario)
def save_validation_scenario_to_disk(sender, instance, **kwargs):

    '''
    When users enter a payload for a validation scenario, write to disk for use in Spark context

    Args:
        sender (auth.models.ValidationScenario): class
        user (auth.models.ValidationScenario): instance
        kwargs: not used
    '''

    # check that transformation directory exists
    validations_dir = '%s/validation' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]
    if not os.path.exists(validations_dir):
        os.mkdir(validations_dir)

    # if previously written to disk, remove
    if instance.filepath:
        try:
            os.remove(instance.filepath)
        except:
            LOGGER.debug('could not remove validation scenario file: %s', instance.filepath)

    # write Schematron type validation to disk
    if instance.validation_type == 'sch':
        filename = 'file_%s.sch' % uuid.uuid4().hex
    if instance.validation_type == 'python':
        filename = 'file_%s.py' % uuid.uuid4().hex
    if instance.validation_type == 'es_query':
        filename = 'file_%s.json' % uuid.uuid4().hex
    if instance.validation_type == 'xsd':
        filename = 'file_%s.xsd' % uuid.uuid4().hex

    filepath = '%s/%s' % (validations_dir, filename)
    with open(filepath, 'w') as out_file:
        out_file.write(instance.payload)

    # update filepath
    instance.filepath = filepath


@receiver(models.signals.pre_delete, sender=DPLABulkDataDownload)
def delete_dbdd_pre_delete(sender, instance, **kwargs):

    # remove download from disk
    if os.path.exists(instance.filepath):
        LOGGER.debug('removing %s from disk', instance.filepath)
        os.remove(instance.filepath)

    # remove ES index if exists
    try:
        if es_handle.indices.exists(instance.es_index):
            LOGGER.debug('removing ES index: %s', instance.es_index)
            es_handle.indices.delete(instance.es_index)
    except:
        LOGGER.debug('could not remove ES index: %s', instance.es_index)


@receiver(models.signals.post_init, sender=CombineBackgroundTask)
def background_task_post_init(sender, instance, **kwargs):

    # if exists already, update status
    if instance.id:
        instance.update()


@receiver(models.signals.pre_delete, sender=CombineBackgroundTask)
def background_task_pre_delete_django_tasks(sender, instance, **kwargs):

    # if export dir exists in task_output, delete as well
    if instance.task_output != {} and 'export_dir' in instance.task_output.keys():
        try:
            LOGGER.debug('removing task export dir: %s', instance.task_output['export_dir'])
            shutil.rmtree(instance.task_output['export_dir'])
        except:
            LOGGER.debug('could not parse task output as JSON')


# MongoEngine signals
mongoengine.signals.pre_delete.connect(StateIO.pre_delete, sender=StateIO)
