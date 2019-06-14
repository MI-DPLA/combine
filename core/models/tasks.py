# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import datetime
import json
import logging

# django imports
from django.db import models

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)

# celery
from celery.result import AsyncResult
from celery.task.control import revoke

# core models imports
from core.models import job as mod_job



class CombineBackgroundTask(models.Model):

    '''
    Model for long running, background tasks
    '''

    name = models.CharField(max_length=255, null=True, default=None)
    task_type = models.CharField(
        max_length=255,
        choices=[
            ('job_delete', 'Job Deletion'),
            ('record_group_delete', 'Record Group Deletion'),
            ('org_delete', 'Organization Deletion'),
            ('validation_report', 'Validation Report Generation'),
            ('export_mapped_fields', 'Export Mapped Fields'),
            ('export_documents', 'Export Documents'),
            ('job_reindex', 'Job Reindex Records'),
            ('job_new_validations', 'Job New Validations'),
            ('job_remove_validation', 'Job Remove Validation')
        ],
        default=None,
        null=True
    )
    celery_task_id = models.CharField(max_length=128, null=True, default=None)
    celery_task_output = models.TextField(null=True, default=None)
    task_params_json = models.TextField(null=True, default=None)
    task_output_json = models.TextField(null=True, default=None)
    start_timestamp = models.DateTimeField(null=True, auto_now_add=True)
    finish_timestamp = models.DateTimeField(null=True, default=None, auto_now_add=False)
    completed = models.BooleanField(default=False)

    def __str__(self):
        return 'CombineBackgroundTask: %s, ID #%s, Celery Task ID #%s' % (self.name, self.id, self.celery_task_id)

    def update(self):

        '''
        Method to update completed status, and affix task to instance
        '''

        # get async task from Redis
        try:

            self.celery_task = AsyncResult(self.celery_task_id)
            self.celery_status = self.celery_task.status

            if not self.completed:

                # if ready (finished)
                if self.celery_task.ready():

                    # set completed
                    self.completed = True

                    # update timestamp
                    self.finish_timestamp = datetime.datetime.now()

                    # handle result type
                    if isinstance(self.celery_task.result, Exception):
                        result = str(self.celery_task.result)
                    else:
                        result = self.celery_task.result

                    # save json of async task output
                    task_output = {
                        'result':result,
                        'status':self.celery_task.status,
                        'task_id':self.celery_task.task_id,
                        'traceback':self.celery_task.traceback
                    }
                    self.celery_task_output = json.dumps(task_output)

                    # save
                    self.save()

        except Exception as err:
            self.celery_task = None
            self.celery_status = 'STOPPED'
            self.completed = True
            self.save()
            LOGGER.debug(str(err))

    def calc_elapsed_as_string(self):

        # determine time elapsed in seconds

        # completed, with timestamp
        if self.completed and self.finish_timestamp != None:
            # use finish timestamp
            seconds_elapsed = (self.finish_timestamp.replace(tzinfo=None) - self.start_timestamp.replace(tzinfo=None)).seconds

        # marked as completed, but not timestamp, set to zero
        elif self.completed:
            seconds_elapsed = 0

        # else, calc until now
        else:
            seconds_elapsed = (datetime.datetime.now() - self.start_timestamp.replace(tzinfo=None)).seconds

        # return as string
        minutes, seconds = divmod(seconds_elapsed, 60)
        hours, minutes = divmod(minutes, 60)

        return "%d:%02d:%02d" % (hours, minutes, seconds)

    @property
    def task_params(self):

        '''
        Property to return JSON params as dict
        '''

        if self.task_params_json:
            return json.loads(self.task_params_json)
        return {}

    def update_task_params(self, update_d, save=True):

        '''
        Method to update tasks params

        Args:
            update_d (dict): Dictionary to update self.task_params with

        Returns:
            None
        '''

        # update json
        task_params = self.task_params
        task_params.update(update_d)
        self.task_params_json = json.dumps(task_params)

        # save
        if save:
            self.save()

    @property
    def task_output(self):

        '''
        Property to return JSON output as dict
        '''

        if self.task_output_json:
            return json.loads(self.task_output_json)
        return {}

    def cancel(self):

        '''
        Method to cancel background task
        '''

        # attempt to stop any Spark jobs
        if 'job_id' in self.task_params.keys():
            job_id = self.task_params['job_id']
            LOGGER.debug('attempt to kill spark jobs related to Job: %s', job_id)
            job = mod_job.Job.objects.get(pk=int(job_id))
            job.stop_job(cancel_livy_statement=False, kill_spark_jobs=True)

        # revoke celery task
        if self.celery_task_id:
            revoke(self.celery_task_id, terminate=True)

        # update status
        self.refresh_from_db()
        self.completed = True
        self.save()

    @classmethod
    def to_rerun_jobs(cls, job_ids):
        task = cls(
            name="Rerun Jobs Prep",
            task_type='rerun_jobs_prep',
            task_params_json=json.dumps({
                'ordered_job_rerun_set': job_ids
            })
        )
        task.save()
        return task
