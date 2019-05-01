import json
import logging
from lxml import etree

from django.core.urlresolvers import reverse
from django.db.models import Q

from core import models
from core.mongo import *

from django_datatables_view.base_datatable_view import BaseDatatableView

from .job import job_details
from .record import record, record_document
from .views import bg_task, bg_task_cancel, bg_task_delete

logger = logging.getLogger(__name__)

####################################################################
# Datatables endpoints 											   #
# https://bitbucket.org/pigletto/django-datatables-view/overview   #
####################################################################


class DTPublishedJson(BaseDatatableView):
    '''
		Prepare and return Datatables JSON for Published records
		'''

    # define the columns that will be returned
    columns = [
        '_id',
        'record_id',
        'job_id',
        'publish_set_id',
        # 'oai_set',
        # 'unique_published',
        'document'
    ]

    # define column names that will be used in sorting
    # order is important and should be same as order of columns
    # displayed by datatables. For non sortable columns use empty
    # value like ''
    order_columns = [
        '_id',
        'record_id',
        'job_id',
        'publish_set_id',
        # 'oai_set',
        # 'unique_published',
        'document'
    ]

    # set max limit of records returned, this is used to protect our site if someone tries to attack our site
    # and make it return huge amount of data
    max_display_length = 1000

    def get_initial_queryset(self):

        # return queryset used as base for futher sorting/filtering

        # get PublishedRecords instance
        pr = models.PublishedRecords(subset=self.kwargs.get('subset', None))

        # return queryset
        return pr.records

    def render_column(self, row, column):

        # handle document metadata

        if column == '_id':
            return '<a href="%s">%s</a>' % (reverse(record, kwargs={
                'org_id': row.job.record_group.organization.id,
                'record_group_id': row.job.record_group.id,
                'job_id': row.job.id, 'record_id': str(row.id)
            }), str(row.id))

        if column == 'record_id':
            return '<a href="%s">%s</a>' % (reverse(record, kwargs={
                'org_id': row.job.record_group.organization.id,
                'record_group_id': row.job.record_group.id,
                'job_id': row.job.id, 'record_id': str(row.id)
            }), row.record_id)

        if column == 'job_id':
            return '<a href="%s">%s</a>' % (reverse(job_details, kwargs={
                'org_id': row.job.record_group.organization.id,
                'record_group_id': row.job.record_group.id,
                'job_id': row.job.id
            }), row.job.name)

        if column == 'document':
            # attempt to parse as XML and return if valid or not
            try:
                xml = etree.fromstring(row.document.encode('utf-8'))
                return '<a target="_blank" href="%s">Valid XML</a>' % (reverse(record_document, kwargs={
                    'org_id': row.job.record_group.organization.id,
                    'record_group_id': row.job.record_group.id,
                    'job_id': row.job.id, 'record_id': str(row.id)
                }))
            except:
                return '<span style="color: red;">Invalid XML</span>'

        # # handle associated job
        # if column == 'unique_published':
        # 	if row.unique_published:
        # 		return '<span style="color:green;">True</span>'
        # 	else:
        # 		return '<span style="color:red;">False</span>'

        else:
            return super(DTPublishedJson, self).render_column(row, column)

    def filter_queryset(self, qs):
        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            # sniff out ObjectId if present
            if len(search) == 24:
                try:
                    oid = ObjectId(search)
                    qs = qs.filter(mongoengine.Q(id=oid))
                except:
                    logger.debug('recieved 24 chars, but not ObjectId')
            else:
                qs = qs.filter(mongoengine.Q(record_id=search) | mongoengine.Q(publish_set_id=search))

        # return
        return qs



class DTRecordsJson(BaseDatatableView):
    '''
		Prepare and return Datatables JSON for Records table in Job Details
		'''

    # define the columns that will be returned
    columns = [
        '_id',
        'record_id',
        'job_id',
        'oai_set',
        'unique',
        'document',
        'error',
        'valid'
    ]

    # define column names that will be used in sorting
    # order is important and should be same as order of columns
    # displayed by datatables. For non sortable columns use empty
    # value like ''
    # order_columns = ['number', 'user', 'state', '', '']
    order_columns = [
        '_id',
        'record_id',
        'job_id',
        'oai_set',
        'unique',
        'document',
        'error',
        'valid'
    ]

    # set max limit of records returned, this is used to protect our site if someone tries to attack our site
    # and make it return huge amount of data
    max_display_length = 1000

    def get_initial_queryset(self):

        # return queryset used as base for futher sorting/filtering

        # if job present, filter by job
        if 'job_id' in self.kwargs.keys():

            # get jobself.kwargs['job_id']
            job = models.Job.objects.get(pk=self.kwargs['job_id'])

            # return filtered queryset
            if 'success_filter' in self.kwargs.keys():
                success_filter = bool(int(self.kwargs['success_filter']))
            else:
                success_filter = None
            return job.get_records(success=success_filter)

        # else, return all records
        else:
            return models.Record.objects

    def render_column(self, row, column):

        # construct record link
        record_link = reverse(record, kwargs={
            'org_id': row.job.record_group.organization.id,
            'record_group_id': row.job.record_group.id,
            'job_id': row.job.id, 'record_id': str(row.id)
        })

        # handle db_id
        if column == '_id':
            return '<a href="%s"><code>%s</code></a>' % (record_link, str(row.id))

        # handle record_id
        if column == 'record_id':
            return '<a href="%s"><code>%s</code></a>' % (record_link, row.record_id)

        # handle document
        elif column == 'document':
            # attempt to parse as XML and return if valid or not
            try:
                xml = etree.fromstring(row.document.encode('utf-8'))
                return '<a target="_blank" href="%s">Valid XML</a>' % (reverse(record_document, kwargs={
                    'org_id': row.job.record_group.organization.id,
                    'record_group_id': row.job.record_group.id,
                    'job_id': row.job.id, 'record_id': str(row.id)
                }))
            except:
                return '<span style="color: red;">Invalid XML</span>'

        # handle associated job
        elif column == 'job':
            return '<a href="%s"><code>%s</code></a>' % (reverse(job_details, kwargs={
                'org_id': row.job.record_group.organization.id,
                'record_group_id': row.job.record_group.id,
                'job_id': row.job.id
            }), row.job.name)

        # handle unique
        elif column == 'unique':
            if row.unique:
                return '<span style="color:green;">Unique in Job</span>'
            else:
                return '<span style="color:red;">Duplicate in Job</span>'

        # handle validation_results
        elif column == 'valid':
            if row.valid:
                return '<span style="color:green;">Valid</span>'
            else:
                return '<span style="color:red;">Invalid</span>'

        else:
            return super(DTRecordsJson, self).render_column(row, column)

    def filter_queryset(self, qs):
        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            # sniff out ObjectId if present
            if len(search) == 24:
                try:
                    oid = ObjectId(search)
                    qs = qs.filter(mongoengine.Q(id=oid))
                except:
                    logger.debug('recieved 24 chars, but not ObjectId')
            else:
                qs = qs.filter(mongoengine.Q(record_id=search))

        # return
        return qs

class DTIndexingFailuresJson(BaseDatatableView):
    '''
		Databales JSON response for Indexing Failures
		'''

    # define the columns that will be returned
    columns = ['_id', 'record_id', 'mapping_error']

    # define column names that will be used in sorting
    order_columns = ['_id', 'record_id', 'mapping_error']

    # set max limit of records returned, this is used to protect our site if someone tries to attack our site
    # and make it return huge amount of data
    max_display_length = 1000

    def get_initial_queryset(self):

        # return queryset used as base for futher sorting/filtering

        # get job
        job = models.Job.objects.get(pk=self.kwargs['job_id'])

        # return filtered queryset
        return models.IndexMappingFailure.objects(job_id=job.id)

    def render_column(self, row, column):

        # determine record link
        target_record = row.record
        record_link = reverse(record, kwargs={
            'org_id': target_record.job.record_group.organization.id,
            'record_group_id': target_record.job.record_group.id,
            'job_id': target_record.job.id,
            'record_id': target_record.id
        })

        if column == '_id':
            return '<a href="%s">%s</a>' % (record_link, target_record.id)

        if column == 'record_id':
            return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

        # handle associated job
        if column == 'job':
            return row.job.name

        else:
            return super(DTIndexingFailuresJson, self).render_column(row, column)


class DTJobValidationScenarioFailuresJson(BaseDatatableView):
    '''
		Prepare and return Datatables JSON for RecordValidation failures from Job, per Validation Scenario
		'''

    # define the columns that will be returned
    columns = [
        'id',
        'record',
        'results_payload',
        'fail_count'
    ]

    # define column names that will be used in sorting
    # order is important and should be same as order of columns
    # displayed by datatables. For non sortable columns use empty
    # value like ''
    # order_columns = ['number', 'user', 'state', '', '']
    order_columns = [
        'id',
        'record',
        'results_payload',
        'fail_count'
    ]

    # set max limit of records returned, this is used to protect our site if someone tries to attack our site
    # and make it return huge amount of data
    max_display_length = 1000

    def get_initial_queryset(self):

        # return queryset used as base for futher sorting/filtering

        # get job
        jv = models.JobValidation.objects.get(pk=self.kwargs['job_validation_id'])

        # return filtered queryset
        return jv.get_record_validation_failures()

    def render_column(self, row, column):

        # determine record link
        target_record = row.record
        record_link = "%s#validation_tab" % reverse(record, kwargs={
            'org_id': target_record.job.record_group.organization.id,
            'record_group_id': target_record.job.record_group.id,
            'job_id': target_record.job.id,
            'record_id': target_record.id
        })

        # handle record id
        if column == 'id':
            # get target record from row
            target_record = row.record
            return '<a href="%s">%s</a>' % (record_link, target_record.id)

        # handle record record_id
        elif column == 'record':
            # get target record from row
            target_record = row.record
            return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

        # handle results_payload
        elif column == 'results_payload':
            rp = json.loads(row.results_payload)['failed']
            return ', '.join(rp)

        # handle all else
        else:
            return super(DTJobValidationScenarioFailuresJson, self).render_column(row, column)

    def filter_queryset(self, qs):
        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            # sniff out ObjectId if present
            if len(search) == 24:
                try:
                    oid = ObjectId(search)
                    qs = qs.filter(mongoengine.Q(record_id=oid))
                except:
                    logger.debug('recieved 24 chars, but not ObjectId')
        # return
        return qs

class DTDPLABulkDataMatches(BaseDatatableView):
    '''
		Prepare and return Datatables JSON for RecordValidation failures from Job, per Validation Scenario
		'''

    # define the columns that will be returned
    columns = [
        'id',
        'record_id'
    ]

    # define column names that will be used in sorting
    # order is important and should be same as order of columns
    # displayed by datatables. For non sortable columns use empty
    # value like ''
    # order_columns = ['number', 'user', 'state', '', '']
    order_columns = [
        'id',
        'record_id'
    ]

    # set max limit of records returned, this is used to protect our site if someone tries to attack our site
    # and make it return huge amount of data
    max_display_length = 1000

    def get_initial_queryset(self):

        # return queryset used as base for futher sorting/filtering

        # get job and records
        job = models.Job.objects.get(pk=self.kwargs['job_id'])

        # return queryset filtered for match/miss
        if self.kwargs['match_type'] == 'matches':
            return job.get_records().filter(dbdm=True)
        elif self.kwargs['match_type'] == 'misses':
            return job.get_records().filter(dbdm=False)

    def render_column(self, row, column):

        # determine record link
        target_record = row
        record_link = reverse(record, kwargs={
            'org_id': target_record.job.record_group.organization.id,
            'record_group_id': target_record.job.record_group.id,
            'job_id': target_record.job.id,
            'record_id': target_record.id
        })

        # handle record id
        if column == 'id':
            # get target record from row
            target_record = row
            return '<a href="%s">%s</a>' % (record_link, target_record.id)

        # handle record record_id
        elif column == 'record_id':
            # get target record from row
            target_record = row
            return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

        # handle all else
        else:
            return super(DTDPLABulkDataMatches, self).render_column(row, column)

    def filter_queryset(self, qs):
        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            # sniff out ObjectId if present
            if len(search) == 24:
                try:
                    oid = ObjectId(search)
                    qs = qs.filter(mongoengine.Q(id=oid))
                except:
                    logger.debug('recieved 24 chars, but not ObjectId')
            else:
                qs = qs.filter(mongoengine.Q(record_id=search))

        # return
        return qs


class JobRecordDiffs(BaseDatatableView):
    '''
		Prepare and return Datatables JSON for Records that were
		transformed during a Transformation Job
		'''

    # define the columns that will be returned
    columns = [
        'id',
        'record_id',
    ]

    # define column names that will be used in sorting
    # order is important and should be same as order of columns
    # displayed by datatables. For non sortable columns use empty
    # value like ''
    order_columns = [
        'id',
        'record_id'
    ]

    # set max limit of records returned, this is used to protect our site if someone tries to attack our site
    # and make it return huge amount of data
    max_display_length = 1000

    def get_initial_queryset(self):

        # return queryset used as base for futher sorting/filtering

        # get job
        job = models.Job.objects.get(pk=self.kwargs['job_id'])
        job_records = job.get_records()

        # filter for records that were transformed
        return job_records.filter(transformed=True)

    def render_column(self, row, column):

        # record link
        record_link = "%s#job_type_specific_tab" % reverse(record, kwargs={
            'org_id': row.job.record_group.organization.id,
            'record_group_id': row.job.record_group.id,
            'job_id': row.job.id, 'record_id': row.id
        })

        # handle db_id
        if column == 'id':
            return '<a href="%s"><code>%s</code></a>' % (record_link, row.id)

        # handle record_id
        if column == 'record_id':
            return '<a href="%s"><code>%s</code></a>' % (record_link, row.record_id)

        else:
            return super(JobRecordDiffs, self).render_column(row, column)

    def filter_queryset(self, qs):

        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            qs = qs.filter(Q(id__contains=search) | Q(record_id__contains=search) | Q(document__contains=search))

        # return
        return qs

class CombineBackgroundTasksDT(BaseDatatableView):
    '''
		Prepare and return Datatables JSON for Records table in Job Details
		'''

    # define the columns that will be returned
    columns = [
        'id',
        'start_timestamp',
        'name',
        'task_type',
        'celery_task_id',
        'completed',
        'duration',
        'actions'
    ]

    # define column names that will be used in sorting
    # order is important and should be same as order of columns
    # displayed by datatables. For non sortable columns use empty
    # value like ''
    # order_columns = ['number', 'user', 'state', '', '']
    order_columns = [
        'id',
        'start_timestamp',
        'name',
        'task_type',
        'celery_task_id',
        'completed',
        'duration',
        'actions'
    ]

    # set max limit of records returned, this is used to protect our site if someone tries to attack our site
    # and make it return huge amount of data
    max_display_length = 1000

    def get_initial_queryset(self):

        # return queryset used as base for futher sorting/filtering
        return models.CombineBackgroundTask.objects

    def render_column(self, row, column):

        if column == 'task_type':
            return row.get_task_type_display()

        elif column == 'celery_task_id':
            return '<code>%s</code>' % row.celery_task_id

        elif column == 'completed':
            if row.completed:
                if row.celery_status in ['STOPPED', 'REVOKED']:
                    return "<span class='text-danger'>%s</span>" % row.celery_status
                else:
                    return "<span class='text-success'>%s</span>" % row.celery_status
            else:
                return "<span class='text-warning'>%s</span>" % row.celery_status

        elif column == 'duration':
            return row.calc_elapsed_as_string()


        elif column == 'actions':
            return '<a href="%s"><button type="button" class="btn btn-success btn-sm">Results <i class="la la-info-circle"></i></button></a> <a href="%s"><button type="button" class="btn btn-danger btn-sm" onclick="return confirm(\'Are you sure you want to cancel this task?\');">Stop <i class="la la-stop"></i></button></a> <a href="%s"><button type="button" class="btn btn-danger btn-sm" onclick="return confirm(\'Are you sure you want to remove this task?\');">Delete <i class="la la-close"></i></button></a>' % (
                reverse(bg_task, kwargs={'task_id': row.id}),
                reverse(bg_task_cancel, kwargs={'task_id': row.id}),
                reverse(bg_task_delete, kwargs={'task_id': row.id}),
            )

        else:
            return super(CombineBackgroundTasksDT, self).render_column(row, column)

    def filter_queryset(self, qs):
        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            qs = qs.filter(Q(id__contains=search) | Q(name__contains=search) | Q(verbose_name__contains=search))

        # return
        return qs
