import json
import logging
from lxml import etree

from django.core.urlresolvers import reverse
from django.db.models import Q
from django_datatables_view.base_datatable_view import BaseDatatableView

from core.models import PublishedRecords, Job, Record, IndexMappingFailure, JobValidation,\
    CombineBackgroundTask
from core.mongo import mongoengine, ObjectId


from .job import job_details
from .record import record, record_document
from .core_background_tasks import bg_task, bg_task_cancel, bg_task_delete

LOGGER = logging.getLogger(__name__)


####################################################################
# Datatables endpoints 											   #
# https://bitbucket.org/pigletto/django-datatables-view/overview   #
####################################################################


class DTPublishedJson(BaseDatatableView):
    """
                Prepare and return Datatables JSON for Published records
                """

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
        pub_records = PublishedRecords(subset=self.kwargs.get('subset', None))

        # return queryset
        return pub_records.records

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
                etree.fromstring(row.document.encode('utf-8'))
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
                    LOGGER.debug('recieved 24 chars, but not ObjectId')
            else:
                qs = qs.filter(mongoengine.Q(record_id=search) |
                               mongoengine.Q(publish_set_id=search))

        # return
        return qs


class DTRecordsJson(BaseDatatableView):
    """
                Prepare and return Datatables JSON for Records table in Job Details
                """

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

        # return queryset used as base for further sorting/filtering

        # if job present, filter by job
        if 'job_id' in self.kwargs.keys():

            # get jobself.kwargs['job_id']
            job = Job.objects.get(pk=self.kwargs['job_id'])

            # return filtered queryset
            if 'success_filter' in self.kwargs.keys():
                success_filter = bool(int(self.kwargs['success_filter']))
            else:
                success_filter = None
            return job.get_records(success=success_filter)

        # else, return all records
        job_ids = list(map(lambda j: j.id, Job.objects.all()))
        records = Record.objects.filter(job_id__in=job_ids)
        return records

    def render_column(self, row, column):

        # construct record link
        record_link = reverse(record, kwargs={
            'org_id': row.job.record_group.organization.id,
            'record_group_id': row.job.record_group.id,
            'job_id': row.job.id,
            'record_id': str(row.id)
        })

        # handle db_id
        if column == '_id':
            return '<a href="%s"><code>%s</code></a>' % (record_link, str(row.id))

        # handle record_id
        if column == 'record_id':
            return '<a href="%s"><code>%s</code></a>' % (record_link, row.record_id)

        # handle document
        if column == 'document':
            # attempt to parse as XML and return if valid or not
            try:
                etree.fromstring(row.document.encode('utf-8'))
                return '<a target="_blank" href="%s">Valid XML</a>' % (reverse(record_document, kwargs={
                    'org_id': row.job.record_group.organization.id,
                    'record_group_id': row.job.record_group.id,
                    'job_id': row.job.id, 'record_id': str(row.id)
                }))
            except:
                return '<span style="color: red;">Invalid XML</span>'

        # handle associated job
        if column == 'job':
            return '<a href="%s"><code>%s</code></a>' % (reverse(job_details, kwargs={
                'org_id': row.job.record_group.organization.id,
                'record_group_id': row.job.record_group.id,
                'job_id': row.job.id
            }), row.job.name)

        # handle unique
        if column == 'unique':
            if row.unique:
                return '<span style="color:green;">Unique in Job</span>'
            return '<span style="color:red;">Duplicate in Job</span>'

        # handle validation_results
        if column == 'valid':
            if row.valid:
                return '<span style="color:green;">Valid</span>'
            return '<span style="color:red;">Invalid</span>'

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
                    LOGGER.debug('received 24 chars, but not ObjectId')
            else:
                qs = qs.filter(mongoengine.Q(record_id=search))

        # return
        return qs


class DTIndexingFailuresJson(BaseDatatableView):
    """
                Databales JSON response for Indexing Failures
                """

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
        job = Job.objects.get(pk=self.kwargs['job_id'])

        # return filtered queryset
        return IndexMappingFailure.objects(job_id=job.id)

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

        return super(DTIndexingFailuresJson, self).render_column(row, column)


class DTJobValidationScenarioFailuresJson(BaseDatatableView):
    """
                Prepare and return Datatables JSON for RecordValidation failures from Job, per Validation Scenario
                """

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
        job_validation = JobValidation.objects.get(
            pk=self.kwargs['job_validation_id'])

        # return filtered queryset
        return job_validation.get_record_validation_failures()

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
        if column == 'record':
            # get target record from row
            target_record = row.record
            return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

        # handle results_payload
        if column == 'results_payload':
            result_payload = json.loads(row.results_payload)['failed']
            return ', '.join(result_payload)

        # handle all else
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
                    LOGGER.debug('recieved 24 chars, but not ObjectId')
        # return
        return qs


class DTDPLABulkDataMatches(BaseDatatableView):
    """
                Prepare and return Datatables JSON for RecordValidation failures from Job, per Validation Scenario
                """

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
        job = Job.objects.get(pk=self.kwargs['job_id'])

        # return queryset filtered for match/miss
        if self.kwargs['match_type'] == 'matches':
            return job.get_records().filter(dbdm=True)
        if self.kwargs['match_type'] == 'misses':
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
        if column == 'record_id':
            # get target record from row
            target_record = row
            return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

        # handle all else
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
                    LOGGER.debug('recieved 24 chars, but not ObjectId')
            else:
                qs = qs.filter(mongoengine.Q(record_id=search))

        # return
        return qs


class JobRecordDiffs(BaseDatatableView):
    """
                Prepare and return Datatables JSON for Records that were
                transformed during a Transformation Job
                """

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
        job = Job.objects.get(pk=self.kwargs['job_id'])
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

        return super(JobRecordDiffs, self).render_column(row, column)

    def filter_queryset(self, qs):

        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            qs = qs.filter(Q(id__contains=search) | Q(
                record_id__contains=search) | Q(document__contains=search))

        # return
        return qs


class CombineBackgroundTasksDT(BaseDatatableView):
    """
                Prepare and return Datatables JSON for Records table in Job Details
                """

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
        return CombineBackgroundTask.objects

    def render_column(self, row, column):

        if column == 'task_type':
            return row.get_task_type_display()

        if column == 'celery_task_id':
            return '<code>%s</code>' % row.celery_task_id

        if column == 'completed':
            if row.completed:
                if row.celery_status in ['STOPPED', 'REVOKED']:
                    row_color = 'danger'
                else:
                    row_color = 'success'
            else:
                row_color = 'warning'
            return '''<div class="progress progress-bar bg-{}" role="progressbar" aria-valuenow="100" aria-valuemin="0" aria-valuemax="100" style="width: 100%">{}</div>'''.format(row_color, row.celery_status)

        if column == 'duration':
            return row.calc_elapsed_as_string()

        if column == 'actions':
            return '<a href="%s"><button type="button" class="btn btn-success btn-sm">Results <i class="la la-info-circle"></i></button></a> <a href="%s"><button type="button" class="btn btn-danger btn-sm" onclick="return confirm(\'Are you sure you want to cancel this task?\');">Stop <i class="la la-stop"></i></button></a> <a href="%s"><button type="button" class="btn btn-danger btn-sm" onclick="return confirm(\'Are you sure you want to remove this task?\');">Delete <i class="la la-close"></i></button></a>' % (
                reverse(bg_task, kwargs={'task_id': row.id}),
                reverse(bg_task_cancel, kwargs={'task_id': row.id}),
                reverse(bg_task_delete, kwargs={'task_id': row.id}),
            )
        return super(CombineBackgroundTasksDT, self).render_column(row, column)

    def filter_queryset(self, qs):
        # use parameters passed in GET request to filter queryset

        # handle search
        search = self.request.GET.get(u'search[value]', None)
        if search:
            qs = qs.filter(Q(id__contains=search) | Q(
                name__contains=search) | Q(verbose_name__contains=search))

        # return
        return qs
