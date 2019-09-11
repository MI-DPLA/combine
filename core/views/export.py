import json
import logging

from django.core.urlresolvers import reverse
from django.shortcuts import redirect

from core import tasks
from core.models import CombineJob, CombineBackgroundTask, GlobalMessageClient,\
    PublishedRecords

LOGGER = logging.getLogger(__name__)


def export_documents(request,
                     export_source=None,
                     job_id=None,
                     subset=None):
    # get records per file
    records_per_file = request.POST.get('records_per_file', False)
    if records_per_file in ['', False]:
        records_per_file = 500

    # get archive type
    archive_type = request.POST.get('archive_type')

    # export for single job
    if export_source == 'job':
        LOGGER.debug('exporting documents from Job')

        # retrieve job
        cjob = CombineJob.get_combine_job(int(job_id))

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Export Documents for Job: %s' % cjob.job.name,
            task_type='export_documents',
            task_params_json=json.dumps({
                'job_id': cjob.job.id,
                'records_per_file': int(records_per_file),
                'archive_type': archive_type
            })
        )
        combine_task.save()

        # handle export output configurations
        combine_task = _handle_export_output(request, export_source, combine_task)

        # run celery task
        background_task = tasks.export_documents.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', background_task)
        combine_task.celery_task_id = background_task.task_id
        combine_task.save()

        # set gm
        gmc = GlobalMessageClient(request.session)
        target = "Job:</strong><br>%s" % cjob.job.name
        gmc.add_gm({
            'html': '<p><strong>Exporting Documents for %s</p><p><a href="%s">'
                    '<button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button>'
                    '</a></p>' % (
                        target, reverse('bg_tasks')),
            'class': 'success'
        })

        return redirect('job_details',
                        org_id=cjob.job.record_group.organization.id,
                        record_group_id=cjob.job.record_group.id,
                        job_id=cjob.job.id)

    # export for published
    if export_source == 'published':
        LOGGER.debug('exporting documents for published records')

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Export Documents for Published Records',
            task_type='export_documents',
            task_params_json=json.dumps({
                'published': True,
                'subset': subset,
                'records_per_file': int(records_per_file),
                'archive_type': archive_type
            })
        )
        combine_task.save()

        # handle export output configurations
        combine_task = _handle_export_output(request, export_source, combine_task)

        # run celery task
        background_task = tasks.export_documents.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', background_task)
        combine_task.celery_task_id = background_task.task_id
        combine_task.save()

        # set gm
        gmc = GlobalMessageClient(request.session)
        target = ":</strong><br>Published Records"
        gmc.add_gm({
            'html': '<p><strong>Exporting Documents for %s</p><p><a href="%s"><button type="button" '
                    'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                        target, reverse('bg_tasks')),
            'class': 'success'
        })

        return redirect('published')


def export_mapped_fields(request,
                         export_source=None,
                         job_id=None,
                         subset=None):
    # get mapped fields export type
    mapped_fields_export_type = request.POST.get('mapped_fields_export_type')

    # check for Kibana check
    kibana_style = request.POST.get('kibana_style', False)
    if kibana_style:
        kibana_style = True

    # get archive type
    archive_type = request.POST.get('archive_type')

    # get selected fields if present
    mapped_field_include = request.POST.getlist('mapped_field_include', False)

    # export for single job
    if export_source == 'job':
        LOGGER.debug('exporting mapped fields from Job')

        # retrieve job
        cjob = CombineJob.get_combine_job(int(job_id))

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Export Mapped Fields for Job: %s' % cjob.job.name,
            task_type='export_mapped_fields',
            task_params_json=json.dumps({
                'job_id': cjob.job.id,
                'mapped_fields_export_type': mapped_fields_export_type,
                'kibana_style': kibana_style,
                'archive_type': archive_type,
                'mapped_field_include': mapped_field_include
            })
        )
        combine_task.save()

        # handle export output configurations
        combine_task = _handle_export_output(request, export_source, combine_task)

        # run celery task
        background_task = tasks.export_mapped_fields.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', background_task)
        combine_task.celery_task_id = background_task.task_id
        combine_task.save()

        # set gm
        gmc = GlobalMessageClient(request.session)
        target = "Job:</strong><br>%s" % cjob.job.name
        gmc.add_gm({
            'html': '<p><strong>Exporting Mapped Fields for %s</p><p><a href="%s"><button type="button" '
                    'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                        target, reverse('bg_tasks')),
            'class': 'success'
        })

        return redirect('job_details',
                        org_id=cjob.job.record_group.organization.id,
                        record_group_id=cjob.job.record_group.id,
                        job_id=cjob.job.id)

    # export for published
    if export_source == 'published':
        LOGGER.debug('exporting mapped fields from published records')

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Export Mapped Fields for Published Records',
            task_type='export_mapped_fields',
            task_params_json=json.dumps({
                'published': True,
                'subset': subset,
                'mapped_fields_export_type': mapped_fields_export_type,
                'kibana_style': kibana_style,
                'archive_type': archive_type,
                'mapped_field_include': mapped_field_include
            })
        )
        combine_task.save()

        # handle export output configurations
        combine_task = _handle_export_output(request, export_source, combine_task)

        # run celery task
        background_task = tasks.export_mapped_fields.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', background_task)
        combine_task.celery_task_id = background_task.task_id
        combine_task.save()

        # set gm
        gmc = GlobalMessageClient(request.session)
        target = ":</strong><br>Published Records"
        gmc.add_gm({
            'html': '<p><strong>Exporting Mapped Fields for %s</p><p><a href="%s"><button type="button" '
                    'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                        target, reverse('bg_tasks')),
            'class': 'success'
        })

        return redirect('published')


def export_tabular_data(request,
                        export_source=None,
                        job_id=None,
                        subset=None):
    # get records per file
    records_per_file = request.POST.get('records_per_file', False)
    if records_per_file in ['', False]:
        records_per_file = 500

    # get mapped fields export type
    tabular_data_export_type = request.POST.get('tabular_data_export_type')

    # get archive type
    archive_type = request.POST.get('archive_type')

    # get fm config json
    fm_export_config_json = request.POST.get('fm_export_config_json')

    # export for single job
    if export_source == 'job':
        LOGGER.debug('exporting tabular data from Job')

        # retrieve job
        cjob = CombineJob.get_combine_job(int(job_id))

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Export Tabular Data for Job: %s' % cjob.job.name,
            task_type='export_tabular_data',
            task_params_json=json.dumps({
                'job_id': cjob.job.id,
                'records_per_file': int(records_per_file),
                'tabular_data_export_type': tabular_data_export_type,
                'archive_type': archive_type,
                'fm_export_config_json': fm_export_config_json
            })
        )
        combine_task.save()

        # handle export output configurations
        combine_task = _handle_export_output(request, export_source, combine_task)

        # run celery task
        background_task = tasks.export_tabular_data.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', background_task)
        combine_task.celery_task_id = background_task.task_id
        combine_task.save()

        # set gm
        gmc = GlobalMessageClient(request.session)
        target = "Job:</strong><br>%s" % cjob.job.name
        gmc.add_gm({
            'html': '<p><strong>Exporting Tabular Data for %s</p><p><a href="%s"><button type="button" '
                    'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                        target, reverse('bg_tasks')),
            'class': 'success'
        })

        return redirect('job_details',
                        org_id=cjob.job.record_group.organization.id,
                        record_group_id=cjob.job.record_group.id,
                        job_id=cjob.job.id)

    # export for published
    if export_source == 'published':
        LOGGER.debug('exporting tabular data from published records')

        # get instance of Published model
        # TODO: not used
        PublishedRecords()

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Export Tabular Data for Published Records',
            task_type='export_tabular_data',
            task_params_json=json.dumps({
                'published': True,
                'subset': subset,
                'records_per_file': int(records_per_file),
                'tabular_data_export_type': tabular_data_export_type,
                'archive_type': archive_type,
                'fm_export_config_json': fm_export_config_json
            })
        )
        combine_task.save()

        # handle export output configurations
        combine_task = _handle_export_output(request, export_source, combine_task)

        # run celery task
        background_task = tasks.export_tabular_data.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', background_task)
        combine_task.celery_task_id = background_task.task_id
        combine_task.save()

        # set gm
        gmc = GlobalMessageClient(request.session)
        target = ":</strong><br>Published Records"
        gmc.add_gm({
            'html': '<p><strong>Exporting Tabular Data for %s</p><p><a href="%s"><button type="button" '
                    'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                        target, reverse('bg_tasks')),
            'class': 'success'
        })

        return redirect('published')


def _handle_export_output(request, export_source, combine_task):
    """
    Function to handle export outputs
        - currently only augmenting with S3 export

    Args:
        request: request object
        export_source: ['job','published']
        combine_task (CombineBackgroundTask): instance of ct to augment

    Returns:
        ct (CombineBackgroundTask)
    """

    # handle s3 export
    s3_export = request.POST.get('s3_export', False)
    if s3_export:
        s3_export = True

    # if s3_export
    if s3_export:
        # update task params
        combine_task.update_task_params({
            's3_export': True,
            's3_bucket': request.POST.get('s3_bucket', None),
            's3_key': request.POST.get('s3_key', None),
            's3_export_type': request.POST.get('s3_export_type', None)
        })

    # save and return
    combine_task.save()
    return combine_task
