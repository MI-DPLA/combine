import logging
import json

from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render

from core import forms, tasks
from core.models import RecordGroup, CombineBackgroundTask, Job, PublishedRecords

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


@login_required
def record_group_id_redirect(request, record_group_id):
    """
        Route to redirect to more verbose Record Group URL
        """

    # get job
    rec_group = RecordGroup.objects.get(pk=record_group_id)

    # redirect
    return redirect('record_group',
                    org_id=rec_group.organization.id,
                    record_group_id=rec_group.id)


def record_group_new(request, org_id):
    """
        Create new Record Group
        """

    # create new organization
    if request.method == 'POST':
        # create new record group
        LOGGER.debug(request.POST)
        form = forms.RecordGroupForm(request.POST)
        new_rg = form.save()

        # redirect to organization page
        return redirect('record_group', org_id=org_id, record_group_id=new_rg.id)


def record_group_delete(request, org_id, record_group_id):
    """
        Create new Record Group
        """

    # retrieve record group
    rec_group = RecordGroup.objects.get(pk=record_group_id)

    # set job status to deleting
    rec_group.name = "%s (DELETING)" % rec_group.name
    rec_group.save()

    # initiate Combine BG Task
    combine_task = CombineBackgroundTask(
        name='Delete RecordGroup: %s' % rec_group.name,
        task_type='delete_model_instance',
        task_params_json=json.dumps({
            'model': 'RecordGroup',
            'record_group_id': rec_group.id
        })
    )
    combine_task.save()

    # run celery task
    bg_task = tasks.delete_model_instance.delay(
        'RecordGroup', rec_group.id, )
    LOGGER.debug('firing bg task: %s', bg_task)
    combine_task.celery_task_id = bg_task.task_id
    combine_task.save()

    # redirect to organization page
    return redirect('organization', org_id=org_id)


@login_required
def record_group(request, org_id, record_group_id):
    """
        View information about a single record group, including any and all jobs run

        Args:
                record_group_id (str/int): PK for RecordGroup table
        """

    LOGGER.debug('retrieving record group ID: %s', record_group_id)

    # retrieve record group
    rec_group = RecordGroup.objects.get(pk=int(record_group_id))

    # get all jobs associated with record group
    jobs = Job.objects.filter(record_group=record_group_id)

    # get all currently applied publish set ids
    publish_set_ids = PublishedRecords.get_publish_set_ids()

    # loop through jobs
    for job in jobs:
        # update status
        job.update_status()

    # get record group job lineage
    job_lineage = rec_group.get_jobs_lineage()

    # get all record groups for this organization
    record_groups = RecordGroup.objects.filter(organization=org_id).exclude(id=record_group_id).exclude(
        for_analysis=True)

    # render page
    return render(request, 'core/record_group.html', {
        'record_group': rec_group,
        'jobs': jobs,
        'job_lineage_json': json.dumps(job_lineage),
        'publish_set_ids': publish_set_ids,
        'record_groups': record_groups,
        'breadcrumbs': breadcrumb_parser(request)
    })
