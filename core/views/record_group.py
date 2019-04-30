import logging
import json

from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render

from core import models, forms, tasks

from .views import breadcrumb_parser

logger =logging.getLogger(__name__)

@login_required
def record_group_id_redirect(request, record_group_id):
    '''
	Route to redirect to more verbose Record Group URL
	'''

    # get job
    record_group = models.RecordGroup.objects.get(pk=record_group_id)

    # redirect
    return redirect('record_group',
                    org_id=record_group.organization.id,
                    record_group_id=record_group.id)


def record_group_new(request, org_id):
    '''
	Create new Record Group
	'''

    # create new organization
    if request.method == 'POST':
        # create new record group
        logger.debug(request.POST)
        f = forms.RecordGroupForm(request.POST)
        new_rg = f.save()

        # redirect to organization page
        return redirect('record_group', org_id=org_id, record_group_id=new_rg.id)


def record_group_delete(request, org_id, record_group_id):
    '''
	Create new Record Group
	'''

    # retrieve record group
    record_group = models.RecordGroup.objects.get(pk=record_group_id)

    # set job status to deleting
    record_group.name = "%s (DELETING)" % record_group.name
    record_group.save()

    # initiate Combine BG Task
    ct = models.CombineBackgroundTask(
        name='Delete RecordGroup: %s' % record_group.name,
        task_type='delete_model_instance',
        task_params_json=json.dumps({
            'model': 'RecordGroup',
            'record_group_id': record_group.id
        })
    )
    ct.save()

    # run celery task
    bg_task = tasks.delete_model_instance.delay('RecordGroup', record_group.id, )
    logger.debug('firing bg task: %s' % bg_task)
    ct.celery_task_id = bg_task.task_id
    ct.save()

    # redirect to organization page
    return redirect('organization', org_id=org_id)


@login_required
def record_group(request, org_id, record_group_id):
    '''
	View information about a single record group, including any and all jobs run

	Args:
		record_group_id (str/int): PK for RecordGroup table
	'''

    logger.debug('retrieving record group ID: %s' % record_group_id)

    # retrieve record group
    record_group = models.RecordGroup.objects.get(pk=int(record_group_id))

    # get all jobs associated with record group
    jobs = models.Job.objects.filter(record_group=record_group_id)

    # get all currently applied publish set ids
    publish_set_ids = models.PublishedRecords.get_publish_set_ids()

    # loop through jobs
    for job in jobs:
        # update status
        job.update_status()

    # get record group job lineage
    job_lineage = record_group.get_jobs_lineage()

    # get all record groups for this organization
    record_groups = models.RecordGroup.objects.filter(organization=org_id).exclude(id=record_group_id).exclude(
        for_analysis=True)

    # render page
    return render(request, 'core/record_group.html', {
        'record_group': record_group,
        'jobs': jobs,
        'job_lineage_json': json.dumps(job_lineage),
        'publish_set_ids': publish_set_ids,
        'record_groups': record_groups,
        'breadcrumbs': breadcrumb_parser(request)
    })

