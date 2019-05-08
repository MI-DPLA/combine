import logging

from django.shortcuts import render, redirect

from core import models

from .view_helpers import breadcrumb_parser

logger = logging.getLogger(__name__)


def bg_tasks(request):
    logger.debug('retrieving background tasks')

    # update all tasks not marked as complete
    nc_tasks = models.CombineBackgroundTask.objects.filter(completed=False)
    for task in nc_tasks:
        task.update()

    return render(request, 'core/bg_tasks.html', {
        'breadcrumbs': breadcrumb_parser(request)
    })


def bg_tasks_delete_all(request):
    logger.debug('deleting all background tasks')

    # delete all Combine Background Tasks
    cts = models.CombineBackgroundTask.objects.all()
    for ct in cts:
        ct.delete()

    return redirect('bg_tasks')


def bg_task(request, task_id):
    # get task
    ct = models.CombineBackgroundTask.objects.get(pk=int(task_id))
    logger.debug('retrieving task: %s' % ct)

    # include job if mentioned in task params
    if 'job_id' in ct.task_params:
        cjob = models.CombineJob.get_combine_job(ct.task_params['job_id'])
    else:
        cjob = None

    return render(request, 'core/bg_task.html', {
        'ct': ct,
        'cjob': cjob,
        'breadcrumbs': breadcrumb_parser(request)
    })


def bg_task_delete(request, task_id):
    # get task
    ct = models.CombineBackgroundTask.objects.get(pk=int(task_id))
    logger.debug('deleting task: %s' % ct)

    ct.delete()

    return redirect('bg_tasks')


def bg_task_cancel(request, task_id):
    # get task
    ct = models.CombineBackgroundTask.objects.get(pk=int(task_id))
    logger.debug('cancelling task: %s' % ct)

    # cancel
    ct.cancel()

    return redirect('bg_tasks')
