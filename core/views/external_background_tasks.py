import logging

from django.contrib.auth.decorators import login_required
from django.db.models.query import QuerySet
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

from core import models

from core.celery import celery_app

from core.mongo import settings

from .view_helpers import breadcrumb_parser

logger = logging.getLogger(__name__)


@login_required
def system(request):
    # single Livy session
    logger.debug("checking for active Livy session")
    livy_session = models.LivySession.get_active_session()

    # if session found, refresh
    if type(livy_session) == models.LivySession:

        # refresh
        livy_session.refresh_from_livy()

        # create and append to list
        livy_sessions = [livy_session]

    elif type(livy_session) == QuerySet:

        # loop and refresh
        for s in livy_session:
            s.refresh_from_livy()

        # set as list
        livy_sessions = livy_session

    else:
        livy_sessions = livy_session

    # get status of background jobs
    if not hasattr(settings, 'COMBINE_DEPLOYMENT') or settings.COMBINE_DEPLOYMENT != 'docker':
        try:
            sp = models.SupervisorRPCClient()
            bgtasks_proc = sp.check_process('celery')
        except:
            logger.debug('supervisor might be down?')
            bgtasks_proc = None
    else:
        bgtasks_proc = None

    # get celery worker status
    active_tasks = celery_app.control.inspect().active()

    if active_tasks is None:
        celery_status = 'stopped'
    else:
        if len(next(iter(active_tasks.values()))) == 0:
            celery_status = 'idle'
        elif len(next(iter(active_tasks.values()))) > 0:
            celery_status = 'busy'

    # return
    return render(request, 'core/system.html', {
        'livy_session': livy_session,
        'livy_sessions': livy_sessions,
        'celery_status': celery_status,
        'bgtasks_proc': bgtasks_proc,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def livy_session_start(request):
    logger.debug('Checking for pre-existing livy sessions')

    # get active livy sessions
    active_ls = models.LivySession.get_active_session()

    # none found
    if not active_ls:
        logger.debug('active livy session not found, starting')
        livy_session = models.LivySession()
        livy_session.start_session()

    elif type(active_ls) == models.LivySession and request.GET.get('restart') == 'true':
        logger.debug(
            'single, active session found, and restart flag passed, restarting')

        # restart
        active_ls.restart_session()

    # redirect
    return redirect('system')


@login_required
def livy_session_stop(request, session_id):
    logger.debug('stopping Livy session by Combine ID: %s' % session_id)

    livy_session = models.LivySession.objects.filter(id=session_id).first()

    # attempt to stop with Livy
    models.LivyClient.stop_session(livy_session.session_id)

    # remove from DB
    livy_session.delete()

    # redirect
    return redirect('system')


@login_required
def bgtasks_proc_action(request, proc_action):
    logger.debug('performing %s on bgtasks_proc' % proc_action)

    # get supervisor handle
    sp = models.SupervisorRPCClient()

    # fire action
    actions = {
        'start': sp.start_process,
        'restart': sp.restart_process,
        'stop': sp.stop_process
    }
    results = actions[proc_action]('celery')
    logger.debug(results)

    # redirect
    return redirect('system')


@login_required
def bgtasks_proc_stderr_log(request):
    # get supervisor handle
    sp = models.SupervisorRPCClient()

    log_tail = sp.stderr_log_tail('celery')

    # redirect
    return HttpResponse(log_tail, content_type='text/plain')


def system_bg_status(request):
    """
    View to return status on:
        - Livy session
        - celery worker
    """

    # get livy status
    lv = models.LivySession.get_active_session()
    if lv:
        if type(lv) == models.LivySession:
            # refresh single session
            lv.refresh_from_livy()
            # set status
            livy_status = lv.status
    else:
        livy_status = 'stopped'

    # get celery worker status
    active_tasks = celery_app.control.inspect().active()

    if active_tasks is None:
        celery_status = 'stopped'
    else:
        if len(next(iter(active_tasks.values()))) == 0:
            celery_status = 'idle'
        elif len(next(iter(active_tasks.values()))) > 0:
            celery_status = 'busy'

    # return json
    return JsonResponse({
        'celery_status': celery_status,
        'livy_status': livy_status
    })
