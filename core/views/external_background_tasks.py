import logging

from django.contrib.auth.decorators import login_required
from django.db.models.query import QuerySet
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

from core import models

from core.celery import celery_app

from core.mongo import settings

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


@login_required
def system(request):
    # single Livy session
    LOGGER.debug("checking for active Livy session")
    livy_session = models.LivySession.get_active_session()

    # if session found, refresh
    if type(livy_session) == models.LivySession:

        # refresh
        livy_session.refresh_from_livy()

        # create and append to list
        livy_sessions = [livy_session]

    elif type(livy_session) == QuerySet:

        # loop and refresh
        for session in livy_session:
            session.refresh_from_livy()

        # set as list
        livy_sessions = livy_session

    else:
        livy_sessions = livy_session

    # get status of background jobs
    if not hasattr(settings, 'COMBINE_DEPLOYMENT') or settings.COMBINE_DEPLOYMENT != 'docker':
        try:
            supervisor = models.SupervisorRPCClient()
            bgtasks_proc = supervisor.check_process('celery')
        except:
            LOGGER.debug('supervisor might be down?')
            bgtasks_proc = None
    else:
        bgtasks_proc = None

    # get celery worker status
    active_tasks = celery_app.control.inspect().active()

    if active_tasks is None:
        celery_status = 'stopped'
    else:
        if not next(iter(active_tasks.values())):
            celery_status = 'idle'
        elif next(iter(active_tasks.values())):
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
    LOGGER.debug('Checking for pre-existing livy sessions')

    # get active livy sessions
    active_ls = models.LivySession.get_active_session()

    # none found
    if not active_ls:
        LOGGER.debug('active livy session not found, starting')
        livy_session = models.LivySession()
        livy_session.start_session()

    elif type(active_ls) == models.LivySession and request.GET.get('restart') == 'true':
        LOGGER.debug(
            'single, active session found, and restart flag passed, restarting')

        # restart
        active_ls.restart_session()

    # redirect
    return redirect('system')


@login_required
def livy_session_stop(request, session_id):
    LOGGER.debug('stopping Livy session by Combine ID: %s', session_id)

    livy_session = models.LivySession.objects.filter(id=session_id).first()

    if livy_session is not None:
        # attempt to stop with Livy
        models.LivyClient.stop_session(livy_session.session_id)

        # remove from DB
        livy_session.delete()

    # redirect
    return redirect('system')


@login_required
def bgtasks_proc_action(request, proc_action):
    LOGGER.debug('performing %s on bgtasks_proc', proc_action)

    # get supervisor handle
    supervisor = models.SupervisorRPCClient()

    # fire action
    actions = {
        'start': supervisor.start_process,
        'restart': supervisor.restart_process,
        'stop': supervisor.stop_process
    }
    results = actions[proc_action]('celery')
    LOGGER.debug(results)

    # redirect
    return redirect('system')


@login_required
def bgtasks_proc_stderr_log(request):
    # get supervisor handle
    supervisor = models.SupervisorRPCClient()

    log_tail = supervisor.stderr_log_tail('celery')

    # redirect
    return HttpResponse(log_tail, content_type='text/plain')


def system_bg_status(request):
    """
    View to return status on:
        - Livy session
        - celery worker
    """

    # get livy status
    livy_session = models.LivySession.get_active_session()
    if livy_session:
        if type(livy_session) == models.LivySession:
            # refresh single session
            livy_session.refresh_from_livy()
            # set status
            livy_status = livy_session.status
    else:
        livy_status = 'stopped'

    # get celery worker status
    active_tasks = celery_app.control.inspect().active()

    if active_tasks is None:
        celery_status = 'stopped'
    else:
        # TODO: what?
        if next(iter(active_tasks.values())):
            celery_status = 'idle'
        elif next(iter(active_tasks.values())):
            celery_status = 'busy'
        else:
            celery_status = 'unknown'

    # return json
    return JsonResponse({
        'celery_status': celery_status,
        'livy_status': livy_status
    })