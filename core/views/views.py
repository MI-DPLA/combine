# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import json
import logging
import re

# django
from django.contrib.auth.decorators import login_required
from django.core.urlresolvers import reverse
from django.db.models.query import QuerySet
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

# import models
from core import models

# import oai server
from core.oai import OAIProvider

# import mongo dependencies
from core.mongo import *

# import celery app
from core.celery import celery_app

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


# breadcrumb parser
def breadcrumb_parser(request):
    """
    Rudimentary breadcrumbs parser
    """

    crumbs = []

    # livy/spark
    regex_match = re.match(r'(.+?/livy_sessions)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>Livy/Spark</span>", reverse('livy_sessions')))

    # configurations
    regex_match = re.match(r'(.+?/configuration)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>Configuration</span>", reverse('configuration')))

    # search
    regex_match = re.match(r'(.+?/search)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>Search</span>", reverse('search')))

    # configurations/test_validation_scenario
    regex_match = re.match(r'(.+?/configuration/test_validation_scenario)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Test Validation Scenario</span>", reverse('test_validation_scenario')))

    # all jobs
    regex_match = re.match(r'(.+?/jobs/all)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>All Jobs</span>", reverse('all_jobs')))

    # analysis
    regex_match = re.match(r'(.+?/analysis)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>Analysis</span>", reverse('analysis')))

    # field analysis
    regex_match = re.match(r'(.+?/analysis/es/index/j([0-9]+)/field_analysis.*)', request.path)
    if regex_match:

        # get job
        j = models.Job.objects.get(pk=int(regex_match.group(2)))

        # get field for analysis
        field_name = request.GET.get('field_name', None)

        # append crumbs
        if j.record_group.organization.for_analysis:
            logger.debug("breadcrumbs: org is for analysis, skipping")
        else:
            crumbs.append((
                "<span class='font-weight-bold'>Organization</span> - <code>%s</code>" % j.record_group.organization.name,
                reverse('organization', kwargs={'org_id': j.record_group.organization.id})))
        if j.record_group.for_analysis:
            logger.debug("breadcrumbs: rg is for analysis, skipping")
        else:
            crumbs.append(("<span class='font-weight-bold'>RecordGroup</span> - <code>%s</code>" % j.record_group.name,
                           reverse('record_group', kwargs={'org_id': j.record_group.organization.id,
                                                           'record_group_id': j.record_group.id})))
        crumbs.append(("<span class='font-weight-bold'>Job</span> - <code>%s</code>" % j.name,
                       reverse('job_details', kwargs=dict(org_id=j.record_group.organization.id,
                                                          record_group_id=j.record_group.id,
                                                          job_id=j.id))))
        crumbs.append(("<span class='font-weight-bold'>Field Analysis - <code>%s</code></span>" % field_name,
                       '%s?%s' % (regex_match.group(1), request.META['QUERY_STRING'])))

    # published
    pub_m = re.match(r'(.+?/published.*)', request.path)
    if pub_m:
        crumbs.append(("<span class='font-weight-bold'>Published</span>", reverse('published')))

    # published subset create
    pub_m = re.match(r'(.+?/published/subsets/create)', request.path)
    if pub_m:
        crumbs.append(
            ("<span class='font-weight-bold'>Published Subset Create</span>", reverse('published_subset_create')))

    # published subset
    pub_m = re.match(r'(.+?/published/subset/(.+))', request.path)
    if pub_m:
        crumbs.append(("<span class='font-weight-bold'>Published Subset: <code>%s</code></span>" % pub_m.group(2),
                       reverse('published_subset', kwargs={'subset': pub_m.group(2)})))

    # organization
    pub_m = re.match(r'(.+?/organization/.*)', request.path)
    if pub_m:
        crumbs.append(("<span class='font-weight-bold'>Organizations</span>", reverse('organizations')))

    # org
    org_m = re.match(r'(.+?/organization/([0-9]+))', request.path)
    if org_m:
        org = models.Organization.objects.get(pk=int(org_m.group(2)))
        if org.for_analysis:
            logger.debug("breadcrumbs: org is for analysis, converting breadcrumbs")
            crumbs.append(("<span class='font-weight-bold'>Analysis</span>", reverse('analysis')))
        else:
            crumbs.append(
                ("<span class='font-weight-bold'>Organization</span> - <code>%s</code>" % org.name, org_m.group(1)))

    # record_group
    rg_m = re.match(r'(.+?/record_group/([0-9]+))', request.path)
    if rg_m:
        rg = models.RecordGroup.objects.get(pk=int(rg_m.group(2)))
        if rg.for_analysis:
            logger.debug("breadcrumbs: rg is for analysis, converting breadcrumbs")
        else:
            crumbs.append(
                ("<span class='font-weight-bold'>RecordGroup</span> - <code>%s</code>" % rg.name, rg_m.group(1)))

    # job
    j_m = re.match(r'(.+?/job/([0-9]+))', request.path)
    if j_m:
        j = models.Job.objects.get(pk=int(j_m.group(2)))
        if j.record_group.for_analysis:
            crumbs.append(("<span class='font-weight-bold'>Analysis</span> - %s" % j.name, j_m.group(1)))
        else:
            crumbs.append(("<span class='font-weight-bold'>Job</span> - <code>%s</code>" % j.name, j_m.group(1)))

    # record
    r_m = re.match(r'(.+?/record/([0-9a-z]+))', request.path)
    if r_m:
        r = models.Record.objects.get(id=r_m.group(2))
        crumbs.append(("<span class='font-weight-bold'>Record</span> - <code>%s</code>" % r.record_id, r_m.group(1)))

    # background tasks
    regex_match = re.match(r'(.+?/background_tasks)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>Background Tasks</span>", reverse('bg_tasks')))

    # background task
    regex_match = re.match(r'(.+?/background_tasks/task/([0-9]+))', request.path)
    if regex_match:
        background_task = models.CombineBackgroundTask.objects.get(pk=int(regex_match.group(2)))
        crumbs.append(
            ("<span class='font-weight-bold'>Task - <code>%s</code></span>" % background_task.name,
             reverse('bg_tasks')))

    # stateio
    regex_match = re.match(r'(.+?/stateio.*)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>State Export/Import</span>", reverse('stateio')))

    # stateio - state details
    regex_match = re.match(r'(.+?/stateio/state/([0-9a-z].*))', request.path)
    if regex_match:
        state = models.StateIO.objects.get(id=regex_match.group(2))
        crumbs.append(("<span class='font-weight-bold'>State - <code>%s</code></span>" % state.name,
                       reverse('stateio_state', kwargs={'state_id': regex_match.group(2)})))

    # stateio - export
    regex_match = re.match(r'(.+?/stateio/export.*)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>Export</span>", reverse('stateio_export')))

    # stateio - export
    regex_match = re.match(r'(.+?/stateio/import.*)', request.path)
    if regex_match:
        crumbs.append(("<span class='font-weight-bold'>Import</span>", reverse('stateio_import')))

    # return
    return crumbs


####################################################################
# Index 														   #
####################################################################

@login_required
def index(request):
    # get username
    username = request.user.username

    # get all organizations
    orgs = models.Organization.objects.exclude(for_analysis=True).all()

    # get record count
    record_count = models.Record.objects.all().count()

    # get published records count
    pr = models.PublishedRecords()
    published_record_count = pr.records.count()

    # get job count
    job_count = models.Job.objects.all().count()

    return render(request, 'core/index.html', {
        'username': username,
        'orgs': orgs,
        'record_count': "{:,}".format(record_count),
        'published_record_count': "{:,}".format(published_record_count),
        'job_count': "{:,}".format(job_count)
    })


####################################################################
# User Livy Sessions and Celery 								   #
####################################################################

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
        logger.debug('single, active session found, and restart flag passed, restarting')

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



####################################################################
# Configuration 												   #
####################################################################

@login_required
def configuration(request):
    # get all transformations
    transformations = models.Transformation.objects.filter(use_as_include=False)

    # get all OAI endpoints
    oai_endpoints = models.OAIEndpoint.objects.all()

    # get all validation scenarios
    validation_scenarios = models.ValidationScenario.objects.all()

    # get record identifier transformation scenarios
    rits = models.RecordIdentifierTransformationScenario.objects.all()

    # get all bulk downloads
    bulk_downloads = models.DPLABulkDataDownload.objects.all()

    # get field mappers
    field_mappers = models.FieldMapper.objects.all()

    # return
    return render(request, 'core/configuration.html', {
        'transformations': transformations,
        'oai_endpoints': oai_endpoints,
        'validation_scenarios': validation_scenarios,
        'rits': rits,
        'field_mappers': field_mappers,
        'bulk_downloads': bulk_downloads,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def oai_endpoint_payload(request, oai_endpoint_id):
    """
    Return JSON of saved OAI endpoint information
    """

    # retrieve OAIEndpoint
    oai_endpoint = models.OAIEndpoint.objects.get(pk=oai_endpoint_id)

    # pop state
    oai_endpoint.__dict__.pop('_state')

    # return as json
    return JsonResponse(oai_endpoint.__dict__)


@login_required
def dpla_bulk_data_download(request):
    """
    View to support the downloading of DPLA bulk data
    """

    if request.method == 'GET':

        # if S3 credentials set
        if settings.AWS_ACCESS_KEY_ID and \
                settings.AWS_SECRET_ACCESS_KEY and \
                settings.AWS_ACCESS_KEY_ID is not None and \
                settings.AWS_SECRET_ACCESS_KEY is not None:

            # get DPLABulkDataClient and keys from DPLA bulk download
            dbdc = models.DPLABulkDataClient()
            bulk_data_keys = dbdc.retrieve_keys()

        else:
            bulk_data_keys = False

        # return
        return render(request, 'core/dpla_bulk_data_download.html', {
            'bulk_data_keys': bulk_data_keys,
            'breadcrumbs': breadcrumb_parser(request)
        })

    if request.method == 'POST':
        # OLD ######################################################################
        logger.debug('initiating bulk data download')

        # get DPLABulkDataClient
        dbdc = models.DPLABulkDataClient()

        # initiate download
        dbdc.download_and_index_bulk_data(request.POST.get('object_key', None))

        # return to configuration screen
        return redirect('configuration')


####################################################################
# OAI Server 													   #
####################################################################

def oai(request, subset=None):
    """
    Parse GET parameters, send to OAIProvider instance from oai.py
    Return XML results
    """

    # get OAIProvider instance
    op = OAIProvider(request.GET, subset=subset)

    # return XML
    return HttpResponse(op.generate_response(), content_type='text/xml')


####################################################################
# Global Search													   #
####################################################################

def search(request):
    """
    Global search of Records
    """

    # if search term present, use
    q = request.GET.get('q', None)
    if q:
        search_params = json.dumps({'q': q})
        logger.debug(search_params)
    else:
        search_params = None

    # generate hierarchy_dict
    job_hierarchy = _stateio_prepare_job_hierarchy()

    return render(request, 'core/search.html', {
        'search_string': q,
        'search_params': search_params,
        'job_hierarchy_json': json.dumps(job_hierarchy),
        'breadcrumbs': breadcrumb_parser(request),
        'page_title': ' | Search'
    })




####################################################################
# Global Messages												   #
####################################################################

@login_required
def gm_delete(request):
    if request.method == 'POST':
        # get gm_id
        gm_id = request.POST.get('gm_id')

        # init GlobalMessageClient
        gmc = models.GlobalMessageClient(request.session)

        # delete by id
        results = gmc.delete_gm(gm_id)

        # redirect
        return JsonResponse({
            'gm_id': gm_id,
            'num_removed': results
        })
