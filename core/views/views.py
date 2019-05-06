# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import ast
import json
import logging
import re
from urllib.parse import urlencode

# django
from django.contrib.auth.decorators import login_required
from django.core.urlresolvers import reverse
from django.db.models.query import QuerySet
from django.http import HttpResponse, JsonResponse, FileResponse
from django.shortcuts import render, redirect

# import models
from core import models

# import oai server
from core.oai import OAIProvider

# import background tasks
from core import tasks

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
# Job Validation Report       									   #
####################################################################

@login_required
def job_reports_create_validation(request, org_id, record_group_id, job_id):
    """
    Generate job report based on validation results
    """

    # retrieve job
    cjob = models.CombineJob.get_combine_job(int(job_id))

    # if GET, prepare form
    if request.method == 'GET':

        # mapped field analysis, generate if not part of job_details
        if 'mapped_field_analysis' in cjob.job.job_details_dict.keys():
            field_counts = cjob.job.job_details_dict['mapped_field_analysis']
        else:
            if cjob.job.finished:
                field_counts = cjob.count_indexed_fields()
                cjob.job.update_job_details({'mapped_field_analysis': field_counts}, save=True)
            else:
                logger.debug('job not finished, not setting')
                field_counts = {}

        # render page
        return render(request, 'core/job_reports_create_validation.html', {
            'cjob': cjob,
            'field_counts': field_counts,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, generate report
    if request.method == 'POST':

        # get job name for Combine Task
        report_name = request.POST.get('report_name')
        if report_name == '':
            report_name = 'j_%s_validation_report' % cjob.job.id
            combine_task_name = "Validation Report: %s" % cjob.job.name
        else:
            combine_task_name = "Validation Report: %s" % report_name

        # handle POST params and save as Combine task params
        task_params = {
            'job_id': cjob.job.id,
            'report_name': report_name,
            'report_format': request.POST.get('report_format'),
            'compression_type': request.POST.get('compression_type'),
            'validation_scenarios': request.POST.getlist('validation_scenario', []),
            'mapped_field_include': request.POST.getlist('mapped_field_include', [])
        }

        # cast to int
        task_params['validation_scenarios'] = [int(vs_id) for vs_id in task_params['validation_scenarios']]

        # remove select, reserved fields if in mapped field request
        task_params['mapped_field_include'] = [f for f in task_params['mapped_field_include'] if
                                               f not in ['record_id', 'db_id', 'oid', '_id']]

        # initiate Combine BG Task
        ct = models.CombineBackgroundTask(
            name=combine_task_name,
            task_type='validation_report',
            task_params_json=json.dumps(task_params)
        )
        ct.save()

        # run celery task
        background_task = tasks.create_validation_report.delay(ct.id)
        logger.debug('firing bg task: %s' % background_task)
        ct.celery_task_id = background_task.task_id
        ct.save()

        # redirect to Background Tasks
        return redirect('bg_tasks')


@login_required
def job_update(request, org_id, record_group_id, job_id):
    """
    Update Job in one of several ways:
        - re-map and index
        - run new / different validations
    """

    # retrieve job
    cjob = models.CombineJob.get_combine_job(int(job_id))

    # if GET, prepare form
    if request.method == 'GET':
        # get validation scenarios
        validation_scenarios = models.ValidationScenario.objects.all()

        # get field mappers
        field_mappers = models.FieldMapper.objects.all()
        orig_fm_config_json = cjob.job.get_fm_config_json()

        # get all bulk downloads
        bulk_downloads = models.DPLABulkDataDownload.objects.all()

        # get update type from GET params
        update_type = request.GET.get('update_type', None)

        # render page
        return render(request, 'core/job_update.html', {
            'cjob': cjob,
            'update_type': update_type,
            'validation_scenarios': validation_scenarios,
            'field_mappers': field_mappers,
            'bulk_downloads': bulk_downloads,
            'xml2kvp_handle': models.XML2kvp(),
            'orig_fm_config_json': orig_fm_config_json,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        logger.debug('updating job')
        logger.debug(request.POST)

        # retrieve job
        cjob = models.CombineJob.get_combine_job(int(job_id))

        # get update type
        update_type = request.POST.get('update_type', None)
        logger.debug('running job update: %s' % update_type)

        # handle re-index
        if update_type == 'reindex':
            # get preferred metadata index mapper
            fm_config_json = request.POST.get('fm_config_json')

            # init re-index
            cjob.reindex_bg_task(fm_config_json=fm_config_json)

            # set gms
            gmc = models.GlobalMessageClient(request.session)
            gmc.add_gm({
                'html': '<p><strong>Re-Indexing Job:</strong><br>%s</p>'
                        '<p><a href="%s"><button type="button" '
                        'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                            cjob.job.name, reverse('bg_tasks')),
                'class': 'success'
            })

            return redirect('job_details',
                            org_id=cjob.job.record_group.organization.id,
                            record_group_id=cjob.job.record_group.id,
                            job_id=cjob.job.id)

        # handle new validations
        if update_type == 'validations':
            # get requested validation scenarios
            validation_scenarios = request.POST.getlist('validation_scenario', [])

            # get validations
            validations = models.ValidationScenario.objects.filter(
                id__in=[int(vs_id) for vs_id in validation_scenarios])

            # init bg task
            cjob.new_validations_bg_task([vs.id for vs in validations])

            # set gms
            gmc = models.GlobalMessageClient(request.session)
            gmc.add_gm({
                'html': '<p><strong>Running New Validations for Job:</strong><br>%s<br>'
                        '<br><strong>Validation Scenarios:</strong><br>%s</p>'
                        '<p><a href="%s"><button type="button" '
                        'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                            cjob.job.name, '<br>'.join([vs.name for vs in validations]), reverse('bg_tasks')),
                'class': 'success'
            })

            return redirect('job_details',
                            org_id=cjob.job.record_group.organization.id,
                            record_group_id=cjob.job.record_group.id,
                            job_id=cjob.job.id)

        # handle validation removal
        if update_type == 'remove_validation':
            # get validation scenario to remove
            jv_id = request.POST.get('jv_id', False)

            # initiate Combine BG Task
            cjob.remove_validation_bg_task(jv_id)

            # set gms
            vs = models.JobValidation.objects.get(pk=int(jv_id)).validation_scenario
            gmc = models.GlobalMessageClient(request.session)
            gmc.add_gm({
                'html': '<p><strong>Removing Validation for Job:</strong><br>%s<br><br>'
                        '<strong>Validation Scenario:</strong><br>%s</p><p><a href="%s"><button type="button" '
                        'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                            cjob.job.name, vs.name, reverse('bg_tasks')),
                'class': 'success'
            })

            return redirect('job_details',
                            org_id=cjob.job.record_group.organization.id,
                            record_group_id=cjob.job.record_group.id,
                            job_id=cjob.job.id)

        # handle validation removal
        if update_type == 'dbdm':
            # get validation scenario to remove
            dbdd_id = request.POST.get('dbdd', False)

            # initiate Combine BG Task
            cjob.dbdm_bg_task(dbdd_id)

            # set gms
            dbdd = models.DPLABulkDataDownload.objects.get(pk=int(dbdd_id))
            gmc = models.GlobalMessageClient(request.session)
            gmc.add_gm({
                'html': '<p><strong>Running DPLA Bulk Data comparison for Job:</strong><br>%s<br><br>'
                        '<strong>Bulk Data S3 key:</strong><br>%s</p><p><a href="%s"><button type="button" '
                        'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                            cjob.job.name, dbdd.s3_key, reverse('bg_tasks')),
                'class': 'success'
            })

            return redirect('job_details',
                            org_id=cjob.job.record_group.organization.id,
                            record_group_id=cjob.job.record_group.id,
                            job_id=cjob.job.id)


####################################################################
# Job Validation Report       									   #
####################################################################

def document_download(request):
    """
    Args (GET params):
        file_location: location on disk for file
        file_download_name: desired download name
        content_type: ContentType Headers
    """

    # known download format params
    download_format_hash = {
        'excel': {
            'extension': '.xlsx',
            'content_type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        },
        'csv': {
            'extension': '.csv',
            'content_type': 'text/plain'
        },
        'tsv': {
            'extension': '.tsv',
            'content_type': 'text/plain'
        },
        'json': {
            'extension': '.json',
            'content_type': 'text/plain'
        },
        'zip': {
            'extension': '.zip',
            'content_type': 'application/zip'
        },
        'targz': {
            'extension': '.tar.gz',
            'content_type': 'application/gzip'
        }
    }

    # get params
    download_format = request.GET.get('download_format', None)
    filepath = request.GET.get('filepath', None)
    name = request.GET.get('name', 'download')
    # TODO: content_type is never used
    content_type = request.GET.get('content_type', 'text/plain')
    preview = request.GET.get('preview', False)

    # if known download format, use hash and overwrite provided or defaults
    if download_format and download_format in download_format_hash.keys():
        format_params = download_format_hash[download_format]
        name = '%s%s' % (name, format_params['extension'])
        content_type = format_params['content_type']

    # generate response
    response = FileResponse(open(filepath, 'rb'))
    if not preview:
        response['Content-Disposition'] = 'attachment; filename="%s"' % name
    return response


####################################################################
# Jobs QA	                   									   #
####################################################################

@login_required
def field_analysis(request, es_index):
    # get field name
    field_name = request.GET.get('field_name')

    # get ESIndex, evaluating stringified list
    esi = models.ESIndex(ast.literal_eval(es_index))

    # get analysis for field
    field_metrics = esi.field_analysis(field_name, metrics_only=True)

    # return
    return render(request, 'core/field_analysis.html', {
        'esi': esi,
        'field_name': field_name,
        'field_metrics': field_metrics,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_indexing_failures(request, org_id, record_group_id, job_id):
    # get CombineJob
    cjob = models.CombineJob.get_combine_job(job_id)

    # return
    return render(request, 'core/job_indexing_failures.html', {
        'cjob': cjob,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def field_analysis_docs(request, es_index, filter_type):
    """

    Table of documents that match a filtered ES query.

    Args:
        es_index (str): string ES index name
        filter_type (str): what kind of filtering to impose on documents returned
    """

    # regardless of filtering type, get field name
    field_name = request.GET.get('field_name')

    # get ESIndex
    esi = models.ESIndex(ast.literal_eval(es_index))

    # begin construction of DT GET params with 'fields_names'
    dt_get_params = [
        ('field_names', 'db_id'),  # get DB ID
        ('field_names', 'combine_id'),  # get Combine ID
        ('field_names', 'record_id'),  # get ID from ES index document
        ('field_names', field_name),  # add field to returned fields
        ('filter_field', field_name),
        ('filter_type', filter_type)
    ]

    # analysis scenario dict
    analysis_scenario = {
        'exists': None,
        'matches': None,
        'value': None
    }

    # field existence
    if filter_type == 'exists':
        # if check exists, get expected GET params
        exists = request.GET.get('exists')
        dt_get_params.append(('exists', exists))

        # update analysis scenario dict
        analysis_scenario['exists'] = exists

    # field equals
    if filter_type == 'equals':

        # if check equals, get expected GET params
        matches = request.GET.get('matches')
        dt_get_params.append(('matches', matches))

        value = request.GET.get('value', None)  # default None if checking non-matches to value
        if value:
            dt_get_params.append(('filter_value', value))

        # update analysis scenario dict
        analysis_scenario['matches'] = matches
        analysis_scenario['value'] = value

    # construct DT Ajax GET parameters string from tuples
    dt_get_params_string = urlencode(dt_get_params)

    # return
    return render(request, 'core/field_analysis_docs.html', {
        'esi': esi,
        'field_name': field_name,
        'filter_type': filter_type,
        'analysis_scenario': analysis_scenario,
        'msg': None,
        'dt_get_params_string': dt_get_params_string,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_validation_scenario_failures(request, org_id, record_group_id, job_id, job_validation_id):
    # get CombineJob
    cjob = models.CombineJob.get_combine_job(job_id)

    # get job validation instance
    jv = models.JobValidation.objects.get(pk=int(job_validation_id))

    # return
    return render(request, 'core/job_validation_scenario_failures.html', {
        'cjob': cjob,
        'jv': jv,
        'breadcrumbs': breadcrumb_parser(request)
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
# Analysis  													   #
####################################################################

def analysis(request):
    """
    Analysis home
    """

    # get all jobs associated with record group
    analysis_jobs = models.Job.objects.filter(job_type='AnalysisJob')

    # get analysis jobs hierarchy
    analysis_hierarchy = models.AnalysisJob.get_analysis_hierarchy()

    # get analysis jobs lineage
    analysis_job_lineage = models.Job.get_all_jobs_lineage(
        organization=analysis_hierarchy['organization'],
        record_group=analysis_hierarchy['record_group'],
        exclude_analysis_jobs=False
    )

    # loop through jobs
    for job in analysis_jobs:
        # update status
        job.update_status()

    # render page
    return render(request, 'core/analysis.html', {
        'jobs': analysis_jobs,
        'job_lineage_json': json.dumps(analysis_job_lineage),
        'for_analysis': True,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_analysis(request):
    """
    Run new analysis job
    """

    # if GET, prepare form
    if request.method == 'GET':

        # retrieve jobs (limiting if needed)
        input_jobs = models.Job.objects.all()

        # limit if analysis_type set
        analysis_type = request.GET.get('type', None)
        subset = request.GET.get('subset', None)
        if analysis_type == 'published':

            # load PublishedRecords
            published = models.PublishedRecords(subset=subset)

            # define input_jobs
            input_jobs = published.published_jobs

        else:
            published = None

        # get validation scenarios
        validation_scenarios = models.ValidationScenario.objects.all()

        # get field mappers
        field_mappers = models.FieldMapper.objects.all()

        # get record identifier transformation scenarios
        rits = models.RecordIdentifierTransformationScenario.objects.all()

        # get job lineage for all jobs (filtered to input jobs scope)
        ld = models.Job.get_all_jobs_lineage(jobs_query_set=input_jobs)

        # get all bulk downloads
        bulk_downloads = models.DPLABulkDataDownload.objects.all()

        # render page
        return render(request, 'core/job_analysis.html', {
            'job_select_type': 'multiple',
            'input_jobs': input_jobs,
            'published': published,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': models.XML2kvp(),
            'analysis_type': analysis_type,
            'bulk_downloads': bulk_downloads,
            'job_lineage_json': json.dumps(ld)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = models.CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=models.AnalysisJob,
            job_params=request.POST)

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if job_status is False:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('analysis')


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
