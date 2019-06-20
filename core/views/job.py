import ast
import json
import logging
from urllib.parse import urlencode

from django.contrib.auth.decorators import login_required
from django.core.urlresolvers import reverse
from django.http import FileResponse, JsonResponse
from django.shortcuts import redirect, render

from core import tasks, xml2kvp
from core.models import RecordGroup, Job, CombineBackgroundTask, PublishedRecords,\
    CombineJob, AnalysisJob, GlobalMessageClient, OAIEndpoint, TransformJob,\
    MergeJob, RecordIdentifierTransformationScenario, FieldMapper, DPLABulkDataDownload,\
    ValidationScenario, HarvestOAIJob, HarvestStaticXMLJob, Transformation, JobValidation,\
    HarvestTabularDataJob, ESIndex
from core.mongo import mc_handle

from .view_helpers import breadcrumb_parser, bool_for_string

LOGGER = logging.getLogger(__name__)


@login_required
def job_id_redirect(request, job_id):
    """
        Route to redirect to more verbose Jobs URL
        """

    # get job
    job = Job.objects.get(pk=job_id)

    # redirect
    return redirect('job_details',
                    org_id=job.record_group.organization.id,
                    record_group_id=job.record_group.id,
                    job_id=job.id)


@login_required
def all_jobs(request):
    """
    View to show all jobs, across all Organizations, RecordGroups, and Job types

    GET Args:
        include_analysis: if true, include Analysis type jobs
    """

    # get all the record groups.
    record_groups = RecordGroup.objects.exclude(for_analysis=True)

    # capture include_analysis GET param if present
    include_analysis = request.GET.get('include_analysis', False)

    # get all jobs associated with record group
    if include_analysis:
        jobs = Job.objects.all()
    else:
        jobs = Job.objects.exclude(job_type='AnalysisJob').all()

    # get job lineage for all jobs
    if include_analysis:
        job_lineage = Job.get_all_jobs_lineage(exclude_analysis_jobs=False)
    else:
        job_lineage = Job.get_all_jobs_lineage(exclude_analysis_jobs=True)

    # loop through jobs and update status
    for job in jobs:
        job.update_status()

    # render page
    return render(request, 'core/all_jobs.html', {
        'jobs': jobs,
        'record_groups': record_groups,
        'job_lineage_json': json.dumps(job_lineage),
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_delete(request, org_id, record_group_id, job_id):
    LOGGER.debug('deleting job by id: %s', job_id)

    # get job
    job = Job.objects.get(pk=job_id)

    # set job status to deleting
    job.name = "%s (DELETING)" % job.name
    job.deleted = True
    job.status = 'deleting'
    job.save()

    # initiate Combine BG Task
    combine_task = CombineBackgroundTask(
        name='Delete Job: %s' % job.name,
        task_type='delete_model_instance',
        task_params_json=json.dumps({
            'model': 'Job',
            'job_id': job.id
        })
    )
    combine_task.save()

    # run celery task
    bg_task = tasks.delete_model_instance.delay('Job', job.id)
    LOGGER.debug('firing bg task: %s', bg_task)
    combine_task.celery_task_id = bg_task.task_id
    combine_task.save()

    # redirect
    return redirect(request.META.get('HTTP_REFERER'))


@login_required
def stop_jobs(request):
    LOGGER.debug('stopping jobs')

    job_ids = request.POST.getlist('job_ids[]')
    LOGGER.debug(job_ids)

    # get downstream toggle
    downstream_toggle = request.POST.get('downstream_stop_toggle', False)
    if downstream_toggle == 'true':
        downstream_toggle = True
    elif downstream_toggle == 'false':
        downstream_toggle = False

    # set of jobs to rerun
    job_stop_set = set()

    # loop through job_ids
    for job_id in job_ids:

        # get CombineJob
        cjob = CombineJob.get_combine_job(job_id)

        # if including downstream
        if downstream_toggle:

            # add rerun lineage for this job to set
            job_stop_set.update(cjob.job.get_downstream_jobs())

        # else, just job
        else:

            job_stop_set.add(cjob.job)

    # sort and run
    ordered_job_delete_set = sorted(list(job_stop_set), key=lambda j: j.id)

    # # loop through and update visible elements of Job for front-end
    for job in ordered_job_delete_set:
        LOGGER.debug('stopping Job: %s', job)

        # stop job
        job.stop_job()

    # set gms
    gmc = GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<p><strong>Stopped Job(s):</strong><br>%s</p>' % (
            '<br>'.join([j.name for j in ordered_job_delete_set])),
        'class': 'danger'
    })

    # return
    return JsonResponse({'results': True})


@login_required
def delete_jobs(request):
    LOGGER.debug('deleting jobs')

    job_ids = request.POST.getlist('job_ids[]')
    LOGGER.debug(job_ids)

    # get downstream toggle
    downstream_toggle = request.POST.get('downstream_delete_toggle', False)
    if downstream_toggle == 'true':
        downstream_toggle = True
    elif downstream_toggle == 'false':
        downstream_toggle = False

    # set of jobs to delete
    job_delete_set = set()

    # loop through job_ids
    for job_id in job_ids:

        # get CombineJob
        cjob = CombineJob.get_combine_job(job_id)

        # if including downstream
        if downstream_toggle:

            # add delete lineage for this job to set
            job_delete_set.update(cjob.job.get_downstream_jobs())

        # else, just job
        else:

            job_delete_set.add(cjob.job)

    # sort and run
    ordered_job_delete_set = sorted(list(job_delete_set), key=lambda j: j.id)

    # # loop through and update visible elements of Job for front-end
    for job in ordered_job_delete_set:
        LOGGER.debug('deleting Job: %s', job)

        # set job status to deleting
        job.name = "%s (DELETING)" % job.name
        job.deleted = True
        job.status = 'deleting'
        job.save()

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Delete Job: #%s' % job.name,
            task_type='delete_model_instance',
            task_params_json=json.dumps({
                'model': 'Job',
                'job_id': job.id
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.delete_model_instance.delay('Job', job.id, )
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()

    # set gms
    gmc = GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<p><strong>Deleting Job(s):</strong><br>%s</p><p>Refresh this page to update status of removing Jobs. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button></p>' % (
            '<br>'.join([j.name for j in ordered_job_delete_set])),
        'class': 'danger'
    })

    # return
    return JsonResponse({'results': True})


@login_required
def move_jobs(request):
    LOGGER.debug('moving jobs')

    job_ids = request.POST.getlist('job_ids[]')
    record_group_id = request.POST.getlist('record_group_id')[0]

    # get downstream toggle
    downstream_toggle = request.POST.get('downstream_move_toggle', False)
    if downstream_toggle == 'true':
        downstream_toggle = True
    elif downstream_toggle == 'false':
        downstream_toggle = False

    # set of jobs to move
    job_move_set = set()

    # loop through job_ids
    for job_id in job_ids:

        # get CombineJob
        cjob = CombineJob.get_combine_job(job_id)

        # if including downstream
        if downstream_toggle:

            # add move lineage for this job to set
            job_move_set.update(cjob.job.get_downstream_jobs())

        # else, just job
        else:

            job_move_set.add(cjob.job)

    # sort and run
    ordered_job_move_set = sorted(list(job_move_set), key=lambda j: j.id)

    # loop through jobs
    for job in ordered_job_move_set:
        LOGGER.debug('moving Job: %s', job)

        new_record_group = RecordGroup.objects.get(pk=record_group_id)
        job.record_group = new_record_group
        job.save()

        LOGGER.debug('Job %s has been moved', job)

    # redirect
    return JsonResponse({'results': True})


@login_required
def job_details(request, org_id, record_group_id, job_id):
    LOGGER.debug('details for job id: %s', job_id)

    # get CombineJob
    cjob = CombineJob.get_combine_job(job_id)

    # update status
    cjob.job.update_status()

    # detailed record count
    record_count_details = cjob.job.get_detailed_job_record_count()

    # get job lineage
    job_lineage = cjob.job.get_lineage()

    # get dpla_bulk_data_match
    dpla_bulk_data_matches = cjob.job.get_dpla_bulk_data_matches()

    # check if limiting to one, pre-existing record
    get_q = request.GET.get('q', None)

    # job details and job type specific augment
    job_detail = cjob.job.job_details_dict

    # mapped field analysis, generate if not part of job_details
    if 'mapped_field_analysis' in job_detail.keys():
        field_counts = job_detail['mapped_field_analysis']
    else:
        if cjob.job.finished:
            field_counts = cjob.count_indexed_fields()
            cjob.job.update_job_details(
                {'mapped_field_analysis': field_counts}, save=True)
        else:
            LOGGER.debug('job not finished, not setting')
            field_counts = {}

    # TODO: What is this accomplishing?
    # OAI Harvest
    if isinstance(cjob, HarvestOAIJob):
        pass

    # Static Harvest
    elif isinstance(cjob, HarvestStaticXMLJob):
        pass

    # Transform
    elif isinstance(cjob, TransformJob):
        pass

    # Merge/Duplicate
    elif isinstance(cjob, MergeJob):
        pass

    # Analysis
    elif isinstance(cjob, AnalysisJob):
        pass

    # get published records, primarily for published sets
    pub_records = PublishedRecords()

    # get published subsets with PublishedRecords static method
    published_subsets = PublishedRecords.get_subsets()

    # loop through subsets and enrich
    for _ in published_subsets:

        # add counts
        counts = mc_handle.combine.misc.find_one(
            {'_id': 'published_field_counts_%s' % _['name']})

        # if counts not yet calculated, do now
        if counts is None:
            counts = PublishedRecords(
                subset=_['name']).count_indexed_fields()
        _['counts'] = counts

    # get field mappers
    field_mappers = FieldMapper.objects.all()

    # return
    return render(request, 'core/job_details.html', {
        'cjob': cjob,
        'record_group': cjob.job.record_group,
        'record_count_details': record_count_details,
        'field_counts': field_counts,
        'field_mappers': field_mappers,
        'xml2kvp_handle': xml2kvp.XML2kvp(),
        'job_lineage_json': json.dumps(job_lineage),
        'dpla_bulk_data_matches': dpla_bulk_data_matches,
        'q': get_q,
        'job_details': job_detail,
        'pr': pub_records,
        'published_subsets': published_subsets,
        'es_index_str': cjob.esi.es_index_str,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_errors(request, org_id, record_group_id, job_id):
    LOGGER.debug('retrieving errors for job id: %s', job_id)

    # get CombineJob
    cjob = CombineJob.get_combine_job(job_id)

    job_error_list = cjob.get_job_errors()

    # return
    return render(request, 'core/job_errors.html', {
        'cjob': cjob,
        'job_errors': job_error_list,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_update_note(request, org_id, record_group_id, job_id):
    if request.method == 'POST':

        # get CombineJob
        cjob = CombineJob.get_combine_job(job_id)

        # get job note
        job_note = request.POST.get('job_note')
        if job_note == '':
            job_note = None

        # update job note
        cjob.job.note = job_note
        cjob.job.save()

        # redirect
        return redirect(request.META.get('HTTP_REFERER'))


@login_required
def job_update_name(request, org_id, record_group_id, job_id):
    if request.method == 'POST':

        # get CombineJob
        cjob = CombineJob.get_combine_job(job_id)

        # get job note
        job_name = request.POST.get('job_name')
        if job_name == '':
            job_name = None

        # update job note
        cjob.job.name = job_name
        cjob.job.save()

        # redirect
        return redirect(request.META.get('HTTP_REFERER'))


@login_required
def job_publish(request, org_id, record_group_id, job_id):
    LOGGER.debug(request.POST)

    # capture entered publish set id
    publish_set_id = request.POST.get('publish_set_id', None)

    # override with pre-existing publish set id is selected
    if request.POST.get('existing_publish_set_id', None) is not None:
        publish_set_id = request.POST.get('existing_publish_set_id')

    # get published subsets to include in
    published_subsets = request.POST.getlist('published_subsets', [])

    # get CombineJob
    cjob = CombineJob.get_combine_job(job_id)

    # init publish
    cjob.publish_bg_task(
        publish_set_id=publish_set_id,
        in_published_subsets=published_subsets)

    # set gms
    gmc = GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<p><strong>Publishing Job:</strong><br>%s<br><br><strong>Publish Set ID:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Published Records</button></a></p>' % (
            cjob.job.name, publish_set_id, reverse('published')),
        'class': 'success'
    })

    return redirect('record_group',
                    org_id=cjob.job.record_group.organization.id,
                    record_group_id=cjob.job.record_group.id)


@login_required
def job_unpublish(request, org_id, record_group_id, job_id):
    # get CombineJob
    cjob = CombineJob.get_combine_job(job_id)

    # init unpublish
    cjob.unpublish_bg_task()

    # set gms
    gmc = GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<p><strong>Unpublishing Job:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Published Records</button></a></p>' % (
            cjob.job.name, reverse('published')),
        'class': 'success'
    })

    return redirect('record_group',
                    org_id=cjob.job.record_group.organization.id,
                    record_group_id=cjob.job.record_group.id)


@login_required
def rerun_jobs(request):
    LOGGER.debug('re-running jobs')

    # get job ids
    job_ids = request.POST.getlist('job_ids[]')

    # get downstream toggle
    downstream_toggle = bool_for_string(request.POST.get('downstream_rerun_toggle', False))
    upstream_toggle = bool_for_string(request.POST.get('upstream_rerun_toggle', False))

    # set of jobs to rerun
    job_rerun_set = set()

    # loop through job_ids
    for job_id in job_ids:

        # get CombineJob
        cjob = CombineJob.get_combine_job(job_id)

        # if including downstream
        if downstream_toggle:
            # add rerun lineage for this job to set
            job_rerun_set.update(cjob.job.get_downstream_jobs(include_self=False))

        if upstream_toggle:
            job_rerun_set.update(cjob.job.get_upstream_jobs(include_self=False))

        # else, just job
        job_rerun_set.add(cjob.job)

    # sort and run
    ordered_job_rerun_set = sorted(list(job_rerun_set), key=lambda j: j.id)

    tasks.rerun_jobs(ordered_job_rerun_set)

    # set gms
    gmc = GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<strong>Preparing to Rerun Job(s):</strong><br>%s<br><br>Refresh this page to update status of Jobs rerunning. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button>' % '<br>'.join(
            [str(j.name) for j in ordered_job_rerun_set]),
        'class': 'success'
    })

    # return, as requested via Ajax which will reload page
    return JsonResponse({'results': True})


@login_required
def clone_jobs(request):
    LOGGER.debug('cloning jobs')

    job_ids = request.POST.getlist('job_ids[]')

    # get downstream toggle
    downstream_toggle = request.POST.get('downstream_clone_toggle', False)
    if downstream_toggle == 'true':
        downstream_toggle = True
    elif downstream_toggle == 'false':
        downstream_toggle = False

    # get rerun toggle
    rerun_on_clone = request.POST.get('rerun_on_clone', False)
    if rerun_on_clone == 'true':
        rerun_on_clone = True
    elif rerun_on_clone == 'false':
        rerun_on_clone = False

    # set of jobs to rerun
    job_clone_set = set()

    # loop through job_ids and add
    for job_id in job_ids:
        cjob = CombineJob.get_combine_job(job_id)
        job_clone_set.add(cjob.job)

    # sort and run
    ordered_job_clone_set = sorted(list(job_clone_set), key=lambda j: j.id)

    # initiate Combine BG Task
    combine_task = CombineBackgroundTask(
        name="Clone Jobs",
        task_type='clone_jobs',
        task_params_json=json.dumps({
            'ordered_job_clone_set': [j.id for j in ordered_job_clone_set],
            'downstream_toggle': downstream_toggle,
            'rerun_on_clone': rerun_on_clone
        })
    )
    combine_task.save()

    # run celery task
    bg_task = tasks.clone_jobs.delay(combine_task.id)
    LOGGER.debug('firing bg task: %s', bg_task)
    combine_task.celery_task_id = bg_task.task_id
    combine_task.save()

    # set gms
    gmc = GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<strong>Cloning Job(s):</strong><br>%s<br><br>Including downstream? <strong>%s</strong><br><br>Refresh this page to update status of Jobs cloning. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button>' % (
            '<br>'.join([str(j.name) for j in ordered_job_clone_set]), downstream_toggle),
        'class': 'success'
    })

    # return, as requested via Ajax which will reload page
    return JsonResponse({'results': True})


@login_required
def job_parameters(request, org_id, record_group_id, job_id):
    # get CombineJob
    cjob = CombineJob.get_combine_job(job_id)

    # if GET, return JSON
    if request.method == 'GET':
        # return
        return JsonResponse(cjob.job.job_details_dict)

    # if POST, udpate
    if request.method == 'POST':

        # get job_details as JSON
        job_details_json = request.POST.get('job_details_json', None)

        if job_details_json is not None:
            cjob.job.job_details = job_details_json
            cjob.job.save()

        return JsonResponse({"msg": "Job Parameters updated!"})


@login_required
def job_harvest_oai(request, org_id, record_group_id):
    """
        Create a new OAI Harvest Job
        """

    # retrieve record group
    record_group = RecordGroup.objects.filter(
        id=record_group_id).first()

    # if GET, prepare form
    if request.method == 'GET':
        # retrieve all OAI endoints
        oai_endpoints = OAIEndpoint.objects.all()

        # get validation scenarios
        validation_scenarios = ValidationScenario.objects.all()

        # get record identifier transformation scenarios
        rits = RecordIdentifierTransformationScenario.objects.all()

        # get field mappers
        field_mappers = FieldMapper.objects.all()

        # get all bulk downloads
        bulk_downloads = DPLABulkDataDownload.objects.all()

        # render page
        return render(request, 'core/job_harvest_oai.html', {
            'record_group': record_group,
            'oai_endpoints': oai_endpoints,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': xml2kvp.XML2kvp(),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=HarvestOAIJob,
            job_params=request.POST
        )

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if job_status == False:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_harvest_static_xml(request, org_id, record_group_id, hash_payload_filename=False):
    """
        Create a new static XML Harvest Job
        """

    # retrieve record group
    record_group = RecordGroup.objects.filter(
        id=record_group_id).first()

    # get validation scenarios
    validation_scenarios = ValidationScenario.objects.all()

    # get field mappers
    field_mappers = FieldMapper.objects.all()

    # get record identifier transformation scenarios
    rits = RecordIdentifierTransformationScenario.objects.all()

    # get all bulk downloads
    bulk_downloads = DPLABulkDataDownload.objects.all()

    # if GET, prepare form
    if request.method == 'GET':
        # render page
        return render(request, 'core/job_harvest_static_xml.html', {
            'record_group': record_group,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': xml2kvp.XML2kvp(),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=HarvestStaticXMLJob,
            job_params=request.POST,
            files=request.FILES,
            hash_payload_filename=hash_payload_filename
        )

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if job_status == False:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_harvest_tabular_data(request, org_id, record_group_id, hash_payload_filename=False):
    """
        Create a new static XML Harvest Job
        """

    # retrieve record group
    record_group = RecordGroup.objects.filter(
        id=record_group_id).first()

    # get validation scenarios
    validation_scenarios = ValidationScenario.objects.all()

    # get field mappers
    field_mappers = FieldMapper.objects.all()

    # get record identifier transformation scenarios
    rits = RecordIdentifierTransformationScenario.objects.all()

    # get all bulk downloads
    bulk_downloads = DPLABulkDataDownload.objects.all()

    # if GET, prepare form
    if request.method == 'GET':
        # render page
        return render(request, 'core/job_harvest_tabular_data.html', {
            'record_group': record_group,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': xml2kvp.XML2kvp(),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=HarvestTabularDataJob,
            job_params=request.POST,
            files=request.FILES,
            hash_payload_filename=hash_payload_filename
        )

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if job_status == False:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_transform(request, org_id, record_group_id):
    """
        Create a new Transform Job
        """

    # retrieve record group
    record_group = RecordGroup.objects.filter(
        id=record_group_id).first()

    # if GET, prepare form
    if request.method == 'GET':

        # get scope of input jobs and retrieve
        input_job_scope = request.GET.get('scope', None)

        # if all jobs, retrieve all jobs
        if input_job_scope == 'all_jobs':
            input_jobs = Job.objects.exclude(
                job_type='AnalysisJob').all()

        # else, limit to RecordGroup
        else:
            input_jobs = record_group.job_set.all()

        # get all transformation scenarios
        transformations = Transformation.objects.filter(
            use_as_include=False)

        # get validation scenarios
        validation_scenarios = ValidationScenario.objects.all()

        # get field mappers
        field_mappers = FieldMapper.objects.all()

        # get record identifier transformation scenarios
        rits = RecordIdentifierTransformationScenario.objects.all()

        # get job lineage for all jobs (filtered to input jobs scope)
        job_lineage = Job.get_all_jobs_lineage(jobs_query_set=input_jobs)

        # get all bulk downloads
        bulk_downloads = DPLABulkDataDownload.objects.all()

        # render page
        return render(request, 'core/job_transform.html', {
            'record_group': record_group,
            'input_jobs': input_jobs,
            'input_job_scope': input_job_scope,
            'transformations': transformations,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': xml2kvp.XML2kvp(),
            'job_lineage_json': json.dumps(job_lineage),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=TransformJob,
            job_params=request.POST)

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if job_status == False:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_merge(request, org_id, record_group_id):
    """
        Merge multiple jobs into a single job
        """

    # retrieve record group
    record_group = RecordGroup.objects.get(pk=record_group_id)

    # if GET, prepare form
    if request.method == 'GET':

        # get scope of input jobs and retrieve
        input_job_scope = request.GET.get('scope', None)

        # if all jobs, retrieve all jobs
        if input_job_scope == 'all_jobs':
            input_jobs = Job.objects.exclude(
                job_type='AnalysisJob').all()

        # else, limit to RecordGroup
        else:
            input_jobs = record_group.job_set.all()

        # get validation scenarios
        validation_scenarios = ValidationScenario.objects.all()

        # get record identifier transformation scenarios
        rits = RecordIdentifierTransformationScenario.objects.all()

        # get field mappers
        field_mappers = FieldMapper.objects.all()

        # get job lineage for all jobs (filtered to input jobs scope)
        job_lineage = Job.get_all_jobs_lineage(jobs_query_set=input_jobs)

        # get all bulk downloads
        bulk_downloads = DPLABulkDataDownload.objects.all()

        # render page
        return render(request, 'core/job_merge.html', {
            'job_select_type': 'multiple',
            'record_group': record_group,
            'input_jobs': input_jobs,
            'input_job_scope': input_job_scope,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': xml2kvp.XML2kvp(),
            'job_lineage_json': json.dumps(job_lineage),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=MergeJob,
            job_params=request.POST)

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if job_status == False:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


def job_lineage_json(request, org_id, record_group_id, job_id):
    """
        Return job lineage as JSON
        """

    # get job
    job = Job.objects.get(pk=int(job_id))

    # get lineage
    job_lineage = job.get_lineage()

    return JsonResponse({
        'job_id_list': [node['id'] for node in job_lineage['nodes']],
        'nodes': job_lineage['nodes'],
        'edges': job_lineage['edges']
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
    cjob = CombineJob.get_combine_job(int(job_id))

    # if GET, prepare form
    if request.method == 'GET':

        # mapped field analysis, generate if not part of job_details
        if 'mapped_field_analysis' in cjob.job.job_details_dict.keys():
            field_counts = cjob.job.job_details_dict['mapped_field_analysis']
        else:
            if cjob.job.finished:
                field_counts = cjob.count_indexed_fields()
                cjob.job.update_job_details(
                    {'mapped_field_analysis': field_counts}, save=True)
            else:
                LOGGER.debug('job not finished, not setting')
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
        task_params['validation_scenarios'] = [
            int(vs_id) for vs_id in task_params['validation_scenarios']]

        # remove select, reserved fields if in mapped field request
        task_params['mapped_field_include'] = [f for f in task_params['mapped_field_include'] if
                                               f not in ['record_id', 'db_id', 'oid', '_id']]

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name=combine_task_name,
            task_type='validation_report',
            task_params_json=json.dumps(task_params)
        )
        combine_task.save()

        # run celery task
        background_task = tasks.create_validation_report.delay(combine_task.id)
        LOGGER.debug('firing bg task: %s', background_task)
        combine_task.celery_task_id = background_task.task_id
        combine_task.save()

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
    cjob = CombineJob.get_combine_job(int(job_id))

    # if GET, prepare form
    if request.method == 'GET':
        # get validation scenarios
        validation_scenarios = ValidationScenario.objects.all()

        # get field mappers
        field_mappers = FieldMapper.objects.all()
        orig_fm_config_json = cjob.job.get_fm_config_json()

        # get all bulk downloads
        bulk_downloads = DPLABulkDataDownload.objects.all()

        # get update type from GET params
        update_type = request.GET.get('update_type', None)

        # render page
        return render(request, 'core/job_update.html', {
            'cjob': cjob,
            'update_type': update_type,
            'validation_scenarios': validation_scenarios,
            'field_mappers': field_mappers,
            'bulk_downloads': bulk_downloads,
            'xml2kvp_handle': xml2kvp.XML2kvp(),
            'orig_fm_config_json': orig_fm_config_json,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        LOGGER.debug('updating job')
        LOGGER.debug(request.POST)

        # retrieve job
        cjob = CombineJob.get_combine_job(int(job_id))

        # get update type
        update_type = request.POST.get('update_type', None)
        LOGGER.debug('running job update: %s', update_type)

        # handle re-index
        if update_type == 'reindex':
            # get preferred metadata index mapper
            fm_config_json = request.POST.get('fm_config_json')

            # init re-index
            cjob.reindex_bg_task(fm_config_json=fm_config_json)

            # set gms
            gmc = GlobalMessageClient(request.session)
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
            validation_scenarios = request.POST.getlist(
                'validation_scenario', [])

            # get validations
            validations = ValidationScenario.objects.filter(
                id__in=[int(vs_id) for vs_id in validation_scenarios])

            # init bg task
            cjob.new_validations_bg_task([vs.id for vs in validations])

            # set gms
            gmc = GlobalMessageClient(request.session)
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
            validation_scenario = JobValidation.objects.get(
                pk=int(jv_id)).validation_scenario
            gmc = GlobalMessageClient(request.session)
            gmc.add_gm({
                'html': '<p><strong>Removing Validation for Job:</strong><br>%s<br><br>'
                        '<strong>Validation Scenario:</strong><br>%s</p><p><a href="%s"><button type="button" '
                        'class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' % (
                            cjob.job.name, validation_scenario.name, reverse('bg_tasks')),
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
            dbdd = DPLABulkDataDownload.objects.get(pk=int(dbdd_id))
            gmc = GlobalMessageClient(request.session)
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

        if update_type == 'publish_set':
            update_body = request.POST
            if update_body.get('publish_set_id', None):
                cjob.job.publish_set_id = update_body['publish_set_id']
            if update_body.get('existing_publish_set_id', None):
                cjob.job.publish_set_id = update_body['existing_publish_set_id']
            redirect_anchor = update_body.get('redirect_anchor', '')
            cjob.job.save()
            return redirect(reverse('job_details', args=[org_id, record_group_id, job_id]) + redirect_anchor)


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
    esi = ESIndex(ast.literal_eval(es_index))

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
    cjob = CombineJob.get_combine_job(job_id)

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
    esi = ESIndex(ast.literal_eval(es_index))

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

        # default None if checking non-matches to value
        value = request.GET.get('value', None)
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
    cjob = CombineJob.get_combine_job(job_id)

    # get job validation instance
    job_validation = JobValidation.objects.get(pk=int(job_validation_id))

    # return
    return render(request, 'core/job_validation_scenario_failures.html', {
        'cjob': cjob,
        'jv': job_validation,
        'breadcrumbs': breadcrumb_parser(request)
    })
