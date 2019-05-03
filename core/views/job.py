import datetime
import json
import logging

from django.contrib.auth.decorators import login_required
from django.core.urlresolvers import reverse
from django.http import JsonResponse
from django.shortcuts import redirect, render

from core import models, tasks

from .views import breadcrumb_parser, mc_handle

logger = logging.getLogger(__name__)


@login_required
def job_id_redirect(request, job_id):
    """
	Route to redirect to more verbose Jobs URL
	"""

    # get job
    job = models.Job.objects.get(pk=job_id)

    # redirect
    return redirect('job_details',
                    org_id=job.record_group.organization.id,
                    record_group_id=job.record_group.id,
                    job_id=job.id)


@login_required
def all_jobs(request):
    # get all the record groups.
    record_groups = models.RecordGroup.objects.exclude(for_analysis=True)

    '''
	View to show all jobs, across all Organizations, RecordGroups, and Job types

	GET Args:
		include_analysis: if true, include Analysis type jobs
	'''

    # capture include_analysis GET param if present
    include_analysis = request.GET.get('include_analysis', False)

    # get all jobs associated with record group
    if include_analysis:
        jobs = models.Job.objects.all()
    else:
        jobs = models.Job.objects.exclude(job_type='AnalysisJob').all()

    # get job lineage for all jobs
    if include_analysis:
        ld = models.Job.get_all_jobs_lineage(exclude_analysis_jobs=False)
    else:
        ld = models.Job.get_all_jobs_lineage(exclude_analysis_jobs=True)

    # loop through jobs and update status
    for job in jobs:
        job.update_status()

    # render page
    return render(request, 'core/all_jobs.html', {
        'jobs': jobs,
        'record_groups': record_groups,
        'job_lineage_json': json.dumps(ld),
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_delete(request, org_id, record_group_id, job_id):
    logger.debug('deleting job by id: %s' % job_id)

    # get job
    job = models.Job.objects.get(pk=job_id)

    # set job status to deleting
    job.name = "%s (DELETING)" % job.name
    job.deleted = True
    job.status = 'deleting'
    job.save()

    # initiate Combine BG Task
    ct = models.CombineBackgroundTask(
        name='Delete Job: %s' % job.name,
        task_type='delete_model_instance',
        task_params_json=json.dumps({
            'model': 'Job',
            'job_id': job.id
        })
    )
    ct.save()

    # run celery task
    bg_task = tasks.delete_model_instance.delay('Job', job.id)
    logger.debug('firing bg task: %s' % bg_task)
    ct.celery_task_id = bg_task.task_id
    ct.save()

    # redirect
    return redirect(request.META.get('HTTP_REFERER'))


@login_required
def stop_jobs(request):
    logger.debug('stopping jobs')

    job_ids = request.POST.getlist('job_ids[]')
    logger.debug(job_ids)

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
        cjob = models.CombineJob.get_combine_job(job_id)

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
        logger.debug('stopping Job: %s' % job)

        # stop job
        job.stop_job()

    # set gms
    gmc = models.GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<p><strong>Stopped Job(s):</strong><br>%s</p>' % (
            '<br>'.join([j.name for j in ordered_job_delete_set])),
        'class': 'danger'
    })

    # return
    return JsonResponse({'results': True})


@login_required
def delete_jobs(request):
    logger.debug('deleting jobs')

    job_ids = request.POST.getlist('job_ids[]')
    logger.debug(job_ids)

    # get downstream toggle
    downstream_toggle = request.POST.get('downstream_delete_toggle', False)
    if downstream_toggle == 'true':
        downstream_toggle = True
    elif downstream_toggle == 'false':
        downstream_toggle = False

    # set of jobs to rerun
    job_delete_set = set()

    # loop through job_ids
    for job_id in job_ids:

        # get CombineJob
        cjob = models.CombineJob.get_combine_job(job_id)

        # if including downstream
        if downstream_toggle:

            # add rerun lineage for this job to set
            job_delete_set.update(cjob.job.get_downstream_jobs())

        # else, just job
        else:

            job_delete_set.add(cjob.job)

    # sort and run
    ordered_job_delete_set = sorted(list(job_delete_set), key=lambda j: j.id)

    # # loop through and update visible elements of Job for front-end
    for job in ordered_job_delete_set:
        logger.debug('deleting Job: %s' % job)

        # set job status to deleting
        job.name = "%s (DELETING)" % job.name
        job.deleted = True
        job.status = 'deleting'
        job.save()

        # initiate Combine BG Task
        ct = models.CombineBackgroundTask(
            name='Delete Job: #%s' % job.name,
            task_type='delete_model_instance',
            task_params_json=json.dumps({
                'model': 'Job',
                'job_id': job.id
            })
        )
        ct.save()

        # run celery task
        bg_task = tasks.delete_model_instance.delay('Job', job.id, )
        logger.debug('firing bg task: %s' % bg_task)
        ct.celery_task_id = bg_task.task_id
        ct.save()

    # set gms
    gmc = models.GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<p><strong>Deleting Job(s):</strong><br>%s</p><p>Refresh this page to update status of removing Jobs. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button></p>' % (
            '<br>'.join([j.name for j in ordered_job_delete_set])),
        'class': 'danger'
    })

    # return
    return JsonResponse({'results': True})


@login_required
def move_jobs(request):
    logger.debug('moving jobs')

    job_ids = request.POST.getlist('job_ids[]')
    record_group_id = request.POST.getlist('record_group_id')[0]

    # get downstream toggle
    downstream_toggle = request.POST.get('downstream_move_toggle', False)
    if downstream_toggle == 'true':
        downstream_toggle = True
    elif downstream_toggle == 'false':
        downstream_toggle = False

    # set of jobs to rerun
    job_move_set = set()

    # loop through job_ids
    for job_id in job_ids:

        # get CombineJob
        cjob = models.CombineJob.get_combine_job(job_id)

        # if including downstream
        if downstream_toggle:

            # add rerun lineage for this job to set
            job_move_set.update(cjob.job.get_downstream_jobs())

        # else, just job
        else:

            job_move_set.add(cjob.job)

    # sort and run
    ordered_job_move_set = sorted(list(job_move_set), key=lambda j: j.id)

    # loop through jobs
    for job in ordered_job_move_set:
        logger.debug('moving Job: %s' % job)

        new_record_group = models.RecordGroup.objects.get(pk=record_group_id)
        job.record_group = new_record_group
        job.save()

        logger.debug('Job %s has been moved' % job)

    # redirect
    return JsonResponse({'results': True})


@login_required
def job_details(request, org_id, record_group_id, job_id):
    logger.debug('details for job id: %s' % job_id)

    # get CombineJob
    cjob = models.CombineJob.get_combine_job(job_id)

    # update status
    cjob.job.update_status()

    # detailed record count
    record_count_details = cjob.job.get_detailed_job_record_count()

    # get job lineage
    job_lineage = cjob.job.get_lineage()

    # get dpla_bulk_data_match
    dpla_bulk_data_matches = cjob.job.get_dpla_bulk_data_matches()

    # check if limiting to one, pre-existing record
    q = request.GET.get('q', None)

    # job details and job type specific augment
    job_details = cjob.job.job_details_dict

    # mapped field analysis, generate if not part of job_details
    if 'mapped_field_analysis' in job_details.keys():
        field_counts = job_details['mapped_field_analysis']
    else:
        if cjob.job.finished:
            field_counts = cjob.count_indexed_fields()
            cjob.job.update_job_details({'mapped_field_analysis': field_counts}, save=True)
        else:
            logger.debug('job not finished, not setting')
            field_counts = {}

    # OAI Harvest
    if type(cjob) == models.HarvestOAIJob:
        pass

    # Static Harvest
    elif type(cjob) == models.HarvestStaticXMLJob:
        pass

    # Transform
    elif type(cjob) == models.TransformJob:
        pass

    # Merge/Duplicate
    elif type(cjob) == models.MergeJob:
        pass

    # Analysis
    elif type(cjob) == models.AnalysisJob:
        pass

    # get published records, primarily for published sets
    pr = models.PublishedRecords()

    # get published subsets with PublishedRecords static method
    published_subsets = models.PublishedRecords.get_subsets()

    # loop through subsets and enrich
    for _ in published_subsets:

        # add counts
        counts = mc_handle.combine.misc.find_one({'_id': 'published_field_counts_%s' % _['name']})

        # if counts not yet calculated, do now
        if counts is None:
            counts = models.PublishedRecords(subset=_['name']).count_indexed_fields()
        _['counts'] = counts

    # get field mappers
    field_mappers = models.FieldMapper.objects.all()

    # return
    return render(request, 'core/job_details.html', {
        'cjob': cjob,
        'record_group': cjob.job.record_group,
        'record_count_details': record_count_details,
        'field_counts': field_counts,
        'field_mappers': field_mappers,
        'xml2kvp_handle': models.XML2kvp(),
        'job_lineage_json': json.dumps(job_lineage),
        'dpla_bulk_data_matches': dpla_bulk_data_matches,
        'q': q,
        'job_details': job_details,
        'pr': pr,
        'published_subsets': published_subsets,
        'es_index_str': cjob.esi.es_index_str,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_errors(request, org_id, record_group_id, job_id):
    logger.debug('retrieving errors for job id: %s' % job_id)

    # get CombineJob
    cjob = models.CombineJob.get_combine_job(job_id)

    job_errors = cjob.get_job_errors()

    # return
    return render(request, 'core/job_errors.html', {
        'cjob': cjob,
        'job_errors': job_errors,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def job_update_note(request, org_id, record_group_id, job_id):
    if request.method == 'POST':

        # get CombineJob
        cjob = models.CombineJob.get_combine_job(job_id)

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
        cjob = models.CombineJob.get_combine_job(job_id)

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
    logger.debug(request.POST)

    # get preferred metadata index mapper
    publish_set_id = request.POST.get('publish_set_id', None)

    # get published subsets to include in
    published_subsets = request.POST.getlist('published_subsets', [])

    # get CombineJob
    cjob = models.CombineJob.get_combine_job(job_id)

    # init publish
    bg_task = cjob.publish_bg_task(
        publish_set_id=publish_set_id,
        in_published_subsets=published_subsets)

    # set gms
    gmc = models.GlobalMessageClient(request.session)
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
    cjob = models.CombineJob.get_combine_job(job_id)

    # init unpublish
    bg_task = cjob.unpublish_bg_task()

    # set gms
    gmc = models.GlobalMessageClient(request.session)
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
    logger.debug('re-running jobs')

    # get job ids
    job_ids = request.POST.getlist('job_ids[]')

    # get downstream toggle
    downstream_toggle = request.POST.get('downstream_rerun_toggle', False)
    if downstream_toggle == 'true':
        downstream_toggle = True
    elif downstream_toggle == 'false':
        downstream_toggle = False

    # set of jobs to rerun
    job_rerun_set = set()

    # loop through job_ids
    for job_id in job_ids:

        # get CombineJob
        cjob = models.CombineJob.get_combine_job(job_id)

        # if including downstream
        if downstream_toggle:

            # add rerun lineage for this job to set
            job_rerun_set.update(cjob.job.get_downstream_jobs())

        # else, just job
        else:

            job_rerun_set.add(cjob.job)

    # sort and run
    ordered_job_rerun_set = sorted(list(job_rerun_set), key=lambda j: j.id)

    # # loop through and update visible elements of Job for front-end
    for re_job in ordered_job_rerun_set:
        re_job.timestamp = datetime.datetime.now()
        re_job.status = 'initializing'
        re_job.record_count = 0
        re_job.finished = False
        re_job.elapsed = 0
        re_job.deleted = True
        re_job.save()

    # initiate Combine BG Task
    ct = models.CombineBackgroundTask(
        name="Rerun Jobs Prep",
        task_type='rerun_jobs_prep',
        task_params_json=json.dumps({
            'ordered_job_rerun_set': [j.id for j in ordered_job_rerun_set]
        })
    )
    ct.save()

    # run celery task
    bg_task = tasks.rerun_jobs_prep.delay(ct.id)
    logger.debug('firing bg task: %s' % bg_task)
    ct.celery_task_id = bg_task.task_id
    ct.save()

    # set gms
    gmc = models.GlobalMessageClient(request.session)
    gmc.add_gm({
        'html': '<strong>Preparing to Rerun Job(s):</strong><br>%s<br><br>Refresh this page to update status of Jobs rerunning. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button>' % '<br>'.join(
            [str(j.name) for j in ordered_job_rerun_set]),
        'class': 'success'
    })

    # return, as requested via Ajax which will reload page
    return JsonResponse({'results': True})


@login_required
def clone_jobs(request):
    logger.debug('cloning jobs')

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
        cjob = models.CombineJob.get_combine_job(job_id)
        job_clone_set.add(cjob.job)

    # sort and run
    ordered_job_clone_set = sorted(list(job_clone_set), key=lambda j: j.id)

    # initiate Combine BG Task
    ct = models.CombineBackgroundTask(
        name="Clone Jobs",
        task_type='clone_jobs',
        task_params_json=json.dumps({
            'ordered_job_clone_set': [j.id for j in ordered_job_clone_set],
            'downstream_toggle': downstream_toggle,
            'rerun_on_clone': rerun_on_clone
        })
    )
    ct.save()

    # run celery task
    bg_task = tasks.clone_jobs.delay(ct.id)
    logger.debug('firing bg task: %s' % bg_task)
    ct.celery_task_id = bg_task.task_id
    ct.save()

    # set gms
    gmc = models.GlobalMessageClient(request.session)
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
    cjob = models.CombineJob.get_combine_job(job_id)

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
    record_group = models.RecordGroup.objects.filter(id=record_group_id).first()

    # if GET, prepare form
    if request.method == 'GET':
        # retrieve all OAI endoints
        oai_endpoints = models.OAIEndpoint.objects.all()

        # get validation scenarios
        validation_scenarios = models.ValidationScenario.objects.all()

        # get record identifier transformation scenarios
        rits = models.RecordIdentifierTransformationScenario.objects.all()

        # get field mappers
        field_mappers = models.FieldMapper.objects.all()

        # get all bulk downloads
        bulk_downloads = models.DPLABulkDataDownload.objects.all()

        # render page
        return render(request, 'core/job_harvest_oai.html', {
            'record_group': record_group,
            'oai_endpoints': oai_endpoints,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': models.XML2kvp(),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = models.CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=models.HarvestOAIJob,
            job_params=request.POST
        )

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if not job_status:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_harvest_static_xml(request, org_id, record_group_id, hash_payload_filename=False):
    """
	Create a new static XML Harvest Job
	"""

    # retrieve record group
    record_group = models.RecordGroup.objects.filter(id=record_group_id).first()

    # get validation scenarios
    validation_scenarios = models.ValidationScenario.objects.all()

    # get field mappers
    field_mappers = models.FieldMapper.objects.all()

    # get record identifier transformation scenarios
    rits = models.RecordIdentifierTransformationScenario.objects.all()

    # get all bulk downloads
    bulk_downloads = models.DPLABulkDataDownload.objects.all()

    # if GET, prepare form
    if request.method == 'GET':
        # render page
        return render(request, 'core/job_harvest_static_xml.html', {
            'record_group': record_group,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': models.XML2kvp(),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = models.CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=models.HarvestStaticXMLJob,
            job_params=request.POST,
            files=request.FILES,
            hash_payload_filename=hash_payload_filename
        )

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if not job_status:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_harvest_tabular_data(request, org_id, record_group_id, hash_payload_filename=False):
    """
	Create a new static XML Harvest Job
	"""

    # retrieve record group
    record_group = models.RecordGroup.objects.filter(id=record_group_id).first()

    # get validation scenarios
    validation_scenarios = models.ValidationScenario.objects.all()

    # get field mappers
    field_mappers = models.FieldMapper.objects.all()

    # get record identifier transformation scenarios
    rits = models.RecordIdentifierTransformationScenario.objects.all()

    # get all bulk downloads
    bulk_downloads = models.DPLABulkDataDownload.objects.all()

    # if GET, prepare form
    if request.method == 'GET':
        # render page
        return render(request, 'core/job_harvest_tabular_data.html', {
            'record_group': record_group,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': models.XML2kvp(),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = models.CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=models.HarvestTabularDataJob,
            job_params=request.POST,
            files=request.FILES,
            hash_payload_filename=hash_payload_filename
        )

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if not job_status:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_transform(request, org_id, record_group_id):
    """
	Create a new Transform Job
	"""

    # retrieve record group
    record_group = models.RecordGroup.objects.filter(id=record_group_id).first()

    # if GET, prepare form
    if request.method == 'GET':

        # get scope of input jobs and retrieve
        input_job_scope = request.GET.get('scope', None)

        # if all jobs, retrieve all jobs
        if input_job_scope == 'all_jobs':
            input_jobs = models.Job.objects.exclude(job_type='AnalysisJob').all()

        # else, limit to RecordGroup
        else:
            input_jobs = record_group.job_set.all()

        # get all transformation scenarios
        transformations = models.Transformation.objects.filter(use_as_include=False)

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
        return render(request, 'core/job_transform.html', {
            'record_group': record_group,
            'input_jobs': input_jobs,
            'input_job_scope': input_job_scope,
            'transformations': transformations,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': models.XML2kvp(),
            'job_lineage_json': json.dumps(ld),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = models.CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=models.TransformJob,
            job_params=request.POST)

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if not job_status:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_merge(request, org_id, record_group_id):
    """
	Merge multiple jobs into a single job
	"""

    # retrieve record group
    record_group = models.RecordGroup.objects.get(pk=record_group_id)

    # if GET, prepare form
    if request.method == 'GET':

        # get scope of input jobs and retrieve
        input_job_scope = request.GET.get('scope', None)

        # if all jobs, retrieve all jobs
        if input_job_scope == 'all_jobs':
            input_jobs = models.Job.objects.exclude(job_type='AnalysisJob').all()

        # else, limit to RecordGroup
        else:
            input_jobs = record_group.job_set.all()

        # get validation scenarios
        validation_scenarios = models.ValidationScenario.objects.all()

        # get record identifier transformation scenarios
        rits = models.RecordIdentifierTransformationScenario.objects.all()

        # get field mappers
        field_mappers = models.FieldMapper.objects.all()

        # get job lineage for all jobs (filtered to input jobs scope)
        ld = models.Job.get_all_jobs_lineage(jobs_query_set=input_jobs)

        # get all bulk downloads
        bulk_downloads = models.DPLABulkDataDownload.objects.all()

        # render page
        return render(request, 'core/job_merge.html', {
            'job_select_type': 'multiple',
            'record_group': record_group,
            'input_jobs': input_jobs,
            'input_job_scope': input_job_scope,
            'validation_scenarios': validation_scenarios,
            'rits': rits,
            'field_mappers': field_mappers,
            'xml2kvp_handle': models.XML2kvp(),
            'job_lineage_json': json.dumps(ld),
            'bulk_downloads': bulk_downloads,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = models.CombineJob.init_combine_job(
            user=request.user,
            record_group=record_group,
            job_type_class=models.MergeJob,
            job_params=request.POST)

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if not job_status:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


def job_lineage_json(request, org_id, record_group_id, job_id):
    """
	Return job lineage as JSON
	"""

    # get job
    job = models.Job.objects.get(pk=int(job_id))

    # get lineage
    job_lineage = job.get_lineage()

    return JsonResponse({
        'job_id_list': [node['id'] for node in job_lineage['nodes']],
        'nodes': job_lineage['nodes'],
        'edges': job_lineage['edges']
    })
