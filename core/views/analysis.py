import json

from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect

from core import models

from .view_helpers import breadcrumb_parser


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
        job_lineage = models.Job.get_all_jobs_lineage(jobs_query_set=input_jobs)

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
            'job_lineage_json': json.dumps(job_lineage)
        })

    # if POST, submit job
    if request.method == 'POST':

        cjob = models.CombineJob.init_combine_job(
            user=request.user,
            # TODO: record_group=record_group,
            job_type_class=models.AnalysisJob,
            job_params=request.POST)

        # start job and update status
        job_status = cjob.start_job()

        # if job_status is absent, report job status as failed
        if job_status is False:
            cjob.job.status = 'failed'
            cjob.job.save()

        return redirect('analysis')
