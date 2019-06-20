# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import logging
import re

# django
from django.core.urlresolvers import reverse

# import models
from core.models import Job, Organization, RecordGroup, Record, CombineBackgroundTask, StateIO

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


def bool_for_string(string):
    if string == 'true':
        return True
    elif string == 'false':
        return False

# breadcrumb parser
def breadcrumb_parser(request):
    """
    Rudimentary breadcrumbs parser
    """

    crumbs = []

    # livy/spark
    regex_match = re.match(r'(.+?/livy_sessions)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Livy/Spark</span>", reverse('livy_sessions')))

    # configurations
    regex_match = re.match(r'(.+?/configuration)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Configuration</span>", reverse('configuration')))

    # search
    regex_match = re.match(r'(.+?/search)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Search</span>", reverse('search')))

    # configurations/test_validation_scenario
    regex_match = re.match(
        r'(.+?/configuration/validation/test)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Test Validation Scenario</span>", reverse('test_validation_scenario')))

    # all jobs
    regex_match = re.match(r'(.+?/jobs/all)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>All Jobs</span>", reverse('all_jobs')))

    # analysis
    regex_match = re.match(r'(.+?/analysis)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Analysis</span>", reverse('analysis')))

    # field analysis
    regex_match = re.match(
        r'(.+?/analysis/es/index/j([0-9]+)/field_analysis.*)', request.path)
    if regex_match:

        # get job
        job = Job.objects.get(pk=int(regex_match.group(2)))

        # get field for analysis
        field_name = request.GET.get('field_name', None)

        # append crumbs
        if job.record_group.organization.for_analysis:
            LOGGER.debug("breadcrumbs: org is for analysis, skipping")
        else:
            crumbs.append((
                "<span class='font-weight-bold'>Organization</span> - <code>%s</code>" % job.record_group.organization.name,
                reverse('organization', kwargs={'org_id': job.record_group.organization.id})))
        if job.record_group.for_analysis:
            LOGGER.debug("breadcrumbs: rg is for analysis, skipping")
        else:
            crumbs.append(("<span class='font-weight-bold'>RecordGroup</span> - <code>%s</code>" % job.record_group.name,
                           reverse('record_group', kwargs={'org_id': job.record_group.organization.id,
                                                           'record_group_id': job.record_group.id})))
        crumbs.append(("<span class='font-weight-bold'>Job</span> - <code>%s</code>" % job.name,
                       reverse('job_details', kwargs=dict(org_id=job.record_group.organization.id,
                                                          record_group_id=job.record_group.id,
                                                          job_id=job.id))))
        crumbs.append(("<span class='font-weight-bold'>Field Analysis - <code>%s</code></span>" % field_name,
                       '%s?%s' % (regex_match.group(1), request.META['QUERY_STRING'])))

    # published
    pub_m = re.match(r'(.+?/published.*)', request.path)
    if pub_m:
        crumbs.append(
            ("<span class='font-weight-bold'>Published</span>", reverse('published')))

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
        crumbs.append(
            ("<span class='font-weight-bold'>Organizations</span>", reverse('organizations')))

    # org
    org_m = re.match(r'(.+?/organization/([0-9]+))', request.path)
    if org_m:
        org = Organization.objects.get(pk=int(org_m.group(2)))
        if org.for_analysis:
            LOGGER.debug(
                "breadcrumbs: org is for analysis, converting breadcrumbs")
            crumbs.append(
                ("<span class='font-weight-bold'>Analysis</span>", reverse('analysis')))
        else:
            crumbs.append(
                ("<span class='font-weight-bold'>Organization</span> - <code>%s</code>" % org.name, org_m.group(1)))

    # record_group
    rg_m = re.match(r'(.+?/record_group/([0-9]+))', request.path)
    if rg_m:
        record_group = RecordGroup.objects.get(pk=int(rg_m.group(2)))
        if record_group.for_analysis:
            LOGGER.debug(
                "breadcrumbs: rg is for analysis, converting breadcrumbs")
        else:
            crumbs.append(
                ("<span class='font-weight-bold'>RecordGroup</span> - <code>%s</code>" % record_group.name, rg_m.group(1)))

    # job
    j_m = re.match(r'(.+?/job/([0-9]+))', request.path)
    if j_m:
        job = Job.objects.get(pk=int(j_m.group(2)))
        if job.record_group.for_analysis:
            crumbs.append(
                ("<span class='font-weight-bold'>Analysis</span> - %s" % job.name, j_m.group(1)))
        else:
            crumbs.append(
                ("<span class='font-weight-bold'>Job</span> - <code>%s</code>" % job.name, j_m.group(1)))

    # record
    r_m = re.match(r'(.+?/record/([0-9a-z]+))', request.path)
    if r_m:
        record = Record.objects.get(id=r_m.group(2))
        crumbs.append(
            ("<span class='font-weight-bold'>Record</span> - <code>%s</code>" % record.record_id, r_m.group(1)))

    # background tasks
    regex_match = re.match(r'(.+?/background_tasks)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Background Tasks</span>", reverse('bg_tasks')))

    # background task
    regex_match = re.match(
        r'(.+?/background_tasks/task/([0-9]+))', request.path)
    if regex_match:
        background_task = CombineBackgroundTask.objects.get(
            pk=int(regex_match.group(2)))
        crumbs.append(
            ("<span class='font-weight-bold'>Task - <code>%s</code></span>" % background_task.name,
             reverse('bg_tasks')))

    # stateio
    regex_match = re.match(r'(.+?/stateio.*)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>State Export/Import</span>", reverse('stateio')))

    # stateio - state details
    regex_match = re.match(r'(.+?/stateio/state/([0-9a-z].*))', request.path)
    if regex_match:
        state = StateIO.objects.get(id=regex_match.group(2))
        crumbs.append(("<span class='font-weight-bold'>State - <code>%s</code></span>" % state.name,
                       reverse('stateio_state', kwargs={'state_id': regex_match.group(2)})))

    # stateio - export
    regex_match = re.match(r'(.+?/stateio/export.*)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Export</span>", reverse('stateio_export')))

    # stateio - export
    regex_match = re.match(r'(.+?/stateio/import.*)', request.path)
    if regex_match:
        crumbs.append(
            ("<span class='font-weight-bold'>Import</span>", reverse('stateio_import')))

    # return
    return crumbs
