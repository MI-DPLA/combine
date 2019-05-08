import logging
import json

from django.shortcuts import render, redirect

from core import forms, models, tasks

from .view_helpers import breadcrumb_parser

logger = logging.getLogger(__name__)


def organizations(request):
    """
	View all Organizations
	"""

    # show organizations
    if request.method == 'GET':
        logger.debug('retrieving organizations')

        # get all organizations
        orgs = models.Organization.objects.exclude(for_analysis=True).all()

        # render page
        return render(request, 'core/organizations.html', {
            'orgs': orgs,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # create new organization
    if request.method == 'POST':
        # create new org
        logger.debug(request.POST)
        f = forms.OrganizationForm(request.POST)
        new_org = f.save()

        return redirect('organization', org_id=new_org.id)


def organization(request, org_id):
    """
	Details for Organization
	"""

    # get organization
    org = models.Organization.objects.get(pk=org_id)

    # get record groups for this organization
    record_groups = models.RecordGroup.objects.filter(organization=org).exclude(for_analysis=True)

    # render page
    return render(request, 'core/organization.html', {
        'org': org,
        'record_groups': record_groups,
        'breadcrumbs': breadcrumb_parser(request)
    })


def organization_delete(request, org_id):
    """
	Delete Organization
	Note: Through cascade deletes, would remove:
		- RecordGroup
			- Job
				- Record
	"""

    # get organization
    org = models.Organization.objects.get(pk=org_id)

    # set job status to deleting
    org.name = "%s (DELETING)" % org.name
    org.save()

    # initiate Combine BG Task
    ct = models.CombineBackgroundTask(
        name='Delete Organization: %s' % org.name,
        task_type='delete_model_instance',
        task_params_json=json.dumps({
            'model': 'Organization',
            'org_id': org.id
        })
    )
    ct.save()

    # run celery task
    bg_task = tasks.delete_model_instance.delay('Organization', org.id, )
    logger.debug('firing bg task: %s' % bg_task)
    ct.celery_task_id = bg_task.task_id
    ct.save()

    return redirect('organizations')
