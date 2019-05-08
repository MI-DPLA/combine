import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render, redirect

from core import models

from core.mongo import settings

from .view_helpers import breadcrumb_parser

logger = logging.getLogger(__name__)


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

