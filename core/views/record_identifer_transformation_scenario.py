import logging

from django.http import JsonResponse
from django.forms.models import model_to_dict
from django.shortcuts import render

from core import models

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


def rits_payload(request, rits_id):
    """
        View payload for record identifier transformation scenario
        """

    # get transformation
    rits = models.RecordIdentifierTransformationScenario.objects.get(
        pk=int(rits_id))

    # return as json package
    return JsonResponse(model_to_dict(rits))


def test_rits(request):
    """
        View to live test record identifier transformation scenarios
        """

    # If GET, serve validation test screen
    if request.method == 'GET':
        # check if limiting to one, pre-existing record
        q = request.GET.get('q', None)

        # get record identifier transformation scenarios
        rits = models.RecordIdentifierTransformationScenario.objects.all()

        # return
        return render(request, 'core/test_rits.html', {
            'q': q,
            'rits': rits,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # If POST, provide raw result of validation test
    if request.method == 'POST':

        LOGGER.debug('testing record identifier transformation')
        LOGGER.debug(request.POST)

        try:

            # make POST data mutable
            request.POST._mutable = True

            # get record
            if request.POST.get('db_id', False):
                record = models.Record.objects.get(
                    id=request.POST.get('db_id'))
            else:
                return JsonResponse({'results': 'Please select a record from the table above!', 'success': False})

            # determine testing type
            if request.POST['record_id_transform_target'] == 'record_id':
                LOGGER.debug('configuring test for record_id')
                request.POST['test_transform_input'] = record.record_id
            elif request.POST['record_id_transform_target'] == 'document':
                LOGGER.debug('configuring test for record_id')
                request.POST['test_transform_input'] = record.document

            # instantiate rits and return test
            rits = models.RITSClient(request.POST)
            return JsonResponse(rits.test_user_input())

        except Exception as err:
            return JsonResponse({'results': str(err), 'success': False})
