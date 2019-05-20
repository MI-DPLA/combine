import logging
import uuid

from django.http import HttpResponse, JsonResponse
from django.forms.models import model_to_dict
from django.shortcuts import render

from core import models

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


def validation_scenario_payload(request, vs_id):
    """
        View payload for validation scenario
        """

    # get transformation
    validation_scenario = models.ValidationScenario.objects.get(pk=int(vs_id))

    if validation_scenario.validation_type == 'sch':
        # return document as XML
        return HttpResponse(validation_scenario.payload, content_type='text/xml')

    return HttpResponse(validation_scenario.payload, content_type='text/plain')


def create_validation_scenario(request):
    # TODO: validate the model
    # TODO: do we care about deduplicating validation scenarios?
    # TODO: error handling
    new_validation_scenario = models.ValidationScenario(
        name=request.POST['vs_name'],
        payload=request.POST['vs_payload'],
        validation_type=request.POST['vs_type']
    )
    new_validation_scenario.save()
    return JsonResponse(model_to_dict(new_validation_scenario))


def test_validation_scenario(request):
    """
        View to live test validation scenario
        """

    # If GET, serve validation test screen
    if request.method == 'GET':
        # get validation scenarios
        validation_scenarios = models.ValidationScenario.objects.all()

        # check if limiting to one, pre-existing record
        q = request.GET.get('q', None)

        # check for pre-requested transformation scenario
        vsid = request.GET.get('validation_scenario', None)

        # return
        return render(request, 'core/test_validation_scenario.html', {
            'q': q,
            'vsid': vsid,
            'validation_scenarios': validation_scenarios,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # If POST, provide raw result of validation test
    if request.method == 'POST':

        LOGGER.debug('running test validation and returning')

        # get record
        record = models.Record.objects.get(id=request.POST.get('db_id'))

        try:
            # init new validation scenario
            validation_scenario = models.ValidationScenario(
                name='temp_vs_%s' % str(uuid.uuid4()),
                payload=request.POST.get('vs_payload'),
                validation_type=request.POST.get('vs_type'),
                default_run=False
            )
            validation_scenario.save()

            # validate with record
            vs_results = validation_scenario.validate_record(record)

            # delete vs
            validation_scenario.delete()

            if request.POST.get('vs_results_format') == 'raw':
                return HttpResponse(vs_results['raw'], content_type="text/plain")
            if request.POST.get('vs_results_format') == 'parsed':
                return JsonResponse(vs_results['parsed'])
            raise Exception('validation results format not recognized')

        except Exception as err:

            if validation_scenario.id:
                # TODO: Not sure how to invoke this code
                LOGGER.debug(
                    'test validation scenario was unsuccessful, deleting temporary vs')
                validation_scenario.delete()

            return HttpResponse(str(err), content_type="text/plain")
