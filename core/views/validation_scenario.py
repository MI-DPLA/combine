import logging
import uuid

from django.http import HttpResponse, JsonResponse
from django.forms.models import model_to_dict
from django.shortcuts import render

from core import models

from .views import breadcrumb_parser

logger = logging.getLogger(__name__)


def validation_scenario_payload(request, vs_id):
    """
	View payload for validation scenario
	"""

    # get transformation
    vs = models.ValidationScenario.objects.get(pk=int(vs_id))

    if vs.validation_type == 'sch':
        # return document as XML
        return HttpResponse(vs.payload, content_type='text/xml')

    else:
        return HttpResponse(vs.payload, content_type='text/plain')


def save_validation_scenario(request):
    # TODO: validate the model
    # TODO: do we care about deduplicating validation scenarios?
    # TODO: error handling
    new_validation_scenario = models.ValidationScenario(
        name=request.POST['vs_name'],
        payload=request.POST['vs_payload'],
        validation_type=request.POST['vs_type']
    )
    result = new_validation_scenario.save()
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

        logger.debug('running test validation and returning')

        # get record
        record = models.Record.objects.get(id=request.POST.get('db_id'))

        try:
            # init new validation scenario
            vs = models.ValidationScenario(
                name='temp_vs_%s' % str(uuid.uuid4()),
                payload=request.POST.get('vs_payload'),
                validation_type=request.POST.get('vs_type'),
                default_run=False
            )
            vs.save()

            # validate with record
            vs_results = vs.validate_record(record)

            # delete vs
            vs.delete()

            if request.POST.get('vs_results_format') == 'raw':
                return HttpResponse(vs_results['raw'], content_type="text/plain")
            elif request.POST.get('vs_results_format') == 'parsed':
                return JsonResponse(vs_results['parsed'])
            else:
                raise Exception('validation results format not recognized')

        except Exception as e:

            logger.debug('test validation scenario was unsucessful, deleting temporary vs')
            vs.delete()

            return HttpResponse(str(e), content_type="text/plain")
