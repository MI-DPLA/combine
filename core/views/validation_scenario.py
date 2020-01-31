import logging
import uuid

from django.core.exceptions import ObjectDoesNotExist
from django.http import HttpResponse, JsonResponse
from django.forms.models import model_to_dict
from django.shortcuts import render, redirect
from django.urls import reverse

from core.models import ValidationScenario, Record, get_validation_scenario_choices
from core.forms import ValidationScenarioForm

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


def validation_scenario_payload(request, vs_id):
    """
        View payload for validation scenario
        """

    # get transformation
    scenario = ValidationScenario.objects.get(pk=int(vs_id))

    if scenario.validation_type == 'sch':
        # return document as XML
        return HttpResponse(scenario.payload, content_type='text/xml')

    return HttpResponse(scenario.payload, content_type='text/plain')


def create_validation_scenario(request):
    # TODO: do we care about deduplicating validation scenarios?
    form = None
    if request.method == "POST":
        form = ValidationScenarioForm(request.POST)
        if form.is_valid():
            new_scenario = ValidationScenario(**form.cleaned_data)
            new_scenario.save()
            return redirect(reverse('configuration'))
    if form is None:
        form = ValidationScenarioForm()
    return render(request, 'core/new_configuration_object.html', {
        'form': form,
        'object_name': 'Validation Scenario',
    })


def validation_scenario(request, vs_id):
    scenario = ValidationScenario.objects.get(pk=int(vs_id))
    form = None
    if request.method == "POST":
        form = ValidationScenarioForm(request.POST)
        if form.is_valid():
            for key in form.cleaned_data:
                setattr(scenario, key, form.cleaned_data[key])
            scenario.save()
            return redirect(reverse('configuration'))
    if form is None:
        form = ValidationScenarioForm(model_to_dict(scenario))
    return render(request, 'core/edit_configuration_object.html', {
        'object': scenario,
        'form': form,
        'object_name': 'Validation Scenario',
    })


def delete_validation_scenario(request, vs_id):
    try:
        scenario = ValidationScenario.objects.get(pk=int(vs_id))
        scenario.delete()
    except ObjectDoesNotExist:
        pass
    return redirect(reverse('configuration'))


def test_validation_scenario(request):
    """
        View to live test validation scenario
        """

    # If GET, serve validation test screen
    if request.method == 'GET':
        # get validation scenarios
        validation_scenarios = ValidationScenario.objects.all()

        # check if limiting to one, pre-existing record
        get_q = request.GET.get('q', None)

        # check for pre-requested transformation scenario
        vsid = request.GET.get('validation_scenario', None)

        valid_types = get_validation_scenario_choices()

        # return
        return render(request, 'core/test_validation_scenario.html', {
            'q': get_q,
            'vsid': vsid,
            'validation_scenarios': validation_scenarios,
            'valid_types': valid_types,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # If POST, provide raw result of validation test
    if request.method == 'POST':

        LOGGER.debug('running test validation and returning')

        # get record
        record = Record.objects.get(id=request.POST.get('db_id'))

        try:
            # init new validation scenario
            scenario = ValidationScenario(
                name='temp_vs_%s' % str(uuid.uuid4()),
                payload=request.POST.get('vs_payload'),
                validation_type=request.POST.get('vs_type'),
                default_run=False
            )
            scenario.save()

            # validate with record
            vs_results = scenario.validate_record(record)

            # delete vs
            scenario.delete()

            if request.POST.get('vs_results_format') == 'raw':
                return HttpResponse(vs_results['raw'], content_type="text/plain")
            if request.POST.get('vs_results_format') == 'parsed':
                return JsonResponse(vs_results['parsed'])
            raise Exception('validation results format not recognized')

        except Exception as err:

            if scenario.id:
                # TODO: Not sure how to invoke this code
                LOGGER.debug(
                    'test validation scenario was unsuccessful, deleting temporary vs')
                scenario.delete()

            return HttpResponse(str(err), content_type="text/plain")
