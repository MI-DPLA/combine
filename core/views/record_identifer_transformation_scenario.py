import logging

from django.http import JsonResponse
from django.forms.models import model_to_dict
from django.shortcuts import render, redirect
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from core.models import RecordIdentifierTransformationScenario, Record, RITSClient
from core.forms import RITSForm

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


def rits_payload(request, rits_id):
    """
        View payload for record identifier transformation scenario
        """

    # get transformation
    rits = RecordIdentifierTransformationScenario.objects.get(
        pk=int(rits_id))

    # return as json package
    return JsonResponse(model_to_dict(rits))


def create_rits(request):
    if request.method == 'POST':
        form = RITSForm(request.POST)
        if form.is_valid():
            new_rits = RecordIdentifierTransformationScenario(**form.cleaned_data)
            new_rits.save()
        return redirect(reverse('configuration'))
    form = RITSForm()
    return render(request, 'core/new_configuration_object.html', {
        'form': form,
        'object_name': 'Record Identifier Transformation Scenario',
    })


def edit_rits(request, rits_id):
    rits = RecordIdentifierTransformationScenario.objects.get(pk=int(rits_id))
    if request.method == 'POST':
        form = RITSForm(request.POST)
        if form.is_valid():
            for key in form.cleaned_data:
                setattr(rits, key, form.cleaned_data[key])
            rits.save()
        return redirect(reverse('configuration'))
    form = RITSForm(model_to_dict(rits))
    return render(request, 'core/edit_configuration_object.html', {
        'object': rits,
        'form': form,
        'object_name': 'Record Identifier Transformation Scenario',
    })


def delete_rits(request, rits_id):
    try:
        rits = RecordIdentifierTransformationScenario.objects.get(pk=int(rits_id))
        rits.delete()
    except ObjectDoesNotExist:
        pass
    return redirect(reverse('configuration'))


def test_rits(request):
    """
        View to live test record identifier transformation scenarios
        """

    # If GET, serve validation test screen
    if request.method == 'GET':
        # check if limiting to one, pre-existing record
        get_q = request.GET.get('q', None)

        # get record identifier transformation scenarios
        rits = RecordIdentifierTransformationScenario.objects.all()

        # return
        return render(request, 'core/test_rits.html', {
            'q': get_q,
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
                record = Record.objects.get(
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
            rits = RITSClient(request.POST)
            return JsonResponse(rits.test_user_input())

        except Exception as err:
            return JsonResponse({'results': str(err), 'success': False})
