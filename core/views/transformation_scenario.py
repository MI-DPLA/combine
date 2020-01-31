import json
import logging
import uuid

from django.forms.models import model_to_dict
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.urls import reverse
from django.core.exceptions import ObjectDoesNotExist

from core.models import Record, Transformation
from core.forms import TransformationForm
from core.models.transformation import get_transformation_type_choices

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


def transformation_scenario_payload(request, trans_id):
    """
        View payload for transformation scenario
        """

    # get transformation
    transformation = Transformation.objects.get(pk=int(trans_id))

    # return transformation as XML
    if transformation.transformation_type == 'xslt':
        return HttpResponse(transformation.payload, content_type='text/xml')

    # return transformation as Python
    if transformation.transformation_type == 'python':
        return HttpResponse(transformation.payload, content_type='text/plain')

    # return transformation as Python
    if transformation.transformation_type == 'openrefine':
        return HttpResponse(transformation.payload, content_type='text/plain')


def create_transformation_scenario(request):
    form = None
    if request.method == "POST":
        form = TransformationForm(request.POST)
        if form.is_valid():
            new_transformation = Transformation(**form.cleaned_data)
            new_transformation.save()
            return redirect(reverse('configuration'))
    if form is None:
        form = TransformationForm()
    return render(request, 'core/new_configuration_object.html', {
        'form': form,
        'object_name': 'Transformation Scenario'
    })


def transformation_scenario(request, ts_id):
    transformation = Transformation.objects.get(pk=int(ts_id))
    form = None
    if request.method == 'POST':
        form = TransformationForm(request.POST)
        if form.is_valid():
            for key in form.cleaned_data:
                setattr(transformation, key, form.cleaned_data[key])
            transformation.save()
            return redirect(reverse('configuration'))
    if form is None:
        form = TransformationForm(model_to_dict(transformation))
    return render(request, 'core/edit_configuration_object.html', {
        'object': transformation,
        'form': form,
        'object_name': 'Transformation Scenario',
    })


def delete_transformation_scenario(request, ts_id):
    try:
        transformation = Transformation.objects.get(pk=int(ts_id))
        transformation.delete()
    except ObjectDoesNotExist:
        pass
    return redirect(reverse('configuration'))


def test_transformation_scenario(request):
    """
        View to live test transformation scenarios
        """

    # If GET, serve transformation test screen
    if request.method == 'GET':
        # get validation scenarios
        transformation_scenarios = Transformation.objects.filter(
            use_as_include=False)

        # check if limiting to one, pre-existing record
        get_q = request.GET.get('q', None)

        # check for pre-requested transformation scenario
        tsid = request.GET.get('transformation_scenario', None)

        valid_types = get_transformation_type_choices()

        # return
        return render(request, 'core/test_transformation_scenario.html', {
            'q': get_q,
            'tsid': tsid,
            'transformation_scenarios': transformation_scenarios,
            'valid_types': valid_types,
            'breadcrumbs': breadcrumb_parser(request)
        })

    # If POST, provide raw result of validation test
    if request.method == 'POST':

        LOGGER.debug('running test transformation and returning')

        # get response type
        response_type = request.POST.get('response_type', False)

        # get record
        record = Record.objects.get(id=request.POST.get('db_id'))
        record_iter = Record.objects.get(id=request.POST.get('db_id'))

        try:

            # testing multiple, chained transformations
            if request.POST.get('trans_test_type') == 'multiple':

                # get and rehydrate sel_trans_json
                sel_trans = json.loads(request.POST.get('sel_trans_json'))

                # loop through transformations
                for trans in sel_trans:
                    # init Transformation instance
                    trans = Transformation.objects.get(
                        pk=int(trans['trans_id']))

                    # transform with record
                    trans_results = trans.transform_record(record_iter)

                    # set to record.document for next iteration
                    record_iter.document = trans_results

                # finally, fall in line with trans_results as record_iter document string
                trans_results = record_iter.document

            # testing single transformation
            elif request.POST.get('trans_test_type') == 'single':

                # init new transformation scenario
                trans = Transformation(
                    name='temp_trans_%s' % str(uuid.uuid4()),
                    payload=request.POST.get('trans_payload'),
                    transformation_type=request.POST.get('trans_type')
                )
                trans.save()

                # transform with record
                trans_results = trans.transform_record(record)

                # delete temporary trans
                trans.delete()

            # if raw transformation results
            if response_type == 'transformed_doc':
                return HttpResponse(trans_results, content_type="text/xml")

            # get diff of original record as combined results
            if response_type == 'combined_html':

                # get combined diff as HTML
                diff_dict = record.get_record_diff(xml_string=trans_results, output='combined_gen',
                                                   combined_as_html=True, reverse_direction=True)
                if diff_dict:
                    diff_html = diff_dict['combined_gen']

                return HttpResponse(diff_html, content_type="text/xml")

            # get diff of original record as side_by_side
            if response_type == 'side_by_side_html':

                # get side_by_side diff as HTML
                diff_dict = record.get_record_diff(xml_string=trans_results, output='side_by_side_html',
                                                   reverse_direction=True)
                if diff_dict:
                    diff_html = diff_dict['side_by_side_html']

                    # strip some CSS
                    diff_html = diff_html.replace(
                        '<div class="container">', '<div>')
                    diff_html = diff_html.replace(
                        'padding-left:30px;', '/*padding-left:30px;*/')
                    diff_html = diff_html.replace(
                        'padding-right:30px;', '/*padding-right:30px;*/')

                return HttpResponse(diff_html, content_type="text/xml")

        except Exception as err:
            LOGGER.debug(
                'test transformation scenario was unsucessful, deleting temporary')
            try:
                if request.POST.get('trans_test_type') == 'single':
                    trans.delete()
            except:
                LOGGER.debug('could not delete temporary transformation')
            return HttpResponse(str(err), content_type="text/plain")
