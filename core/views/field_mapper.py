import json
import logging
import jsonschema

from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.urls import reverse
from django.forms import model_to_dict
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from core.models import FieldMapper, Record
from core.xml2kvp import XML2kvp
from core.forms import FieldMapperForm

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


def field_mapper_payload(request, fm_id):

    """
    View payload for field mapper
    """

    # get transformation
    field_mapper = FieldMapper.objects.get(pk=int(fm_id))

    # get type
    doc_type = request.GET.get('type', None)

    if field_mapper.field_mapper_type == 'xml2kvp':

        if not doc_type:
            return JsonResponse(json.loads(field_mapper.config_json))

        if doc_type and doc_type == 'config':
            return JsonResponse(json.loads(field_mapper.config_json))

        if doc_type and doc_type == 'payload':
            return JsonResponse(json.loads(field_mapper.payload))


def create_field_mapper(request):

    form = None
    if request.method == "POST":
        form = FieldMapperForm(request.POST)
        if form.is_valid():
            new_field_mapper = FieldMapper(**form.cleaned_data)
            new_field_mapper.save()
            return redirect(reverse('configuration'))
    if form is None:
        form = FieldMapperForm
        if getattr(settings, 'ENABLE_PYTHON', 'false') != 'true':
            no_python = list(filter(lambda x: x[0] != 'python',
                form.base_fields['field_mapper_type'].choices))
            form.base_fields['field_mapper_type'].choices = no_python
    return render(request, 'core/new_configuration_object.html', {
        'form': form,
        'object_name': 'Field Mapper',
    })


def edit_field_mapper(request, fm_id):

    field_mapper = FieldMapper.objects.get(pk=int(fm_id))
    form = None
    if request.method == 'POST':
        form = FieldMapperForm(request.POST)
        if form.is_valid():
            for key in form.cleaned_data:
                setattr(field_mapper, key, form.cleaned_data[key])
            field_mapper.save()
            return redirect(reverse('configuration'))
    if form is None:
        form = FieldMapperForm(model_to_dict(field_mapper))
    return render(request, 'core/edit_configuration_object.html', {
        'object': field_mapper,
        'form': form,
        'object_name': 'Field Mapper',
    })


def delete_field_mapper(request, fm_id):

    try:
        field_mapper = FieldMapper.objects.get(pk=int(fm_id))
        field_mapper.delete()
    except ObjectDoesNotExist:
        pass
    return redirect(reverse('configuration'))


def field_mapper_update(request):

    """
    Create and save JSON to FieldMapper instance, or update pre-existing
    """

    LOGGER.debug(request.POST)

    # get update type
    update_type = request.POST.get('update_type')

    # handle new FieldMapper creation
    if update_type == 'new':
        LOGGER.debug('creating new FieldMapper instance')

        field_mapper = FieldMapper(
            name=request.POST.get('fm_name'),
            config_json=request.POST.get('fm_config_json'),
            field_mapper_type='xml2kvp'
        )

        # validate fm_config before creating
        try:
            field_mapper.validate_config_json()
            field_mapper.save()
            return JsonResponse({'results': True,
                                 'msg': 'New Field Mapper configurations were <strong>saved</strong> as: <strong>%s</strong>' % request.POST.get(
                                     'fm_name')}, status=201)
        except jsonschema.ValidationError as err:
            return JsonResponse({'results': False,
                                 'msg': 'Could not <strong>create</strong> <strong>%s</strong>, the following error was had: %s' % (
                                     field_mapper.name, str(err))}, status=409)

    # handle update
    if update_type == 'update':
        LOGGER.debug('updating pre-existing FieldMapper instance')

        # get fm instance
        field_mapper = FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

        # update and save
        field_mapper.config_json = request.POST.get('fm_config_json')

        # validate fm_config before updating
        try:
            field_mapper.validate_config_json()
            field_mapper.save()
            return JsonResponse({'results': True,
                                 'msg': 'Field Mapper configurations for <strong>%s</strong> were <strong>updated</strong>' % field_mapper.name},
                                status=200)
        except jsonschema.ValidationError as err:
            return JsonResponse({'results': False,
                                 'msg': 'Could not <strong>update</strong> <strong>%s</strong>, the following error was had: %s' % (
                                     field_mapper.name, str(err))}, status=409)

    # handle delete
    if update_type == 'delete':
        LOGGER.debug('deleting pre-existing FieldMapper instance')

        # get fm instance
        field_mapper = FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

        # delete
        field_mapper.delete()
        return JsonResponse({'results': True,
                             'msg': 'Field Mapper configurations for <strong>%s</strong> were <strong>deleted</strong>' % field_mapper.name},
                            status=200)


def test_field_mapper(request):

    """
    View to live test field mapper configurations
    """

    if request.method == 'GET':
        # get field mapper
        field_mappers = FieldMapper.objects.all()

        # check if limiting to one, pre-existing record
        get_q = request.GET.get('q', None)

        # check for pre-requested transformation scenario
        fmid = request.GET.get('fmid', None)

        # return
        return render(request, 'core/test_field_mapper.html', {
            'q': get_q,
            'fmid': fmid,
            'field_mappers': field_mappers,
            'xml2kvp_handle': XML2kvp(),
            'breadcrumbs': breadcrumb_parser(request)
        })

    # If POST, provide mapping of record
    if request.method == 'POST':

        LOGGER.debug('running test field mapping')
        LOGGER.debug(request.POST)

        # get record
        record = Record.objects.get(id=request.POST.get('db_id'))

        # get field mapper info
        request.POST.get('field_mapper') # TODO: unused
        fm_config_json = request.POST.get('fm_config_json')

        try:

            # parse record with XML2kvp
            fm_config = json.loads(fm_config_json)
            kvp_dict = XML2kvp.xml_to_kvp(record.document, **fm_config)

            # return as JSON
            return JsonResponse(kvp_dict)

        except Exception as err:

            LOGGER.debug('field mapper was unsuccessful')
            return JsonResponse({'error': str(err)})
