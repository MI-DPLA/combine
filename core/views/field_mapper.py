import json
import logging
import jsonschema

from django.http import JsonResponse
from django.shortcuts import render

from core import models

from .view_helpers import breadcrumb_parser

LOGGER = logging.getLogger(__name__)


def field_mapper_payload(request, fm_id):
    """
        View payload for field mapper
        """

    # get transformation
    field_mapper = models.FieldMapper.objects.get(pk=int(fm_id))

    # get type
    doc_type = request.GET.get('type', None)

    if field_mapper.field_mapper_type == 'xml2kvp':

        if not doc_type:
            return JsonResponse(field_mapper.config_json, safe=False)

        if doc_type and doc_type == 'config':
            return JsonResponse(field_mapper.config_json, safe=False)

        if doc_type and doc_type == 'payload':
            return JsonResponse(field_mapper.payload, safe=False)


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

        field_mapper = models.FieldMapper(
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
        field_mapper = models.FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

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
        field_mapper = models.FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

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
        field_mappers = models.FieldMapper.objects.all()

        # check if limiting to one, pre-existing record
        # TODO: what is q?
        q = request.GET.get('q', None)

        # check for pre-requested transformation scenario
        fmid = request.GET.get('fmid', None)

        # return
        return render(request, 'core/test_field_mapper.html', {
            'q': q,
            'fmid': fmid,
            'field_mappers': field_mappers,
            'xml2kvp_handle': models.XML2kvp(),
            'breadcrumbs': breadcrumb_parser(request)
        })

    # If POST, provide mapping of record
    if request.method == 'POST':

        LOGGER.debug('running test field mapping')
        LOGGER.debug(request.POST)

        # get record
        record = models.Record.objects.get(id=request.POST.get('db_id'))

        # get field mapper info
        request.POST.get('field_mapper') # TODO: unused
        fm_config_json = request.POST.get('fm_config_json')

        try:

            # parse record with XML2kvp
            fm_config = json.loads(fm_config_json)
            kvp_dict = models.XML2kvp.xml_to_kvp(record.document, **fm_config)

            # return as JSON
            return JsonResponse(kvp_dict)

        except Exception as err:

            LOGGER.debug('field mapper was unsucessful')
            return JsonResponse({'error': str(err)})