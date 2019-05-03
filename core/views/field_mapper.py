import json
import jsonschema
import logging

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from core import models

from .views import breadcrumb_parser

logger = logging.getLogger(__name__)


def field_mapper_payload(request, fm_id):
    '''
	View payload for field mapper
	'''

    # get transformation
    fm = models.FieldMapper.objects.get(pk=int(fm_id))

    # get type
    doc_type = request.GET.get('type', None)

    if fm.field_mapper_type == 'xml2kvp':

        if not doc_type:
            return HttpResponse(fm.config_json, content_type='application/json')

        elif doc_type and doc_type == 'config':
            return HttpResponse(fm.config_json, content_type='application/json')

        elif doc_type and doc_type == 'payload':
            return HttpResponse(fm.payload, content_type='application/json')


def field_mapper_update(request):
    '''
	Create and save JSON to FieldMapper instance, or update pre-existing
	'''

    logger.debug(request.POST)

    # get update type
    update_type = request.POST.get('update_type')

    # handle new FieldMapper creation
    if update_type == 'new':
        logger.debug('creating new FieldMapper instance')

        fm = models.FieldMapper(
            name=request.POST.get('fm_name'),
            config_json=request.POST.get('fm_config_json'),
            field_mapper_type='xml2kvp'
        )

        # validate fm_config before creating
        try:
            fm.validate_config_json()
            fm.save()
            return JsonResponse({'results': True,
                                 'msg': 'New Field Mapper configurations were <strong>saved</strong> as: <strong>%s</strong>' % request.POST.get(
                                     'fm_name')}, status=201)
        except jsonschema.ValidationError as e:
            return JsonResponse({'results': False,
                                 'msg': 'Could not <strong>create</strong> <strong>%s</strong>, the following error was had: %s' % (
                                     fm.name, str(e))}, status=409)

    # handle update
    if update_type == 'update':
        logger.debug('updating pre-existing FieldMapper instance')

        # get fm instance
        fm = models.FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

        # update and save
        fm.config_json = request.POST.get('fm_config_json')

        # validate fm_config before updating
        try:
            fm.validate_config_json()
            fm.save()
            return JsonResponse({'results': True,
                                 'msg': 'Field Mapper configurations for <strong>%s</strong> were <strong>updated</strong>' % fm.name},
                                status=200)
        except jsonschema.ValidationError as e:
            return JsonResponse({'results': False,
                                 'msg': 'Could not <strong>update</strong> <strong>%s</strong>, the following error was had: %s' % (
                                     fm.name, str(e))}, status=409)

    # handle delete
    if update_type == 'delete':
        logger.debug('deleting pre-existing FieldMapper instance')

        # get fm instance
        fm = models.FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

        # delete
        fm.delete()
        return JsonResponse({'results': True,
                             'msg': 'Field Mapper configurations for <strong>%s</strong> were <strong>deleted</strong>' % fm.name},
                            status=200)


def test_field_mapper(request):
    '''
	View to live test field mapper configurations
	'''

    if request.method == 'GET':
        # get field mapper
        field_mappers = models.FieldMapper.objects.all()

        # check if limiting to one, pre-existing record
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

        logger.debug('running test field mapping')
        logger.debug(request.POST)

        # get record
        record = models.Record.objects.get(id=request.POST.get('db_id'))

        # get field mapper info
        field_mapper = request.POST.get('field_mapper')
        fm_config_json = request.POST.get('fm_config_json')

        try:

            # parse record with XML2kvp
            fm_config = json.loads(fm_config_json)
            kvp_dict = models.XML2kvp.xml_to_kvp(record.document, **fm_config)

            # return as JSON
            return JsonResponse(kvp_dict)

        except Exception as e:

            logger.debug('field mapper was unsucessful')
            return JsonResponse({'error': str(e)})
