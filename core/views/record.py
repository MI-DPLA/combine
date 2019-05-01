import json
import logging

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from core import models

from .views import breadcrumb_parser

logger = logging.getLogger(__name__)

def record(request, org_id, record_group_id, job_id, record_id):
    '''
	Single Record page
	'''

    # get record
    record = models.Record.objects.get(id=record_id)

    # build ancestry in both directions
    record_stages = record.get_record_stages()

    # get details depending on job type
    logger.debug('Job type is %s, retrieving details' % record.job.job_type)
    try:
        job_details = record.job.job_details_dict
    except:
        logger.debug('could not load job details')
        job_details = {}

    # attempt to retrieve pre-existing DPLA document
    dpla_api_doc = record.dpla_api_record_match()
    if dpla_api_doc is not None:
        dpla_api_json = json.dumps(dpla_api_doc, indent=4, sort_keys=True)
    else:
        dpla_api_json = None

    # retrieve diffs, if any, from input record
    # request only combined diff at this point
    record_diff_dict = record.get_input_record_diff(output='combined_gen', combined_as_html=True)

    # retrieve field mapper config json used
    try:
        job_fm_config_json = json.dumps(job_details['field_mapper_config'])
    except:
        job_fm_config_json = json.dumps({'error': 'job field mapping configuration json could not be found'})

    # attempt to get document as pretty print
    try:
        pretty_document = record.document_pretty_print()
        pretty_format_msg = False
    except Exception as e:
        pretty_document = record.document
        pretty_format_msg = str(e)

    # return
    return render(request, 'core/record.html', {
        'record_id': record_id,
        'record': record,
        'record_stages': record_stages,
        'job_details': job_details,
        'dpla_api_doc': dpla_api_doc,
        'dpla_api_json': dpla_api_json,
        'record_diff_dict': record_diff_dict,
        'pretty_document': pretty_document,
        'pretty_format_msg': pretty_format_msg,
        'job_fm_config_json': job_fm_config_json,
        'breadcrumbs': breadcrumb_parser(request)
    })


def record_document(request, org_id, record_group_id, job_id, record_id):
    '''
	View document for record
	'''

    # get record
    record = models.Record.objects.get(id=record_id)

    # return document as XML
    return HttpResponse(record.document, content_type='text/xml')


def record_indexed_document(request, org_id, record_group_id, job_id, record_id):
    '''
	View indexed, ES document for record
	'''

    # get record
    record = models.Record.objects.get(id=record_id)

    # return ES document as JSON
    return JsonResponse(record.get_es_doc())


def record_error(request, org_id, record_group_id, job_id, record_id):
    '''
	View document for record
	'''

    # get record
    record = models.Record.objects.get(id=record_id)

    # return document as XML
    return HttpResponse("<pre>%s</pre>" % record.error)


def record_validation_scenario(request, org_id, record_group_id, job_id, record_id, job_validation_id):
    '''
	Re-run validation test for single record

	Returns:
		results of validation
	'''

    # get record
    record = models.Record.objects.get(id=record_id)

    # get validation scenario
    vs = models.ValidationScenario.objects.get(pk=int(job_validation_id))

    # schematron type validation
    if vs.validation_type == 'sch':
        vs_result = vs.validate_record(record)

        # return
        return HttpResponse(vs_result['raw'], content_type='text/xml')

    # python type validation
    if vs.validation_type == 'python':
        vs_result = vs.validate_record(record)

        # return
        return JsonResponse(vs_result['parsed'], safe=False)


def record_combined_diff_html(request, org_id, record_group_id, job_id, record_id):
    '''
	Return combined diff of Record against Input Record
	'''

    # get record
    record = models.Record.objects.get(id=record_id)

    # get side_by_side diff as HTML
    diff_dict = record.get_input_record_diff(output='combined_gen', combined_as_html=True)

    if diff_dict:

        # get combined output as html from output
        html = diff_dict['combined_gen']

        # return document as HTML
        return HttpResponse(html, content_type='text/html')

    else:
        return HttpResponse("Record was not altered during Transformation.", content_type='text/html')


def record_side_by_side_diff_html(request, org_id, record_group_id, job_id, record_id):
    '''
	Return side_by_side diff of Record against Input Record
		- uses sxsdiff (https://github.com/timonwong/sxsdiff)
		- if embed == true, strip some uncessary HTML and return
	'''

    # get record
    record = models.Record.objects.get(id=record_id)

    # check for embed flag
    embed = request.GET.get('embed', False)

    # get side_by_side diff as HTML
    diff_dict = record.get_input_record_diff(output='side_by_side_html')

    if diff_dict:

        # get side_by_side html from output
        html = diff_dict['side_by_side_html']

        # if embed flag set, alter CSS
        # these are defaulted in sxsdiff library, currently
        # easier to pinpoint and remove these than fork library and alter
        html = html.replace('<div class="container">', '<div>')
        html = html.replace('padding-left:30px;', '/*padding-left:30px;*/')
        html = html.replace('padding-right:30px;', '/*padding-right:30px;*/')

        # return document as HTML
        return HttpResponse(html, content_type='text/html')

    else:
        return HttpResponse("Record was not altered during Transformation.", content_type='text/html')

