import json
import logging

from django.shortcuts import render

from .view_helpers import breadcrumb_parser
from .stateio import _stateio_prepare_job_hierarchy

from core.es import es_handle
from elasticsearch_dsl import Search

LOGGER = logging.getLogger(__name__)


def search(request):
    """
    Global search of Records
    """

    # if search term present, use
    get_q = request.GET.get('q', None)
    if get_q:
        search_params = json.dumps({'q': get_q})
        LOGGER.debug(search_params)
    else:
        search_params = None

    # generate hierarchy_dict
    job_hierarchy = _stateio_prepare_job_hierarchy()

    return render(request, 'core/search.html', {
        'search_string': get_q,
        'search_params': search_params,
        'job_hierarchy_json': json.dumps(job_hierarchy),
        'breadcrumbs': breadcrumb_parser(request),
        'page_title': ' | Search'
    })

def test_search_query(request):
    """
    Develop ElasticSearch queries against existing Records to use for other Jobs.
    """

    indices = list(filter(lambda i: i[0] != '.', es_handle.indices.stats()['indices'].keys()))
    indices.sort()

    LOGGER.debug("********************")
    LOGGER.debug(indices)
    LOGGER.debug("********************")

    return render(request, 'core/test_search_query.html', {
        'indices': indices
    })

