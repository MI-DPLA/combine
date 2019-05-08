import json
import logging

from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render

from core import models
from core.mongo import mc_handle

from .view_helpers import breadcrumb_parser
from .stateio import _stateio_prepare_job_hierarchy

LOGGER = logging.getLogger(__name__)


@login_required
def published(request, subset=None):
    """
        Published records
        """

    # get instance of Published model
    pub_records = models.PublishedRecords(subset=subset)

    # get field counts
    if pub_records.records.count() > 0:
        # get count of fields for all published job indices
        field_counts = pub_records.count_indexed_fields()
    else:
        field_counts = {}

    # get field mappers
    field_mappers = models.FieldMapper.objects.all()

    # get published subsets with PublishedRecords static method
    subsets = models.PublishedRecords.get_subsets()

    # loop through subsets and enrich
    for _ in subsets:

        # add counts
        counts = mc_handle.combine.misc.find_one(
            {'_id': 'published_field_counts_%s' % _['name']})

        # if counts not yet calculated, do now
        if counts is None:
            counts = models.PublishedRecords(
                subset=_['name']).count_indexed_fields()
        _['counts'] = counts

    # generate hierarchy_dict
    job_hierarchy = _stateio_prepare_job_hierarchy()

    return render(request, 'core/published.html', {
        'published': pub_records,
        'field_mappers': field_mappers,
        'xml2kvp_handle': models.XML2kvp(),
        'field_counts': field_counts,
        'es_index_str': pub_records.esi.es_index_str,
        'subsets': subsets,
        'job_hierarchy_json': json.dumps(job_hierarchy),
        'job_hierarchy_json_subset': json.dumps(
            getattr(pub_records, 'ps_doc', {}).get('hierarchy', [])
        ),
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def published_subset_create(request):
    """
        Create subset of published records
                - output should be a Mongo document in combine.misc
                called "published_subset_[SUBSET]"

        Subset Form/Doc
                - slug/id for subset: lowercase, no spaces, sanitize
                - human name
                - description
                - publish sets to include
                        - also include "loose" records?
        """

    if request.method == 'GET':

        # get all published sets
        pub_records = models.PublishedRecords()

        # generate hierarchy_dict
        job_hierarchy = _stateio_prepare_job_hierarchy()

        return render(request, 'core/published_subset_create.html', {
            'published': pub_records,
            'job_hierarchy_json': json.dumps(job_hierarchy),
            'breadcrumbs': breadcrumb_parser(request)
        })

    if request.method == 'POST':

        LOGGER.debug('creating new published subset')

        # sanitize name
        name = request.POST.get('name')
        name = ''.join(c for c in name if c.isalnum())
        name = name.lower()

        # confirm sets are present
        sets = request.POST.getlist('sets')

        # handle non set records
        include_non_set_records = request.POST.get('include_non_set_records', False)

        # handle org / rg hierarchy
        hierarchy = json.loads(request.POST.get('hierarchy', []))

        # create new published subset
        mc_handle.combine.misc.insert_one(
            {
                'name': name,
                'description': request.POST.get('description', None),
                'type': 'published_subset',
                'publish_set_ids': sets,
                'hierarchy': hierarchy,
                'include_non_set_records': include_non_set_records
            })

        return redirect('published_subset',
                        subset=name)


@login_required
def published_subset_edit(request, subset):
    """
        Edit Published Subset
        """

    if request.method == 'GET':

        # get subset published records
        pub_records = models.PublishedRecords()
        published_subset = models.PublishedRecords(subset=subset)
        published_subset.ps_doc['id'] = str(published_subset.ps_doc['_id'])

        # generate hierarchy_dict
        job_hierarchy = _stateio_prepare_job_hierarchy()

        return render(request, 'core/published_subset_edit.html', {
            'published': pub_records,
            'published_subset': published_subset,
            'job_hierarchy_json': json.dumps(job_hierarchy),
            'job_hierarchy_json_subset': json.dumps(published_subset.ps_doc.get('hierarchy', [])),
            'breadcrumbs': breadcrumb_parser(request)
        })

    if request.method == 'POST':

        LOGGER.debug('updating published subset')

        # confirm sets are present
        sets = request.POST.getlist('sets')

        # handle non set records
        include_non_set_records = request.POST.get('include_non_set_records', False)

        # handle org / rg hierarchy
        hierarchy = json.loads(request.POST.get('hierarchy', []))

        # update published subset
        pub_records = models.PublishedRecords(subset=subset)
        pub_records.update_subset({
            'description': request.POST.get('description', None),
            'type': 'published_subset',
            'publish_set_ids': sets,
            'hierarchy': hierarchy,
            'include_non_set_records': include_non_set_records
        })
        pub_records.remove_subset_precounts()

        return redirect('published_subset',
                        subset=subset)


@login_required
def published_subset_delete(request, subset):
    """
        Delete published subset
        """

    deleted = mc_handle.combine.misc.delete_one(
        {'type': 'published_subset', 'name': subset})
    LOGGER.debug(deleted.raw_result)
    deleted = mc_handle.combine.misc.delete_one(
        {'_id': 'published_field_counts_%s' % subset})
    LOGGER.debug(deleted.raw_result)
    return redirect('published')
