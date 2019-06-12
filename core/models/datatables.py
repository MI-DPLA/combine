# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import ast
import json
import logging
import time

# pandas
import pandas as pd

# django imports
from django.http import JsonResponse
from django.views import View

from core.es import es_handle
from core.models.elasticsearch import ESIndex
from core.models.job import Record

from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.utils import AttrList

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


class DTElasticFieldSearch(View):

    '''
    Model to query ElasticSearch and return DataTables ready JSON.
    This model is a Django Class-based view.
    This model is located in core.models, as it still may function seperate from a Django view.

    NOTE: Consider breaking aggregation search to own class, very different approach
    '''

    def __init__(
            self,
            fields=None,
            es_index=None,
            dt_input={
                'draw':None,
                'start':0,
                'length':10
            }
        ):
        '''
        Args:
            fields (list): list of fields to return from ES index
            es_index (str): ES index
            dt_input (dict): DataTables formatted GET parameters as dictionary

        Returns:
            None
                - sets parameters
        '''

        LOGGER.debug('initiating DTElasticFieldSearch connector')

        # fields to retrieve from index
        self.fields = fields

        # ES index
        self.es_index = es_index

        # dictionary INPUT DataTables ajax
        self.dt_input = dt_input

        # placeholder for query to build
        self.query = None

        # request
        self.request = None

        # dictionary OUTPUT to DataTables
        # self.dt_output = DTResponse().__dict__
        self.dt_output = {
            'draw': None,
            'recordsTotal': None,
            'recordsFiltered': None,
            'data': []
        }
        self.dt_output['draw'] = dt_input['draw']


    def filter(self):

        '''
        Filter based on dt_input paramters

        Args:
            None

        Returns:
            None
                - modifies self.query
        '''

        # filtering applied before DataTables input
        filter_type = self.request.GET.get('filter_type', None)
        filter_field = self.request.GET.get('filter_field', None)
        filter_value = self.request.GET.get('filter_value', None)

        # equals filtering
        if filter_type == 'equals':
            LOGGER.debug('equals type filtering')

            # determine if including or excluding
            matches = self.request.GET.get('matches', None)
            matches = bool(matches and matches.lower() == 'true')

            # filter query
            LOGGER.debug('filtering by field:value: %s:%s', filter_field, filter_value)

            if matches:
                LOGGER.debug('filtering to matches')
                self.query = self.query.filter(Q('term', **{'%s.keyword' % filter_field : filter_value}))
            else:
                # filter where filter_field == filter_value AND filter_field exists
                LOGGER.debug('filtering to non-matches')
                self.query = self.query.exclude(Q('term', **{'%s.keyword' % filter_field : filter_value}))
                self.query = self.query.filter(Q('exists', field=filter_field))

        # exists filtering
        elif filter_type == 'exists':
            LOGGER.debug('exists type filtering')

            # determine if including or excluding
            exists = self.request.GET.get('exists', None)
            exists = bool(exists and exists.lower() == 'true')

            # filter query
            if exists:
                LOGGER.debug('filtering to exists')
                self.query = self.query.filter(Q('exists', field=filter_field))
            else:
                LOGGER.debug('filtering to non-exists')
                self.query = self.query.exclude(Q('exists', field=filter_field))

        # further filter by DT provided keyword
        if self.dt_input['search[value]'] != '':
            LOGGER.debug('general type filtering')
            self.query = self.query.query('match', _all=self.dt_input['search[value]'])


    def sort(self):

        '''
        Sort based on dt_input parameters.

        Note: Sorting is different for the different types of requests made to DTElasticFieldSearch.

        Args:
            None

        Returns:
            None
                - modifies self.query_results
        '''

        # get sort params from dt_input
        sorting_cols = 0
        sort_key = 'order[%s][column]' % (sorting_cols)
        while sort_key in self.dt_input:
            sorting_cols += 1
            sort_key = 'order[%s][column]' % (sorting_cols)

        for i in range(sorting_cols):
            # sorting column
            sort_dir = 'asc'
            sort_col = int(self.dt_input.get('order[%s][column]' % (i)))
            # sorting order
            sort_dir = self.dt_input.get('order[%s][dir]' % (i))

            LOGGER.debug('detected sort: %s / %s', sort_col, sort_dir)

        # field per doc (ES Search Results)
        if self.search_type == 'fields_per_doc':

            # determine if field is sortable
            if sort_col < len(self.fields):

                # if db_id, do not add keyword
                if self.fields[sort_col] == 'db_id':
                    sort_field_string = self.fields[sort_col]
                # else, add .keyword
                else:
                    sort_field_string = "%s.keyword" % self.fields[sort_col]

                if sort_dir == 'desc':
                    sort_field_string = "-%s" % sort_field_string
                LOGGER.debug("sortable field, sorting by %s, %s", sort_field_string, sort_dir)
            else:
                LOGGER.debug("cannot sort by column %s", sort_col)

            # apply sorting to query
            self.query = self.query.sort(sort_field_string)

        # value per field (DataFrame)
        if self.search_type == 'values_per_field':

            if sort_col < len(self.query_results.columns):
                asc = True
                if sort_dir == 'desc':
                    asc = False
                self.query_results = self.query_results.sort_values(self.query_results.columns[sort_col], ascending=asc)


    def paginate(self):

        '''
        Paginate based on dt_input paramters

        Args:
            None

        Returns:
            None
                - modifies self.query
        '''

        # using offset (start) and limit (length)
        start = int(self.dt_input['start'])
        length = int(self.dt_input['length'])

        if self.search_type == 'fields_per_doc':
            self.query = self.query[start : (start + length)]

        if self.search_type == 'values_per_field':
            self.query_results = self.query_results[start : (start + length)]


    def to_json(self):

        '''
        Return dt_output as JSON

        Returns:
            (json)
        '''

        return json.dumps(self.dt_output)


    def get(self, request, es_index, search_type):

        '''
        Django Class-based view, GET request.
        Route to appropriate response builder (e.g. fields_per_doc, values_per_field)

        Args:
            request (django.request): request object
            es_index (str): ES index
        '''

        # save request
        self.request = request

        # handle es index
        esi = ESIndex(ast.literal_eval(es_index))
        self.es_index = esi.es_index

        # save DT params
        self.dt_input = self.request.GET

        # time respond build
        stime = time.time()

        # return fields per document
        if search_type == 'fields_per_doc':
            self.fields_per_doc()

        # aggregate-based search, count of values per field
        if search_type == 'values_per_field':
            self.values_per_field()

        # end time
        LOGGER.debug('DTElasticFieldSearch calc time: %s', (time.time()-stime))

        # for all search types, build and return response
        return JsonResponse(self.dt_output)


    def fields_per_doc(self):

        '''
        Perform search to get all fields, for all docs.
        Loops through self.fields, returns rows per ES document with values (or None) for those fields.
        Helpful for high-level understanding of documents for a given query.

        Note: can be used outside of Django context, but must set self.fields first
        '''

        # set search type
        self.search_type = 'fields_per_doc'

        # get field names
        if self.request:
            field_names = self.request.GET.getlist('field_names')
            self.fields = field_names

        # initiate es query
        self.query = Search(using=es_handle, index=self.es_index)

        # get total document count, pre-filtering
        self.dt_output['recordsTotal'] = self.query.count()

        # apply filtering to ES query
        self.filter()

        # apply sorting to ES query
        self.sort()

        # self.sort()
        self.paginate()

        # get document count, post-filtering
        self.dt_output['recordsFiltered'] = self.query.count()

        # execute and retrieve search
        self.query_results = self.query.execute()

        # loop through hits
        for hit in self.query_results.hits:

            # get combine record
            record = Record.objects.get(id=hit.db_id)

            # loop through rows, add to list while handling data types
            row_data = []
            for field in self.fields:
                field_value = getattr(hit, field, None)

                # handle ES lists
                if isinstance(field_value, AttrList):
                    row_data.append(str(field_value))

                # all else, append
                else:
                    row_data.append(field_value)

            # place record's org_id, record_group_id, and job_id in front
            row_data = [
                record.job.record_group.organization.id,
                record.job.record_group.id,
                record.job.id
            ] + row_data

            # add list to object
            self.dt_output['data'].append(row_data)


    def values_per_field(self, terms_limit=10000):

        '''
        Perform aggregation-based search to get count of values for single field.
        Helpful for understanding breakdown of a particular field's values and usage across documents.

        Note: can be used outside of Django context, but must set self.fields first
        '''

        # set search type
        self.search_type = 'values_per_field'

        # get single field
        if self.request:
            self.fields = self.request.GET.getlist('field_names')
            self.field = self.fields[0]
        else:
            self.field = self.fields[0] # expects only one for this search type, take first

        # initiate es query
        self.query = Search(using=es_handle, index=self.es_index)

        # add agg bucket for field values
        self.query.aggs.bucket(self.field, A('terms', field='%s.keyword' % self.field, size=terms_limit))

        # return zero
        self.query = self.query[0]

        # apply filtering to ES query
        self.filter()

        # execute search and convert to dataframe
        search_result = self.query.execute()
        self.query_results = pd.DataFrame([val.to_dict() for val in search_result.aggs[self.field]['buckets']])

        # rearrange columns
        cols = self.query_results.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        self.query_results = self.query_results[cols]

        # get total document count, pre-filtering
        self.dt_output['recordsTotal'] = len(self.query_results)

        # get document count, post-filtering
        self.dt_output['recordsFiltered'] = len(self.query_results)

        # apply sorting to DataFrame
        '''
        Think through if sorting on ES query or resulting Dataframe is better option.
        Might have to be DataFrame, as sorting is not allowed for aggregations in ES when they are string type:
        https://discuss.elastic.co/t/ordering-terms-aggregation-based-on-pipeline-metric/31839/2
        '''
        self.sort()

        # paginate
        self.paginate()

        # loop through field values
        for index, row in self.query_results.iterrows():

            # iterate through columns and place in list
            row_data = [row.key, row.doc_count]

            # add list to object
            self.dt_output['data'].append(row_data)



class DTElasticGenericSearch(View):

    '''
    Model to query ElasticSearch and return DataTables ready JSON.
    This model is a Django Class-based view.
    This model is located in core.models, as it still may function seperate from a Django view.
    '''

    def __init__(
            self,
            fields=['db_id', 'combine_id', 'record_id'],
            es_index='j*',
            dt_input={
                'draw':None,
                'start':0,
                'length':10
            }
        ):
        '''
        Args:
            fields (list): list of fields to return from ES index
            es_index (str): ES index
            dt_input (dict): DataTables formatted GET parameters as dictionary

        Returns:
            None
                - sets parameters
        '''

        LOGGER.debug('initiating DTElasticGenericSearch connector')

        # fields to retrieve from index
        self.fields = fields

        # ES index
        self.es_index = es_index

        # dictionary INPUT DataTables ajax
        self.dt_input = dt_input

        # placeholder for query to build
        self.query = None

        # request
        self.request = None

        # dictionary OUTPUT to DataTables
        # self.dt_output = DTResponse().__dict__
        self.dt_output = {
            'draw': None,
            'recordsTotal': None,
            'recordsFiltered': None,
            'data': []
        }
        self.dt_output['draw'] = dt_input['draw']


    def filter(self):

        '''
        Filter based on dt_input paramters

        Args:
            None

        Returns:
            None
                - modifies self.query
        '''

        LOGGER.debug('DTElasticGenericSearch: filtering')

        # get search string if present
        search_term = self.request.GET.get('search[value]')

        # if search term present, refine query
        if search_term != '':

            # escape colons
            search_term = search_term.replace(':', '\:')

            # get search type
            search_type = self.request.GET.get('search_type', None)

            # exact phrase (full case-sensitive string)
            if search_type == 'exact_phrase':

                self.query = self.query.query(
                    Q(
                        "multi_match",
                        query=search_term.replace("'", "\'"),
                        fields=['*.keyword']))

            # wildcard (wildcard searching against full keyword)
            elif search_type == 'wildcard':

                self.query = self.query.query(
                    'query_string',
                    query=search_term.replace("'", "\'"),
                    fields=["*.keyword"],
                    analyze_wildcard=True)

            # any token (matches single tokens)
            elif search_type in ['any_token', None]:

                self.query = self.query.query(
                    "match",
                    _all=search_term.replace("'", "\'"))


    def sort(self):

        '''
        Sort based on dt_input parameters.

        Note: Sorting is different for the different types of requests made to DTElasticFieldSearch.

        Args:
            None

        Returns:
            None
                - modifies self.query_results
        '''

        # if using deep paging, will need to implement some sorting to search_after
        self.query = self.query.sort('record_id.keyword', 'db_id.keyword')


    def paginate(self):

        '''
        Paginate based on dt_input paramters

        Args:
            None

        Returns:
            None
                - modifies self.query
        '''

        # using offset (start) and limit (length)
        start = int(self.dt_input['start'])
        length = int(self.dt_input['length'])
        self.query = self.query[start : (start + length)]

        # use search_after for "deep paging"
        """
        This will require capturing current sorts from the DT table, and applying last
        value here
        """
        # self.query = self.query.extra(search_after=['036182a450f31181cf678197523e2023',1182966])


    def to_json(self):

        '''
        Return dt_output as JSON

        Returns:
            (json)
        '''

        return json.dumps(self.dt_output)


    def get(self, request):

        '''
        Django Class-based view, GET request.

        Args:
            request (django.request): request object
            es_index (str): ES index
        '''

        # save parameters to self
        self.request = request
        self.dt_input = self.request.GET

        # filter indices if need be
        self.filter_es_indices()

        # time respond build
        stime = time.time()

        # execute search
        self.search()

        # end time
        LOGGER.debug('DTElasticGenericSearch: response time %s', (time.time()-stime))

        # for all search types, build and return response
        return JsonResponse(self.dt_output)


    def filter_es_indices(self):

        filter_jobs = [
            'j%s' % int(job_id.split('|')[-1])
            for job_id in self.request.GET.getlist('jobs[]')
            if job_id.startswith('job')
        ]
        if len(filter_jobs) > 0:
            self.es_index = filter_jobs
        else:
            self.es_index = 'j*'


    def search(self):

        '''
        Execute search
        '''

        # initiate es query
        self.query = Search(using=es_handle, index=self.es_index)

        # get total document count, pre-filtering
        self.dt_output['recordsTotal'] = self.query.count()

        # apply filtering to ES query
        self.filter()

        # # apply sorting to ES query
        self.sort()

        # # self.sort()
        self.paginate()

        # get document count, post-filtering
        self.dt_output['recordsFiltered'] = self.query.count()

        # execute and retrieve search
        self.query_results = self.query.execute()

        # loop through hits
        for hit in self.query_results.hits:

            try:

                # get combine record
                record = Record.objects.get(id=hit.db_id)

                # loop through rows, add to list while handling data types
                row_data = []
                for field in self.fields:
                    field_value = getattr(hit, field, None)

                    # handle ES lists
                    if isinstance(field_value, AttrList):
                        row_data.append(str(field_value))

                    # all else, append
                    else:
                        row_data.append(field_value)

                # add record lineage in front
                row_data = self._prepare_record_hierarchy_links(record, row_data)

                # add list to object
                self.dt_output['data'].append(row_data)

            except Exception as err:
                LOGGER.debug("error retrieving DB record based on id %s, from index %s: %s", hit.db_id, hit.meta.index, str(err))


    @staticmethod
    def _prepare_record_hierarchy_links(record, row_data):

        '''
        Method to prepare links based on the hierarchy of the Record
        '''

        urls = record.get_lineage_url_paths()

        to_append = [
            '<a href="%s">%s</a>' % (urls['organization']['path'], urls['organization']['name']),
            '<a href="%s">%s</a>' % (urls['record_group']['path'], urls['record_group']['name']),
            '<a href="%s"><span class="%s">%s</span></a>' % (urls['job']['path'], record.job.job_type_family(), urls['job']['name']),
            urls['record']['path'],
        ]

        return to_append + row_data
