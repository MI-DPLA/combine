from django.conf import settings
from elasticsearch import Elasticsearch

es_handle = Elasticsearch(hosts=[settings.ES_HOST])
