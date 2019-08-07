import json
from pprint import pprint
import requests

from django.core.management.base import BaseCommand
from django.conf import settings

class Command(BaseCommand):
    help = "Report status for subsystems and services"

    def handle(self, *args, **options):
        print('ElasticSearch host: {}'.format(settings.ES_HOST))
        es_status = requests.get('http://{}:9200/_cluster/health'.format(settings.ES_HOST))
        es_json = json.loads(es_status.content.decode('utf-8'))
        print('ElasticSearch status:')
        pprint(es_json)
