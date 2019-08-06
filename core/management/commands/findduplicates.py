import logging
from django.core.management.base import BaseCommand
from core.models import Record

LOGGER = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Generate a report of duplicate records'

    def handle(self, *args, **options):
        total_count = Record.objects.count()
        print("total record count: {}".format(total_count))
        distinct_id_count = len(Record.objects.distinct('record_id'))
        print("distinct record id count: {}".format(distinct_id_count))
        pipeline = [
            {"$group": {"_id": "$record_id", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}},
            {"$project": {"name": "$_id", "count": 1, "_id": 0}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        cursor = Record.objects.aggregate(*pipeline)
        print(list(cursor))
        pass