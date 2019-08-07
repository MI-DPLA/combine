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

        same_id_same_doc_pipeline = [
            {
                "$group": {
                    "_id": {"record_id": "$record_id", "document": "$document"},
                    "uniqueIds": {"$addToSet": "$_id"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]
        sisd_cursor = Record.objects.aggregate(*same_id_same_doc_pipeline)
        full_duplicates = list(sisd_cursor)
        print("count of duplicates same-id same-doc: {}".format(len(full_duplicates)))

        same_id_different_doc_pipeline = [
            {
                "$group": {
                    "_id": {"record_id": "$record_id"},
                    "uniqueDocs": {"$addToSet": "$document"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {
                "$unwind": "$uniqueDocs"
            },
            {
                "$group": {
                    "_id": {"record_id": "$_id.record_id"},
                    "uniqueDocs": {"$addToSet": "$uniqueDocs"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]
        sidd_cursor = Record.objects.aggregate(*same_id_different_doc_pipeline)
        different_docs = list(sidd_cursor)
        print("count of duplicates same-id different-doc: {}".format(len(different_docs)))

        different_id_same_doc_pipeline = [
            {
                "$group": {
                    "_id": {"document": "$document"},
                    "uniqueRecordIds": {"$addToSet": "$record_id"},
                    "count": {"$sum": 1}
                },
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {
                "$unwind": "$uniqueRecordIds"
            },
            {
                "$group": {
                    "_id": {"document": "$_id.document"},
                    "uniqueRecordIds": {"$addToSet": "$uniqueRecordIds"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]
        disd_cursor = Record.objects.aggregate(*different_id_same_doc_pipeline)
        same_docs = list(disd_cursor)
        print("count of duplicates different-id same-doc: {}".format(len(same_docs)))
