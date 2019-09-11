from django.conf import settings

# bson and ObjectId
# pylint: disable=unused-import
from bson import ObjectId
# pylint: enable=unused-import

# import mongoengine and connect
import mongoengine

# import pymongo and establish client
import pymongo

mongoengine.connect('combine', host=settings.MONGO_HOST, port=27017)
mc_handle = pymongo.MongoClient(host=settings.MONGO_HOST, port=27017)
