from django.conf import settings

# import mongoengine and connect
import mongoengine

# import pymongo and establish client
import pymongo

mongoengine.connect('combine', host=settings.MONGO_HOST, port=27017)
mc_handle = pymongo.MongoClient(host=settings.MONGO_HOST, port=27017)
