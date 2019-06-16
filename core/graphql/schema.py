
# generic imports
import json
import logging

# graphene imports
import graphene
from graphene import ObjectType, String, Schema

# graphene_django
from graphene_django.types import DjangoObjectType

# graphene_mongo
from graphene_mongo import MongoengineObjectType

# core imports
from core.models import Organization, RecordGroup, Job, Record, RecordValidation

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



class RecordMappedFields(ObjectType):

    '''
    Returning the unknown fields that result from Record mapping is currently not possible:
    https://github.com/graphql/graphql-spec/issues/101

    Possible workaround would be returning string of ES document (see mapped_fields_json below)

    Another option would be query defined aliases and mapping, a la:
    https://github.com/graphql/graphql-spec/issues/101#issuecomment-170170967
        - a query could ping the ES index and retrieve all fields, looping through and aliasing
        - see mapped_field below
        - could potentially ping Job for all fields as part of the
        CombineJob.mapped_fields_analysis() method

        example query:
        query {
          all_records(first:100) {
            id
            abstract:mapped_field(field_name:"mods_abstract") {
              value
            }
            subjects:mapped_field(field_name:"mods_subject_topic") {
              value
            }
          }
        }

        This is clearly not optimal, as each mapped_field that is requested in the graphql query, for each Record,
        is a separate request to ES
    '''

    record_id = String()
    mods_subject_topic = String()



class RecordValidationType(MongoengineObjectType):

    class Meta:
        model = RecordValidation



class RecordType(MongoengineObjectType):

    class Meta:
        model = Record

    # Mapped Fields
    mapped_fields = graphene.Field(RecordMappedFields)

    def resolve_mapped_fields(instance, info, **kwargs):
        return instance.get_es_doc()

    mapped_fields_json = graphene.Field(graphene.String)

    def resolve_mapped_fields_json(instance, info, **kwargs):
        return json.dumps(instance.get_es_doc())

    mapped_field = graphene.List(graphene.String, field_name=graphene.String())
    def resolve_mapped_field(instance, info, **kwargs):
        field_value = instance.get_es_doc().get(kwargs.get('field_name'))
        if type(field_value) == list:
            return [value for value in field_value]
        else:
            return [field_value]

    # Validations
    validations = graphene.List(RecordValidationType)
    def resolve_validations(instance, info, **kwargs):
        return instance.get_validation_errors()



class JobType(DjangoObjectType):

    class Meta:
        model = Job

    all_records = graphene.List(RecordType, first=graphene.Int())
    def resolve_all_records(instance, info, **kwargs):

        first = kwargs.get('first')

        if first is not None:
            return instance.get_records()[:int(first)]
        return instance.get_records()



class RecordGroupType(DjangoObjectType):
    class Meta:
        model = RecordGroup



class OrganizationType(DjangoObjectType):
    class Meta:
        model = Organization



class Query(graphene.ObjectType):

    # Record
    all_records = graphene.List(RecordType,
                                first=graphene.Int()
                                )

    # Jobs
    job = graphene.Field(JobType,
                         id=graphene.Int()
                         )
    all_jobs = graphene.List(JobType,
                             first=graphene.Int()
                             )

    # Record Groups
    record_group = graphene.Field(RecordGroupType,
                                  id=graphene.Int(),
                                  name=graphene.String()
                                  )
    all_record_groups = graphene.List(RecordGroupType)

    # Organizations
    organization = graphene.Field(OrganizationType,
                                  id=graphene.Int(),
                                  name=graphene.String()
                                  )
    all_organizations = graphene.List(OrganizationType)


    # resolvers
    def resolve_organization(self, info, **kwargs):

        id = kwargs.get('id')
        name = kwargs.get('name')

        if id is not None:
            return Organization.objects.get(pk=id)

        if name is not None:
            return Organization.objects.get(name=name)

        return None


    def resolve_all_organizations(self, info, **kwargs):
        return Organization.objects.all()


    def resolve_record_group(self, info, **kwargs):

        id = kwargs.get('id')
        name = kwargs.get('name')

        if id is not None:
            return RecordGroup.objects.get(pk=id)

        if name is not None:
            return RecordGroup.objects.get(name=name)

        return None


    def resolve_all_record_groups(self, info, **kwargs):

        return RecordGroup.objects.select_related('organization').all()


    def resolve_job(self, info, **kwargs):

        id = kwargs.get('id')

        if id is not None:
            return Job.objects.get(pk=id)

        return None


    def resolve_all_jobs(self, info, **kwargs):

        first = kwargs.get('first')

        # handle first slice
        if first is not None:
            return Job.objects.all()[:int(first)]

        return Job.objects.select_related('record_group').all()


    def resolve_all_records(self, info, **kwargs):

        first = kwargs.get('first')

        # handle first slice
        if first is not None:
            return Record.objects.all()[:int(first)]

        return Record.objects.all()



# init schema
schema = graphene.Schema(
    query=Query,
    auto_camelcase=False
)
