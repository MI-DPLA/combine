# graphene imports
import graphene
from graphene import ObjectType, String, Schema

# graphene_django
from graphene_django.types import DjangoObjectType

# graphene_mongo
from graphene_mongo import MongoengineObjectType

# core imports
from core.models import Organization, RecordGroup, Job, Record


class OrganizationType(DjangoObjectType):
    class Meta:
        model = Organization


class RecordGroupType(DjangoObjectType):
    class Meta:
        model = RecordGroup


class JobType(DjangoObjectType):
    class Meta:
        model = Job


class RecordType(MongoengineObjectType):
    class Meta:
        model = Record


class Query(graphene.ObjectType):

    # Organization
    organization = graphene.Field(OrganizationType,
                                  id=graphene.Int(),
                                  name=graphene.String()
                                  )
    all_organizations = graphene.List(OrganizationType)

    # Record Group
    all_record_groups = graphene.List(RecordGroupType)

    # Job
    all_jobs = graphene.List(JobType,
                             first=graphene.Int()
                             )

    # Record
    all_records = graphene.List(RecordType,
                                first=graphene.Int()
                                )

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

    def resolve_all_record_groups(self, info, **kwargs):
        return RecordGroup.objects.select_related('organization').all()

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


    ####################################################################################################
    # ElasticSearch exp.
    ####################################################################################################

    class RecordMappedFields(ObjectType):
        record_id = String()
        mods_extension_bibNo = String()

    # # Record Mapped Fields
    # all_record_mapped_fields = graphene.List(RecordMappedFields)
    #
    # def resolve_all_record_mapped_fields(self, info, **kwargs):
    #
    #     return [ {'record_id':'abc%s' % x} for x in range(0,20) ]

    record_mapped_fields= graphene.Field(RecordMappedFields,
                                  id=graphene.String(),
                                  )

    def resolve_record_mapped_fields(self, info, **kwargs):

        id = kwargs.get('id')

        # get Record
        record = Record.objects.get(id=id)

        # return Record' mapped fields as ES doc
        return record.get_es_doc()

    ####################################################################################################


# return schema
schema = graphene.Schema(
    query=Query,
    auto_camelcase=False
)
