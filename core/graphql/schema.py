import graphene

from graphene_django.types import DjangoObjectType

from core.models import Organization, RecordGroup, Job



class OrganizationType(DjangoObjectType):
    class Meta:
        model = Organization


class RecordGroupType(DjangoObjectType):
    class Meta:
        model = RecordGroup


class JobType(DjangoObjectType):
    class Meta:
        model = Job


class Query(graphene.ObjectType):

    # Organization
    organization = graphene.Field(OrganizationType,
        id=graphene.Int(),
        name=graphene.String()
    )
    all_organizations= graphene.List(OrganizationType)

    # Record Group
    all_record_groups = graphene.List(RecordGroupType)

    # Job
    all_jobs = graphene.List(JobType)

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
        return Job.objects.select_related('record_group').all()




schema = graphene.Schema(query=Query)