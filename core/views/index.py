from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from core.models import Organization, Record, PublishedRecords, Job


@login_required
def index(request):
    # get username
    username = request.user.username

    # get all organizations
    orgs = Organization.objects.exclude(for_analysis=True).all()

    # get record count
    record_count = Record.objects.all().count()

    # get published records count
    pub_records = PublishedRecords()
    published_record_count = pub_records.records.count()

    # get job count
    job_count = Job.objects.all().count()

    return render(request, 'core/index.html', {
        'username': username,
        'orgs': orgs,
        'record_count': "{:,}".format(record_count),
        'published_record_count': "{:,}".format(published_record_count),
        'job_count': "{:,}".format(job_count)
    })
