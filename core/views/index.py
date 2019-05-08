from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from core import models


@login_required
def index(request):
    # get username
    username = request.user.username

    # get all organizations
    orgs = models.Organization.objects.exclude(for_analysis=True).all()

    # get record count
    record_count = models.Record.objects.all().count()

    # get published records count
    pub_records = models.PublishedRecords()
    published_record_count = pub_records.records.count()

    # get job count
    job_count = models.Job.objects.all().count()

    return render(request, 'core/index.html', {
        'username': username,
        'orgs': orgs,
        'record_count': "{:,}".format(record_count),
        'published_record_count': "{:,}".format(published_record_count),
        'job_count': "{:,}".format(job_count)
    })
