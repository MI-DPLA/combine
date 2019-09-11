from django.http import HttpResponse

from core.oai import OAIProvider


def oai(request, subset=None):
    """
    Parse GET parameters, send to OAIProvider instance from oai.py
    Return XML results
    """

    # get OAIProvider instance
    provider = OAIProvider(request.GET, subset=subset)

    # return XML
    return HttpResponse(provider.generate_response(), content_type='text/xml')
