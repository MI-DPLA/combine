from django.contrib.auth.decorators import login_required
from django.core.exceptions import ObjectDoesNotExist
from django.forms.models import model_to_dict
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.urls import reverse

from core.models import OAIEndpoint
from core.forms import OAIEndpointForm


@login_required
def oai_endpoint_payload(request, oai_endpoint_id):
    """
    Return JSON of saved OAI endpoint information
    """

    # retrieve OAIEndpoint
    oai_endpoint = OAIEndpoint.objects.get(pk=oai_endpoint_id)

    # pop state
    oai_endpoint.__dict__.pop('_state')

    # return as json
    return JsonResponse(oai_endpoint.__dict__)


@login_required
def create_oai_endpoint(request):
    if request.method == "POST":
        form = OAIEndpointForm(request.POST)
        if form.is_valid():
            new_endpoint = OAIEndpoint(**form.cleaned_data)
            new_endpoint.save()
        return redirect(reverse('configuration'))
    form = OAIEndpointForm
    return render(request, 'core/new_oai_endpoint.html', {
        'form': form
    })


@login_required
def edit_oai_endpoint(request, oai_endpoint_id):
    oai_endpoint = OAIEndpoint.objects.get(pk=int(oai_endpoint_id))
    if request.method == 'POST':
        form = OAIEndpointForm(request.POST)
        if form.is_valid():
            for key in form.cleaned_data:
                setattr(oai_endpoint, key, form.cleaned_data[key])
            oai_endpoint.save()
        else:
            print(form.errors.as_json())
        return redirect(reverse('configuration'))
    form = OAIEndpointForm(model_to_dict(oai_endpoint))
    return render(request, 'core/edit_oai_endpoint.html', {
        'oai_endpoint': oai_endpoint,
        'form': form,
    })


@login_required
def delete_oai_endpoint(request, oai_endpoint_id):
    try:
        endpoint = OAIEndpoint.objects.get(pk=int(oai_endpoint_id))
        endpoint.delete()
    except ObjectDoesNotExist:
        pass
    return redirect(reverse('configuration'))
