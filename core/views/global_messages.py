from django.contrib.auth.decorators import login_required
from django.http import JsonResponse

from core import models


@login_required
def gm_delete(request):
    if request.method == 'POST':
        # get gm_id
        gm_id = request.POST.get('gm_id')

        # init GlobalMessageClient
        gmc = models.GlobalMessageClient(request.session)

        # delete by id
        results = gmc.delete_gm(gm_id)

        # redirect
        return JsonResponse({
            'gm_id': gm_id,
            'num_removed': results
        })
