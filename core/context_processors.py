# general
from core.celery import celery_app
from django.conf import settings
from django.db.models.query import QuerySet
import subprocess

# combine
from core.models import LivySession, SupervisorRPCClient, CombineBackgroundTask


def combine_settings(request):
    '''
    Make some settings variables available to all templates
    '''

    # prepare combine settings
    combine_settings_keys = [
        'APP_HOST',
        'DPLA_API_KEY',
        'OAI_RESPONSE_SIZE',
        'COMBINE_OAI_IDENTIFIER',
        'COMBINE_DEPLOYMENT',
        'COMBINE_VERSION'
    ]
    combine_settings_dict = {k: getattr(
        settings, k, None) for k in combine_settings_keys}

    # return
    return combine_settings_dict


def livy_session(request):
    '''
    Make Livy session information available to all views
    '''

    # get active livy session
    lv = LivySession.get_active_session()
    if lv:
        if type(lv) == LivySession:
            # refresh single session
            lv.refresh_from_livy()
        elif type(lv) == QuerySet:
            # multiple Combine LivySession founds, loop through
            for s in lv:
                s.refresh_from_livy()
        else:
            pass

    return {
        'LIVY_SESSION': lv
    }


def combine_git_info(request):
    '''
    Return state of HEAD for Combine git repo
    '''

    # one liner for branch or tag
    git_head = subprocess.Popen(
        'head_name="$(git symbolic-ref HEAD 2>/dev/null)" || head_name="$(git describe --tags)"; echo $head_name',
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().decode('utf-8').rstrip("\n")
    if "/" in git_head:
        git_head = git_head.split('/')[-1]

    # return
    return {'COMBINE_GIT_BRANCH': git_head}
