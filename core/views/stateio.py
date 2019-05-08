import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import FileResponse, JsonResponse
from django.shortcuts import render, redirect

from core import models

from .view_helpers import breadcrumb_parser

logger = logging.getLogger(__name__)


@login_required
def stateio(request):
    """
        Root view for StateIO
        """

    # retrieve exports and imports
    stateio_exports = models.StateIO.objects.filter(stateio_type='export')
    stateio_imports = models.StateIO.objects.filter(stateio_type='import')

    # return
    return render(request, 'core/stateio.html', {
        'stateio_exports': stateio_exports,
        'stateio_imports': stateio_imports,
        'breadcrumbs': breadcrumb_parser(request)
    })


@login_required
def stateio_state(request, state_id):
    """
        Single state view
        """

    # retrieve state
    state = models.StateIO.objects.get(id=state_id)

    # handle export state
    if state.stateio_type == 'export':

        # retrieve imports, if any, that share this export_id
        associated_imports = models.StateIO.objects.filter(
            export_manifest__export_id=state.export_id,
            stateio_type='import')

        # generate io results json
        if 'exports' in state.export_manifest:
            io_results_json = _generate_io_results_json(
                state.export_manifest['exports'])
        else:
            io_results_json = False

        # return
        return render(request, 'core/stateio_state_export.html', {
            'state': state,
            'associated_imports': associated_imports,
            'io_results_json': json.dumps(io_results_json, sort_keys=True),
            'breadcrumbs': breadcrumb_parser(request)
        })

    # handle import state
    if state.stateio_type == 'import':

        if state.status == 'finished':
            # retrieve export used for import, if exists in same instance of Combine
            associated_export_q = models.StateIO.objects.filter(
                export_id=state.export_manifest['export_id'],
                stateio_type='export')
            if associated_export_q.count() == 1:
                associated_export = associated_export_q.first()
            else:
                associated_export = None
        else:
            associated_export = None

        # generate io results json
        if 'imports' in state.import_manifest:
            io_results_json = _generate_io_results_json(
                state.import_manifest['imports'])
        else:
            io_results_json = False

        # return
        return render(request, 'core/stateio_state_import.html', {
            'state': state,
            'associated_export': associated_export,
            'io_results_json': json.dumps(io_results_json, sort_keys=True),
            'breadcrumbs': breadcrumb_parser(request)
        })


def _generate_io_results_json(io_results):
    """
        Function to generate jstree ready JSON when provided
        with an export or import manifest

        Args:
                io_results (dict): Dictionary of IO results, either export or import
        """

    # results_flag
    io_results_flag = False

    # model translation for serializable strings for models
    model_type_hash = {
        'jobs_hierarchy': {
            'jobs': 'Jobs',
            'record_groups': 'Record Groups',
            'orgs': 'Organizations',
        },
        'config_scenarios': {
            'dbdd': 'DPLA Bulk Data Downloads',
            'oai_endpoints': 'OAI Endpoints',
            'rits': 'Record Identifier Transformation Scenarios',
            'transformations': 'Transformation Scenarios',
            'validations': 'Validation Scenarios'
        }
    }

    # init dictionary
    io_results_json = []

    # loop through jobs and configs
    for obj_type, obj_subsets in model_type_hash.items():

        logger.debug('building %s' % obj_type)

        # obj_type_flag
        obj_type_flag = False

        # init obj type level dict
        obj_type_hash = {
            'jobs_hierarchy': {
                'name': 'Organizations, Record Groups, and Jobs',
                'icon': 'la la-sitemap'
            },
            'config_scenarios': {
                'name': 'Configuration Scenarios',
                'icon': 'la la-gears'
            }
        }

        obj_type_dict = {
            # 'id':obj_type,
            'text': obj_type_hash[obj_type]['name'],
            'state': {'opened': True},
            'children': [],
            'icon': obj_type_hash[obj_type]['icon']
        }

        # loop through model types and build dictionary
        for model_key, model_name in obj_subsets.items():

            # init model level dict
            model_type_dict = {
                # 'id':model_key,
                'text': model_name,
                'state': {'opened': True},
                'children': [],
                'icon': 'la la-folder-open'
            }

            # loop through io results
            for io_obj in io_results[model_key]:
                model_type_dict['children'].append(
                    {
                        # 'id':io_obj['id'],
                        'text': io_obj['name'],
                        'state': {'opened': True},
                        'icon': 'la la-file',
                        'children': [],
                        'li_attr': {
                            'io_obj': True
                        }
                    }
                )

            # append model type dict to
            if len(io_results[model_key]) > 0:
                io_results_flag = True
                obj_type_flag = True
                obj_type_dict['children'].append(model_type_dict)

        # append obj type dict if contains children
        if obj_type_flag:
            io_results_json.append(obj_type_dict)

    # if results found for any type, return and imply True
    if io_results_flag:
        return io_results_json
    else:
        return False


@login_required
def stateio_state_manifest(request, state_id, manifest_type):
    """
        View export/import manifest from state
        """

    # retrieve state
    state = models.StateIO.objects.get(id=state_id)

    # return
    return JsonResponse(getattr(state, manifest_type, None))


@login_required
def stateio_state_delete(request, state_id):
    """
        Delete single state view
        """

    # retrieve state
    state = models.StateIO.objects.get(id=state_id)

    # delete and redirect
    state.delete()

    # return
    return redirect('stateio')


@login_required
def stateio_state_download(request, state_id):
    """
        Download export state
        """

    # retrieve state
    state = models.StateIO.objects.get(id=state_id)

    # set filepath as download location on disk
    filepath = state.export_path

    # set filename
    filename = filepath.split('/')[-1]

    # generate response
    response = FileResponse(open(filepath, 'rb'))
    response['Content-Disposition'] = 'attachment; filename="%s"' % filename
    return response


@login_required
def stateio_state_stop(request, state_id):
    """
        Attempt to stop state when running as bg task
        """

    # retrieve state
    state = models.StateIO.objects.get(id=state_id)

    # issue cancel
    if state.bg_task:
        state.bg_task.cancel()

    # update status
    state.status = 'stopped'
    state.finished = True
    state.save()

    # return
    return redirect('stateio')


@login_required
def stateio_export(request):
    """
        Export state
        """

    if request.method == 'GET':
        # generate hierarchy_dict
        job_hierarchy = _stateio_prepare_job_hierarchy()

        # generate config scenarios
        config_scenarios = _stateio_prepare_config_scenarios()

        # return
        return render(request, 'core/stateio_export.html', {
            'job_hierarchy_json': json.dumps(job_hierarchy),
            'config_scenarios_json': json.dumps(config_scenarios),
            'breadcrumbs': breadcrumb_parser(request)
        })

    if request.method == 'POST':

        # capture optional export name
        export_name = request.POST.get('export_name', None)
        if export_name == '':
            export_name = None
        logger.debug('initing export: %s' % export_name)

        # capture and parse jobs_hierarchy_ids
        jobs_hierarchy_ids = request.POST.getlist('jobs_hierarchy_ids[]')
        jobs = [int(obj.split('|')[-1])
                for obj in jobs_hierarchy_ids if obj.startswith('job')]
        record_groups = [int(obj.split('|')[-1])
                         for obj in jobs_hierarchy_ids if obj.startswith('record_group')]
        orgs = [int(obj.split('|')[-1])
                for obj in jobs_hierarchy_ids if obj.startswith('org')]

        # capture and parse config_scenarios_ids
        config_scenarios_ids = [config_id for config_id in request.POST.getlist('config_scenarios_ids[]') if
                                '|' in config_id]

        # init export as bg task
        ct = models.StateIOClient.export_state_bg_task(
            export_name=export_name,
            jobs=jobs,
            record_groups=record_groups,
            orgs=orgs,
            config_scenarios=config_scenarios_ids  # preserve prefixes through serialization
        )

        # retrieve StateIO instance, use metadata for msg
        stateio = models.StateIO.objects.get(id=ct.task_params['stateio_id'])

        # set gms
        gmc = models.GlobalMessageClient(request.session)
        gmc.add_gm({
            'html': '<p><strong>Exporting State:</strong><br>%s</p><p>Refresh this page for updates: <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button></p>' % (
                stateio.name),
            'class': 'success'
        })

        # return
        return JsonResponse({'msg': 'success'})


def _stateio_prepare_job_hierarchy(
        include_record_groups=True,
        include_jobs=True):
    # generate JSON that will be used by jstree to create Org, Record Group, Jobs selections
    """
        Target structure:
        // Expected format of the node (there are no required fields)
        {
          id          : "string" // will be autogenerated if omitted
          text        : "string" // node text
          icon        : "string" // string for custom
          state       : {
            opened    : boolean  // is the node open
            disabled  : boolean  // is the node disabled
            selected  : boolean  // is the node selected
          },
          children    : []  // array of strings or objects
          li_attr     : {}  // attributes for the generated LI node
          a_attr      : {}  // attributes for the generated A node
        }
        """
    # init dictionary with root node
    hierarchy_dict = {
        'id': 'root_jobs',
        'text': 'Organizations, Record Groups, and Jobs',
        'state': {'opened': True},
        'children': [],
        'icon': 'la la-sitemap'
    }

    # add Organizations --> Record Group --> Jobs
    for org in models.Organization.objects.filter(for_analysis=False):

        # init org dict
        org_dict = {
            'id': 'org|%s' % org.id,
            'text': org.name,
            'state': {'opened': False},
            'children': [],
            'icon': 'la la-folder-open'
        }

        if include_record_groups:
            # loop through child Record Groups and add
            for rg in org.recordgroup_set.all():

                # init rg dict
                rg_dict = {
                    'id': 'record_group|%s' % rg.id,
                    'text': rg.name,
                    'state': {'opened': False},
                    'children': [],
                    'icon': 'la la-folder-open'
                }

                if include_jobs:
                    # loop through Jobs and add
                    for job in rg.job_set.all():
                        # init job dict
                        job_dict = {
                            'id': 'job|%s' % job.id,
                            'text': job.name,
                            'state': {'opened': False},
                            'icon': 'la la-file'
                        }

                        # append to rg
                        rg_dict['children'].append(job_dict)

                # add back to org
                org_dict['children'].append(rg_dict)

        # add org to root hierarchy
        hierarchy_dict['children'].append(org_dict)

    # return embedded in list
    return [hierarchy_dict]


def _stateio_prepare_config_scenarios():
    # generate JSON that will be used by jstree to create configuration scenarios
    """
        Target structure:
        // Expected format of the node (there are no required fields)
        {
          id          : "string" // will be autogenerated if omitted
          text        : "string" // node text
          icon        : "string" // string for custom
          state       : {
            opened    : boolean  // is the node open
            disabled  : boolean  // is the node disabled
            selected  : boolean  // is the node selected
          },
          children    : []  // array of strings or objects
          li_attr     : {}  // attributes for the generated LI node
          a_attr      : {}  // attributes for the generated A node
        }
        """
    # init dictionary with root node
    config_scenarios_dict = {
        'id': 'root_config',
        'text': 'Configurations and Scenarios',
        'state': {'opened': True},
        'children': [],
        'icon': 'la la-gears'
    }

    def _add_config_scenarios(config_scenarios_dict, model, id_str, text_str, id_prefix):

        # set base dict
        model_dict = {
            'id': id_str,
            'text': text_str,
            'state': {'opened': False},
            'children': [],
            'icon': 'la la-folder-open'
        }

        # loop through instances
        for obj in model.objects.all():
            model_dict['children'].append({
                'id': '%s|%s' % (id_prefix, obj.id),
                'text': obj.name,
                'state': {'opened': False},
                'children': [],
                'icon': 'la la-file'
            })

        # append to config_scenarios_dict
        config_scenarios_dict['children'].append(model_dict)

    # loop through models and append to config scenarios dict
    for model_tup in [
        (config_scenarios_dict, models.ValidationScenario, 'validation_scenarios', 'Validation Scenarios',
         'validations'),
        (
            config_scenarios_dict, models.Transformation, 'transformations', 'Transformation Scenarios',
            'transformations'),
        (config_scenarios_dict, models.OAIEndpoint,
         'oai_endpoints', 'OAI Endpoints', 'oai_endpoints'),
        (config_scenarios_dict, models.RecordIdentifierTransformationScenario, 'rits',
         'Record Identifier Transformation Scenarios', 'rits'),
        (config_scenarios_dict, models.FieldMapper, 'field_mapper_configs', 'Field Mapper Configurations',
         'field_mapper_configs'),
        (config_scenarios_dict, models.DPLABulkDataDownload,
         'dbdds', 'DPLA Bulk Data Downloads', 'dbdd')
    ]:
        # add to config_scenarios_dict
        _add_config_scenarios(*model_tup)

    # return embedded in list
    return [config_scenarios_dict]


@login_required
def stateio_import(request):
    """
        Import state
        """

    if request.method == 'GET':

        # return
        return render(request, 'core/stateio_import.html', {
            'breadcrumbs': breadcrumb_parser(request)
        })

    elif request.method == 'POST':

        # capture optional export name
        import_name = request.POST.get('import_name', None)
        if import_name == '':
            import_name = None
        logger.debug('initing import: %s' % import_name)

        # handle filesystem location
        if request.POST.get('filesystem_location', None) not in ['', None]:
            export_path = request.POST.get('filesystem_location').strip()
            logger.debug(
                'importing state based on filesystem location: %s' % export_path)

        # handle URL
        elif request.POST.get('url_location', None) not in ['', None]:
            export_path = request.POST.get('url_location').strip()
            logger.debug(
                'importing state based on remote location: %s' % export_path)

        # handle file upload
        elif type(request.FILES.get('export_upload_payload', None)) is not None:

            logger.debug('handling file upload')

            # save file to disk
            payload = request.FILES.get('export_upload_payload', None)
            new_file = '/tmp/%s' % payload.name
            with open(new_file, 'wb') as f:
                f.write(payload.read())
                payload.close()

            # set export_path
            export_path = new_file
            logger.debug('saved uploaded state to %s' % export_path)

        # init export as bg task
        ct = models.StateIOClient.import_state_bg_task(
            import_name=import_name,
            export_path=export_path
        )

        # retrieve StateIO instance, use metadata for msg
        stateio = models.StateIO.objects.get(id=ct.task_params['stateio_id'])

        # set gms
        gmc = models.GlobalMessageClient(request.session)
        gmc.add_gm({
            'html': '<p><strong>Importing State:</strong><br>%s</p><p>Refresh this page for updates: <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button></p>' % (
                stateio.name),
            'class': 'success'
        })

        return redirect('stateio')
