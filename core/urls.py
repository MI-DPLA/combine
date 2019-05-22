from django.conf.urls import url
from django.contrib import admin
from django.contrib.auth import views as auth_views

from . import views
from .models import DTElasticFieldSearch, DTElasticGenericSearch

urlpatterns = [

    # System
    url(r'^system$', views.system, name='system'),

    # User Livy sessions
    url(r'^system/livy_sessions/start$',
        views.livy_session_start, name='livy_session_start'),
    url(r'^system/livy_sessions/(?P<session_id>[0-9]+)/stop$',
        views.livy_session_stop, name='livy_session_stop'),
    url(r'^system/bg_status$', views.system_bg_status, name='system_bg_status'),

    # Organizations
    url(r'^organization/all$', views.organizations, name='organizations'),
    url(r'^organization/(?P<org_id>[0-9]+)$',
        views.organization, name='organization'),
    url(r'^organization/(?P<org_id>[0-9]+)/delete$',
        views.organization_delete, name='organization_delete'),

    # Record Groups
    url(r'^record_group/(?P<record_group_id>[0-9]+)$', views.record_group_id_redirect, name='record_group_id_redirect'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/new$', views.record_group_new, name='record_group_new'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)$', views.record_group,
        name='record_group'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/delete$', views.record_group_delete,
        name='record_group_delete'),

    # Jobs
    url(r'^job/(?P<job_id>[0-9]+)$', views.job_id_redirect, name='job_id_redirect'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)$',
        views.job_details, name='job_details'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/details$',
        views.job_details, name='job_details'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/delete$',
        views.job_delete, name='job_delete'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/publish$',
        views.job_publish, name='job_publish'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/unpublish$',
        views.job_unpublish, name='job_unpublish'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/errors$',
        views.job_errors, name='job_errors'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/update_note$',
        views.job_update_note, name='job_update_note'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/update_name$',
        views.job_update_name, name='job_update_name'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/harvest/oai/new$',
        views.job_harvest_oai, name='job_harvest_oai'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/harvest/static/xml/new$',
        views.job_harvest_static_xml, name='job_harvest_static_xml'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/harvest/static/tabular/new$',
        views.job_harvest_tabular_data, name='job_harvest_tabular_data'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/transform/new$',
        views.job_transform, name='job_transform'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/merge/new$', views.job_merge,
        name='job_merge'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/reports/create_validation_report$',
        views.job_reports_create_validation, name='job_reports_create_validation'),
    url(
        r'^organization/(?P<org_id>([0-9]|(DYNAMIC_ORG_ID))+)/record_group/(?P<record_group_id>([0-9]|(DYNAMIC_RG_ID))+)/job/(?P<job_id>([0-9]|(DYNAMIC_ID))+)/job_lineage_json$',
        views.job_lineage_json, name='job_lineage_json'),
    url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/update$',
        views.job_update, name='job_update'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_parameters$',
        views.job_parameters, name='job_parameters'),

    # Job Record Validation Scenarios
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_validation_scenario_failures/(?P<job_validation_id>[0-9]+)$',
        views.job_validation_scenario_failures, name='job_validation_scenario_failures'),

    # Record Group Job Analysis
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/analysis/indexing_failures$',
        views.job_indexing_failures, name='job_indexing_failures'),

    # ElasticSearch Analysis
    url(r'^analysis/es/index/(?P<es_index>.+)/field_analysis$', views.field_analysis, name='field_analysis'),
    url(r'^analysis/es/index/(?P<es_index>.+)/field_analysis/docs/(?P<filter_type>.+)$', views.field_analysis_docs,
        name='field_analysis_docs'),

    # Jobs General
    url(r'^jobs/all$', views.all_jobs, name='all_jobs'),
    url(r'^jobs/move_jobs$', views.move_jobs, name='move_jobs'),
    url(r'^jobs/stop_jobs$', views.stop_jobs, name='stop_jobs'),
    url(r'^jobs/delete_jobs$', views.delete_jobs, name='delete_jobs'),
    url(r'^jobs/rerun_jobs$', views.rerun_jobs, name='rerun_jobs'),
    url(r'^jobs/clone_jobs$', views.clone_jobs, name='clone_jobs'),

    # Records
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9a-z]+)$',
        views.record, name='record'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9a-z]+)/document$',
        views.record_document, name='record_document'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9a-z]+)/indexed_document$',
        views.record_indexed_document, name='record_indexed_document'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9a-z]+)/error$',
        views.record_error, name='record_error'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9a-z]+)/validation_scenario/(?P<job_validation_id>[0-9]+)$',
        views.record_validation_scenario, name='record_validation_scenario'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9a-z]+)/diff/combined$',
        views.record_combined_diff_html, name='record_combined_diff_html'),
    url(
        r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9a-z]+)/diff/side_by_side$',
        views.record_side_by_side_diff_html, name='record_side_by_side_diff_html'),

    # Configuration
    url(r'^configuration$', views.configuration, name='configuration'),
    url(r'^configuration/dpla_bulk_data/download$',
        views.dpla_bulk_data_download, name='dpla_bulk_data_download'),

    # OAI Endpoints
    url(r'^configuration/oai_endpoint/(?P<oai_endpoint_id>[0-9]+)/payload$',
        views.oai_endpoint_payload, name='oai_endpoint_payload'),

    # Validation Scenarios
    url(r'^configuration/validation/(?P<vs_id>[0-9]+)/payload$',
        views.validation_scenario_payload, name='validation_scenario_payload'),
    url(r'^configuration/validation/create$',
        views.create_validation_scenario, name='create_validation_scenario'),
    url(r'^configuration/validation/(?P<vs_id>[0-9]+)$',
        views.validation_scenario, name='validation_scenario'),
    url(r'^configuration/validation/(?P<vs_id>[0-9]+)/delete$',
        views.delete_validation_scenario, name='delete_validation_scenario'),
    url(r'^configuration/validation/test$',
        views.test_validation_scenario, name='test_validation_scenario'),

    # Transformation Scenarios
    url(r'^configuration/transformation/(?P<trans_id>[0-9]+)/payload$',
        views.transformation_scenario_payload, name='transformation_scenario_payload'),
    url(r'^configuration/transformation/create$',
        views.create_transformation_scenario, name='create_transformation_scenario'),
    url(r'^configuration/transformation/(?P<ts_id>[0-9]+)$',
        views.transformation_scenario, name='transformation_scenario'),
    url(r'^configuration/transformation/(?P<ts_id>[0-9]+)/delete',
        views.delete_transformation_scenario, name='delete_transformation_scenario'),
    url(r'^configuration/transformation/test$',
        views.test_transformation_scenario, name='test_transformation_scenario'),

    # Field Mapper Configurations
    url(r'^configuration/field_mapper/(?P<fm_id>[0-9]+)/payload$',
        views.field_mapper_payload, name='field_mapper_payload'),
    url(r'^configuration/field_mapper/update$',
        views.field_mapper_update, name='field_mapper_update'),
    url(r'^configuration/test_field_mapper$',
        views.test_field_mapper, name='test_field_mapper'),

    # Record Identifier Transformation Scenarios
    url(r'^configuration/rits/(?P<rits_id>[0-9]+)/payload$',
        views.rits_payload, name='rits_payload'),
    url(r'^configuration/test_rits$', views.test_rits, name='test_rits'),

    # Publish
    url(r'^published$', views.published, name='published'),
    url(r'^published/published_dt_json$', views.DTPublishedJson.as_view(), name='published_dt_json'),
    url(r'^published/published_dt_json/subset/(?P<subset>.+)$', views.DTPublishedJson.as_view(),
        name='published_dt_json'),
    url(r'^published/subsets/create$', views.published_subset_create, name='published_subset_create'),
    url(r'^published/subsets/edit/(?P<subset>.+)$', views.published_subset_edit, name='published_subset_edit'),
    url(r'^published/subsets/delete/(?P<subset>.+)$', views.published_subset_delete, name='published_subset_delete'),
    url(r'^published/subset/(?P<subset>.+)$', views.published, name='published_subset'),

    # Export
    url(r'^export/mapped_fields/(?P<export_source>[a-zA-Z]+)/(?P<job_id>[0-9]+)$', views.export_mapped_fields,
        name='export_mapped_fields'),
    url(r'^export/mapped_fields/(?P<export_source>[a-zA-Z]+)$', views.export_mapped_fields,
        name='export_mapped_fields'),
    url(r'^export/mapped_fields/(?P<export_source>[a-zA-Z]+)/subset/(?P<subset>.+)$', views.export_mapped_fields,
        name='export_mapped_fields'),
    url(r'^export/documents/(?P<export_source>[a-zA-Z]+)/(?P<job_id>[0-9]+)$', views.export_documents,
        name='export_documents'),
    url(r'^export/documents/(?P<export_source>[a-zA-Z]+)$', views.export_documents, name='export_documents'),
    url(r'^export/documents/(?P<export_source>[a-zA-Z]+)/subset/(?P<subset>.+)$', views.export_documents,
        name='export_documents'),
    url(r'^export/tabular_data/(?P<export_source>[a-zA-Z]+)/(?P<job_id>[0-9]+)$', views.export_tabular_data,
        name='export_tabular_data'),
    url(r'^export/tabular_data/(?P<export_source>[a-zA-Z]+)$', views.export_tabular_data, name='export_tabular_data'),
    url(r'^export/tabular_data/(?P<export_source>[a-zA-Z]+)/subset/(?P<subset>.+)$', views.export_tabular_data,
        name='export_tabular_data'),

    # OAI
    url(r'^oai$', views.oai, name='oai'),
    url(r'^oai/subset/(?P<subset>.+)$', views.oai, name='oai_subset'),

    # Global Search
    url(r'^search$', views.search, name='search'),

    # Datatables Endpoints
    url(r'^datatables/all_records/records_dt_json$', views.DTRecordsJson.as_view(), name='all_records_dt_json'),
    url(
        r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/records_dt_json$',
        views.DTRecordsJson.as_view(), name='records_dt_json'),
    url(
        r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/records_dt_json/(?P<success_filter>[0-1]+)$',
        views.DTRecordsJson.as_view(), name='records_dt_json'),
    url(
        r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/indexing_failures_dt_json$',
        views.DTIndexingFailuresJson.as_view(), name='indexing_failures_dt_json'),
    url(
        r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_validation_scenario_failures_json/(?P<job_validation_id>[0-9]+)$',
        views.DTJobValidationScenarioFailuresJson.as_view(), name='job_validation_scenario_failures_json'),
    url(
        r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/dpla_bulk_data/(?P<match_type>.*)$',
        views.DTDPLABulkDataMatches.as_view(), name='dpla_bulk_data_matches'),
    url(
        r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record_diffs$',
        views.JobRecordDiffs.as_view(), name='job_record_diffs'),
    url(r'^datatables/background_tasks$', views.CombineBackgroundTasksDT.as_view(), name='bg_tasks_dt'),
    url(r'^datatables/es/index/(?P<es_index>.+)/(?P<search_type>.+)/records_es_field_dt_json$',
        DTElasticFieldSearch.as_view(), name='records_es_field_dt_json'),
    url(r'^datatables/es/search$', DTElasticGenericSearch.as_view(), name='records_es_generic_dt_json'),

    # Analysis
    url(r'^analysis$', views.analysis, name='analysis'),
    url(r'^analysis/new$', views.job_analysis, name='job_analysis'),

    # Background Tasks
    url(r'^background_tasks$', views.bg_tasks, name='bg_tasks'),
    url(r'^background_tasks/process/action/(?P<proc_action>[0-9a-z]+)$', views.bgtasks_proc_action,
        name='bgtasks_proc_action'),
    url(r'^background_tasks/process/logs/err$', views.bgtasks_proc_stderr_log, name='bgtasks_proc_stderr_log'),
    url(r'^background_tasks/delete_all$', views.bg_tasks_delete_all, name='bg_tasks_delete_all'),
    url(r'^background_tasks/task/(?P<task_id>[0-9]+)$', views.bg_task, name='bg_task'),
    url(r'^background_tasks/task/(?P<task_id>[0-9]+)/delete$', views.bg_task_delete, name='bg_task_delete'),
    url(r'^background_tasks/task/(?P<task_id>[0-9]+)/cancel$', views.bg_task_cancel, name='bg_task_cancel'),

    # Document Download
    url(r'^document_download$', views.document_download,
        name='document_download'),

    # Global Messages (GMs)
    url(r'^gm/delete$', views.gm_delete, name='gm_delete'),

    # StateIO
    url(r'^stateio$', views.stateio, name='stateio'),
    url(r'^stateio/state/(?P<state_id>[0-9a-z]+)$', views.stateio_state, name='stateio_state'),
    url(r'^stateio/state/(?P<state_id>[0-9a-z]+)/manifest/(?P<manifest_type>.+)$', views.stateio_state_manifest,
        name='stateio_state_manifest'),
    url(r'^stateio/state/(?P<state_id>[0-9a-z]+)/delete$', views.stateio_state_delete, name='stateio_state_delete'),
    url(r'^stateio/state/(?P<state_id>[0-9a-z]+)/download$', views.stateio_state_download,
        name='stateio_state_download'),
    url(r'^stateio/state/(?P<state_id>[0-9a-z]+)/stop$', views.stateio_state_stop, name='stateio_state_stop'),
    url(r'^stateio/export$', views.stateio_export, name='stateio_export'),
    url(r'^stateio/import$', views.stateio_import, name='stateio_import'),

    # General
    url(r'^login$', auth_views.login, name='login'),
    url(r'^logout$', auth_views.logout, name='logout'),
    url(r'^', views.index, name='combine_home'),
]
