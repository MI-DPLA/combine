from django.conf.urls import url
from django.contrib import admin
from django.contrib.auth import views as auth_views

from . import views
from .models import DTElasticSearch

urlpatterns = [

	# User Livy sessions
	url(r'^livy_sessions$', views.livy_sessions, name='livy_sessions'),
	url(r'^livy_sessions/stop$', views.livy_session_start, name='livy_session_start'),
	url(r'^livy_sessions/(?P<session_id>[0-9]+)/stop$', views.livy_session_stop, name='livy_session_stop'),

	# Organizations
	url(r'^organization/all$', views.organizations, name='organizations'),
	url(r'^organization/(?P<org_id>[0-9]+)$', views.organization, name='organization'),
	url(r'^organization/(?P<org_id>[0-9]+)/delete$', views.organization_delete, name='organization_delete'),

	# Record Groups
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/new$', views.record_group_new, name='record_group_new'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)$', views.record_group, name='record_group'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/delete$', views.record_group_delete, name='record_group_delete'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/update_publish_set_id$', views.record_group_update_publish_set_id, name='record_group_update_publish_set_id'),

	# Jobs
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)$', views.job_details, name='job_details'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/details$', views.job_details, name='job_details'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/delete$', views.job_delete, name='job_delete'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/errors$', views.job_errors, name='job_errors'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/update_note$', views.job_update_note, name='job_update_note'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/update_name$', views.job_update_name, name='job_update_name'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_dpla_field_map$', views.job_dpla_field_map, name='job_dpla_field_map'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/harvest/oai/new$', views.job_harvest_oai, name='job_harvest_oai'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/harvest/static/xml/new$', views.job_harvest_static_xml, name='job_harvest_static_xml'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/transform/new$', views.job_transform, name='job_transform'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/merge/new$', views.job_merge, name='job_merge'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/publish/new$', views.job_publish, name='job_publish'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/reports/create_validation_report$', views.job_reports_create_validation, name='job_reports_create_validation'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/reports/create_audit_report$', views.job_reports_create_audit, name='job_reports_create_audit'),
	url(r'^organization/(?P<org_id>([0-9]|(DYNAMIC_ORG_ID))+)/record_group/(?P<record_group_id>([0-9]|(DYNAMIC_RG_ID))+)/job/(?P<job_id>([0-9]|(DYNAMIC_ID))+)/job_lineage_json$', views.job_lineage_json, name='job_lineage_json'),

	# Job Record Validation Scenarios
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_validation_scenario_failures/(?P<job_validation_id>[0-9]+)$', views.job_validation_scenario_failures, name='job_validation_scenario_failures'),

	# Record Group Job Analysis
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/analysis/indexing_failures$', views.job_indexing_failures, name='job_indexing_failures'),

	# ElasticSearch Analysis
	url(r'^analysis/es/index/(?P<es_index>.+)/field_analysis$', views.field_analysis, name='field_analysis'),
	url(r'^analysis/es/index/(?P<es_index>.+)/field_analysis/docs/(?P<filter_type>.+)$', views.field_analysis_docs, name='field_analysis_docs'),

	# Jobs General
	url(r'^jobs/all$', views.all_jobs, name='all_jobs'),

	# Records
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9]+)$', views.record, name='record'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9]+)/document$', views.record_document, name='record_document'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9]+)/error$', views.record_error, name='record_error'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9]+)/validation_scenario/(?P<job_validation_id>[0-9]+)$', views.record_validation_scenario, name='record_validation_scenario'),

	# Configuration
	url(r'^configuration$', views.configuration, name='configuration'),
	url(r'^configuration/transformation/(?P<trans_id>[0-9]+)/payload$', views.transformation_scenario_payload, name='transformation_scenario_payload'),
	url(r'^configuration/oai_endpoint/(?P<oai_endpoint_id>[0-9]+)/payload$', views.oai_endpoint_payload, name='oai_endpoint_payload'),
	url(r'^configuration/validation/(?P<vs_id>[0-9]+)/payload$', views.validation_scenario_payload, name='validation_scenario_payload'),
	url(r'^configuration/test_validation_scenario$', views.test_validation_scenario, name='test_validation_scenario'),

	# Publish
	url(r'^published$', views.published, name='published'),
	url(r'^published/published_dt_json$', views.DTPublishedJson.as_view(), name='published_dt_json'),

	# OAI
	url(r'^oai$', views.oai, name='oai'),

	# Datatables Endpoints
	url(r'^datatables/all_records/records_dt_json$', views.DTRecordsJson.as_view(), name='all_records_dt_json'),
	url(r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/records_dt_json$', views.DTRecordsJson.as_view(), name='records_dt_json'),
	url(r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/indexing_failures_dt_json$', views.DTIndexingFailuresJson.as_view(), name='indexing_failures_dt_json'),
	url(r'^datatables/organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_validation_scenario_failures_json/(?P<job_validation_id>[0-9]+)$', views.DTJobValidationScenarioFailuresJson.as_view(), name='job_validation_scenario_failures_json'),
	url(r'^datatables/es/index/(?P<es_index>.+)/(?P<search_type>.+)/records_es_dt_json$', DTElasticSearch.as_view(), name='records_es_dt_json'),

	# Analysis
	url(r'^analysis$', views.analysis, name='analysis'),
	url(r'^analysis/new$', views.job_analysis, name='job_analysis'),	

	# general views
	url(r'^login$', auth_views.login, name='login'),
    url(r'^logout$', auth_views.logout, name='logout'),
    url(r'^', views.index, name='combine_home'),
]