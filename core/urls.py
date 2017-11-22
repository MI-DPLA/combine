from django.conf.urls import url
from django.contrib import admin
from django.contrib.auth import views as auth_views

from . import views

urlpatterns = [

	# User Livy sessions
	url(r'^livy_sessions$', views.livy_sessions, name='livy_sessions'),
	url(r'^livy_sessions/stop$', views.livy_session_start, name='livy_session_start'),
	url(r'^livy_sessions/(?P<session_id>[0-9]+)/stop$', views.livy_session_stop, name='livy_session_stop'),

	# Organizations
	url(r'^organizations$', views.organizations, name='organizations'),
	url(r'^organization/(?P<org_id>[0-9]+)$', views.organization, name='organization'),

	# Record Groups
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/new$', views.record_group_new, name='record_group_new'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)$', views.record_group, name='record_group'),

	# Jobs
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)$', views.job_details, name='job_details'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/details$', views.job_details, name='job_details'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/delete$', views.job_delete, name='job_delete'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/errors$', views.job_errors, name='job_errors'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/update_note$', views.job_update_note, name='job_update_note'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_dpla_field_map$', views.job_dpla_field_map, name='job_dpla_field_map'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/harvest/new$', views.job_harvest, name='job_harvest'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/transform/new$', views.job_transform, name='job_transform'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/merge/new$', views.job_merge, name='job_merge'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/publish/new$', views.job_publish, name='job_publish'),
	# DEBUG ##################################################################################################################################
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/job_es_docs$', views.job_es_docs, name='job_es_docs'),
	# DEBUG ##################################################################################################################################

	# Record Group Job Analysis
	# url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/analysis/fields$', views.field_analysis, name='field_analysis'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/analysis/indexing_failures$', views.job_indexing_failures, name='job_indexing_failures'),

	# ElasticSearch Analysis
	url(r'^analysis/es/index/(?P<es_index>.+)/field_analysis$', views.field_analysis, name='field_analysis'),

	# Jobs General
	url(r'^jobs/all$', views.all_jobs, name='all_jobs'),
	url(r'^jobs/input_select$', views.job_input_select, name='job_input_select'),

	# Records
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9]+)$', views.record, name='record'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9]+)/document$', views.record_document, name='record_document'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/record/(?P<record_id>[0-9]+)/error$', views.record_error, name='record_error'),

	# Configuration
	url(r'^configurations$', views.configuration, name='configuration'),
	url(r'^configurations/transformation/(?P<trans_id>[0-9]+)/payload$', views.trans_scen_payload, name='trans_scen_payload'),
	url(r'^configurations/oai_endpoint/(?P<oai_endpoint_id>[0-9]+)/payload$', views.oai_endpoint_payload, name='oai_endpoint_payload'),

	# Publish
	url(r'^published$', views.published, name='published'),
	url(r'^published/published_dt_json$', views.DTPublishedJson.as_view(), name='published_dt_json'),

	# OAI
	url(r'^oai$', views.oai, name='oai'),

	# Datatables Endpoints
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/records_dt_json$', views.DTRecordsJson.as_view(), name='records_dt_json'),
	url(r'^organization/(?P<org_id>[0-9]+)/record_group/(?P<record_group_id>[0-9]+)/job/(?P<job_id>[0-9]+)/indexing_failures_dt_json$', views.DTIndexingFailuresJson.as_view(), name='indexing_failures_dt_json'),
	url(r'^es/index/(?P<es_index>.+)/records_es_dt_json$', views.records_es_dt_json, name='records_es_dt_json'),

	# general views
	url(r'^login$', auth_views.login, name='login'),
    url(r'^logout$', auth_views.logout, name='logout'),
    url(r'^', views.index, name='combine_home'),
]