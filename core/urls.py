from django.conf.urls import url
from django.contrib import admin
from django.contrib.auth import views as auth_views

from . import views

urlpatterns = [

	# Livy sessions
	url(r'^livy_sessions$', views.livy_sessions, name='livy_sessions'),
	url(r'^livy_sessions/create$', views.livy_session_create, name='livy_session_create'),
	url(r'^livy_sessions/([0-9]*)$', views.livy_session_status, name='livy_session_status'),

	# Record Groups
	url(r'^record_groups$', views.record_groups, name='record_groups'),
	url(r'^record_groups/([0-9]*)$', views.record_group, name='record_groups'),

	# general views
	url(r'^login$', auth_views.login, name='login'),
    url(r'^logout$', auth_views.logout, name='logout'),
    url(r'^', views.index, name='combine_home'),
]