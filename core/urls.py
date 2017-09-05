from django.conf.urls import url

from . import views

urlpatterns = [

	# Livy sessions
	url(r'^livy_sessions$', views.livy_sessions, name='livy_sessions'),
	url(r'^livy_sessions/create$', views.livy_session_create, name='livy_session_create'),
	url(r'^livy_sessions/([0-9]*)$', views.livy_session_status, name='livy_session_status'),

	# default index
    url(r'^', views.index, name='test'),
]