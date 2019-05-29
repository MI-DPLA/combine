# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import logging
from xmlrpc import client as xmlrpc_client

# import mongo dependencies
from core.mongo import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



class SupervisorRPCClient(object):

	def __init__(self):

		self.server = xmlrpc_client.ServerProxy('http://localhost:9001/RPC2')


	def get_server_state(self):

		return self.server.supervisor.getState()


	def list_processes(self):

		return self.server.supervisor.getAllProcessInfo()


	def check_process(self, process_name):

		return self.server.supervisor.getProcessInfo(process_name)


	def start_process(self, process_name):

		return self.server.supervisor.startProcess(process_name)


	def stop_process(self, process_name):

		return self.server.supervisor.stopProcess(process_name)


	def restart_process(self, process_name):

		'''
		RPC throws Fault 70 if not running, catch when stopping
		'''

		# attempt to stop
		try:
			self.stop_process(process_name)
		except Exception as e:
			logger.debug(str(e))

		# start process
		return self.start_process(process_name)


	def stdout_log_tail(self, process_name, offset=0, length=10000):

		return self.server.supervisor.tailProcessStdoutLog(process_name, offset, length)[0]


	def stderr_log_tail(self, process_name, offset=0, length=10000):

		return self.server.supervisor.tailProcessStderrLog(process_name, offset, length)[0]