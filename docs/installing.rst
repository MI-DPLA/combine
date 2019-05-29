********************
Installing Combine
********************

Combine has a fair amount of server components, dependencies, and configurations that must be in place to work, as it leverages [Apache Spark](https://spark.apache.org/), among other applications, for processing on the backend.  There are a couple of deployment options.

Docker
======

A GitHub repository `Combine-Docker <https://github.com/MI-DPLA/combine-docker>`_ exists to help stand up an instance of Combine as a series of interconnected Docker containers.

Server Provisioning with Vagrant and/or Ansible
===============================================

To this end, use the repository, `Combine-playbook <https://github.com/MI-DPLA/combine-playbook>`_ , which has been created to assist with provisioning a server with everything neccessary, and in place, to run Combine.  This repository provides routes for server provisioning via `Vagrant <https://www.vagrantup.com/>`_ and/or `Ansible <https://www.ansible.com/>`_ . Please visit the `Combine-playbook <https://github.com/MI-DPLA/combine-playbook>`_ repository for more information about installation.
