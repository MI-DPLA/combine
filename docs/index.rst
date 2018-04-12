*****************
🚜 Combine
*****************


Overview
========

Combine is a Django application to facilitate the harvesting, transformation, analysis, and publishing of metadata records by Service Hubs for inclusion in the `Digital Public Library of America (DPLA) <https://dp.la/>`_.

The name "Combine", pronounced ``/kämˌbīn/``, is a nod to the `combine harvester used in farming <https://en.wikipedia.org/wiki/Combine_harvester>`_ famous for, "combining three separate harvesting operations - reaping, threshing, and winnowing - into a single process"  Instead of grains, we have metadata records!  These metadata records may come in a variety of metadata formats, various states of transformation, and may or may not be valid in the context of a particular data model.  Like the combine equipment used for farming, this application is designed to provide a single point of interaction for multiple steps along the way of harvesting, transforming, and analyzing metadata in preperation for inclusion in DPLA.


Installation
============

Combine has a fair amount of server components, dependencies, and configurations that must be in place to work, as it leverages `Apache Spark <https://spark.apache.org/>`_, among other applications, for processing on the backend.

To this end, a separate GitHub repository, `Combine-playbook <https://github.com/WSULib/combine-playbook>`_, has been created to assist with provisioning a server with everything neccessary, and in place, to run Combine.  This repository provides routes for server provisioning via `Vagrant <https://www.vagrantup.com/>`_ and/or `Ansible <https://www.ansible.com/>`_. Please visit the `Combine-playbook <https://github.com/WSULib/combine-playbook>`_ repository for more information about installation.



Documentation Table of Contents
===============================

.. toctree::
   :maxdepth: 2
      
   quickstart
   tests