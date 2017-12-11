# Overview

Combine is a Django application to facilitate the harvesting, transformation, analysis, and publishing of metadata records by Service Hubs for inclusion in the [Digital Public Library of America (DPLA)](https://dp.la/).

# Installation

Combine has a decent amount of dependencies and configurations that must be in place to work, as it leverages [Apache Spark](https://spark.apache.org/) for processing on the backend.  As such, please see the following repositories for help provisioning a server that includes Apache Spark, the Combine Django application, and other dependencies:

  * [combine-vm](https://github.com/WSULib/combine-vm): Vagrant and VirtualBox
  * [combine-playbook](https://github.com/WSULib/combine-playbook): Ansible playbook to deploy Combine to a target server

