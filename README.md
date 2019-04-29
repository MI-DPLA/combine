# &#128668; Combine

## Overview

Combine is a Django application to facilitate the harvesting, transformation, analysis, and publishing of metadata records by Service Hubs for inclusion in the [Digital Public Library of America (DPLA)](https://dp.la/).

The name "Combine", pronounced /kämˌbīn/, is a nod to the [combine harvester used in farming](https://en.wikipedia.org/wiki/Combine_harvester) famous for, "combining three separate harvesting operations - reaping, threshing, and winnowing - into a single process"  Instead of grains, we have metadata records!  These metadata records may come in a variety of metadata formats, various states of transformation, and may or may not be valid in the context of a particular data model.  Like the combine equipment used for farming, this application is designed to provide a single point of interaction for multiple steps along the way of harvesting, transforming, and analyzing metadata in preperation for inclusion in DPLA.

## Documentation

[![Documentation Status](https://readthedocs.org/projects/combine/badge/?version=master)](http://combine.readthedocs.io/en/master/?badge=master)

Documentation is available at [Read the Docs](http://combine.readthedocs.io/).

## Installation

Combine has a fair amount of server components, dependencies, and configurations that must be in place to work, as it leverages [Apache Spark](https://spark.apache.org/), among other applications, for processing on the backend.  There are a couple of deployment options.

### Docker

A GitHub repository [Combine-Docker](https://github.com/WSULib/combine-docker) exists to help stand up an instance of Combine as a series of interconnected Docker containers.

### Server Provisioning with Vagrant and/or Ansible

To this end, use the repository, [Combine-playbook](https://github.com/WSULib/combine-playbook), which has been created to assist with provisioning a server with everything neccessary, and in place, to run Combine.  This repository provides routes for server provisioning via [Vagrant](https://www.vagrantup.com/) and/or [Ansible](https://www.ansible.com/). Please visit the [Combine-playbook](https://github.com/WSULib/combine-playbook) repository for more information about installation.

## Development

### Running locally

Install dependencies with `pip install -r requirements.txt`. On Linux, you may need to install the system packages `libxml2-devel/-dev`, `python3-devel/-dev`, `libxslt-devel/-dev`. On Fedora Linux, you may need to install the system package `redhat-rpm-config`.

You will want to create a `combine/localsettings.py` file. You can use the template, but you might want to set the `BINARY_STORAGE`, `STATEIO_EXPORT_DIR`, and `STATEIO_IMPORT_DIR` filepaths to somewhere in your home directory so the process doesn't need root permissions to create its storage directories.

Run `./manage.py makemigrations core` to make the sql migrations for the app, and then run `./manage.py migrate`.
