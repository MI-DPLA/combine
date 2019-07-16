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

A GitHub repository [Combine-Docker](https://github.com/MI-DPLA/combine-docker) exists to help stand up an instance of Combine as a series of interconnected Docker containers.

### Server Provisioning with Vagrant and/or Ansible

To this end, use the repository, [Combine-playbook](https://github.com/MI-DPLA/combine-playbook), which has been created to assist with provisioning a server with everything neccessary, and in place, to run Combine.  This repository provides routes for server provisioning via [Vagrant](https://www.vagrantup.com/) and/or [Ansible](https://www.ansible.com/). Please visit the [Combine-playbook](https://github.com/MI-DPLA/combine-playbook) repository for more information about installation.

## Tech Stack Details

### Django
The whole app is a Django app.

### MySQL
The system configuration is stored in MySQL. This includes users, organizations, record groups, jobs, transformations, validation scenarios, and so on.

### Mongo
The harvested and transformed Records themselves are stored in MongoDB, to deal with MySQL's scaling problems.

### ElasticSearch
We use ElasticSearch for indexing and searching the contents of Records.

### Celery
Celery runs background tasks that don't deal with largescale data, like prepping job reruns or importing/exporting state.

### Redis
Redis is just keeping track of Celery's job queue.

### Livy
Livy is a REST interface to make it easier to interact with Spark.

### Apache Spark
Spark runs all the Jobs that harvest or alter records, for better scalability.

### Hadoop
Hadoop is just backing up Spark.

## User-suppliable Configurations

### Field Mapper
Field Mappers let you make changes when mapping a Record from XML to key/value pairs (JSON).
#### XML to Key-Value Pair
#### XSL Stylesheet
#### Python Code Snippet

### Transformation
Transformations let you take a Record in one format and turn it into a new Record in another format.
#### XSLT Stylesheet
#### Python Code Snippet
#### Open Refine Actions

### Validation Scenario
You can run Validation Scenarios against the Records in a Job to find out which records do or do not meet the requirements of the Validation Scenario. 
#### Schematron
#### Python Code Snippet
#### ElasticSearch Query
#### XML Schema

### Record Identifier Transformation Scenario
RITS are used to transform a Record's Identifier, which is used for publishing and for uniqueness checks.
#### Regular Expression
#### Python Code Snippet
#### XPath Expression
