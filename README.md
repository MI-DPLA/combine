# &#128668; Combine

## Overview

Combine is a Django application to facilitate the harvesting, transformation, analysis, and publishing of metadata records by Service Hubs for inclusion in the [Digital Public Library of America (DPLA)](https://dp.la/).

The name "Combine", pronounced /kämˌbīn/, is a nod to the [combine harvester used in farming](https://en.wikipedia.org/wiki/Combine_harvester) famous for, "combining three separate harvesting operations - reaping, threshing, and winnowing - into a single process"  Instead of grains, we have metadata records!  These metadata records may come in a variety of metadata formats, various states of transformation, and may or may not be valid in the context of a particular data model.  Like the combine equipment used for farming, this application is designed to provide a single point of interaction for multiple steps along the way of harvesting, transforming, and analyzing metadata in preparation for inclusion in DPLA.

## Documentation

[![Documentation Status](https://readthedocs.org/projects/combine/badge/?version=master)](http://combine.readthedocs.io/en/master/?badge=master)

The combine team is in the process of updating the documentation. The installation process and user interface have had significant changes. In the meantime some out-of-date documentation is available at [Read the Docs](http://combine.readthedocs.io/).

## Installation

Combine has a fair amount of server components, dependencies, and configurations that must be in place to work, as it leverages [Apache Spark](https://spark.apache.org/), among other applications, for processing on the backend.  For previous version of combine there were a couple of deployment options. However, for the current and future versions (v0.11.1 and after) only the docker option is available.

### Docker

A GitHub repository [Combine-Docker](https://github.com/MI-DPLA/combine-docker) exists to help stand up an instance of Combine as a series of interconnected Docker containers.

### Security Warning

Combine code should be run behind your institution's firewall on a secured server. Access to combine should be protected by your instituion-wide identity and password system, preferably using two-factor authentication. If your institution supports using VPNs for access to the server's network that is a good additional step. 

This is in addition to the combine's own passwords. While we haven't got explicit documentation on how to set up SSL inside the provided nginx in combine it's possible and strongly recommended.

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
