************
Command Line
************

Though Combine is designed primarily as a GUI interface, the command line provides a potentially powerful and rich interface to the models and methods that make up the Combine data model.  This documentation is meant to expose some of those patterns and conventions.

There are few command line contexts:

  - `Django shell <#django-python-shell>`_

    - *A shell that loads all Django models, with some additional methods for interacting with Jobs, Records, etc.*

  - `Django commands <#combine-django-commands>`_

    - *Combine specific actions that can be executed from bash shell, via Django's manage.py*

  - `Pyspark shell <#pyspark-shell>`_

    - *A pyspark shell that is useful for interacting with Jobs and Records via a spark context.*

These are described in more detail below.

**Note:** For all contexts, the OS ``combine`` user is assumed, using the Combine `Miniconda <https://conda.io/miniconda.html>`__ python environement, which can be activated from any filepath location by
typing:

.. code-block:: bash

    # become combine user, if not already
    su combine

    # activate combine python environment
    source activate combine


Django Python Shell
===================


Starting
--------

From the location ``/opt/combine`` run the following:

.. code-block:: bash

    ./runconsole.py


Useful and Example Commands
---------------------------

**Convenience methods for retrieving instances of Organizations, Record Groups, Jobs, Records**

.. code-block:: python

    '''
    Most all convenience methods are expecting a DB identifier for instance retrieval
    '''

    # retrieve Organization #14
    org = get_o(14)

    # retrieve Record Group #18
    rg = get_rg(18)

    # retrieve Job #308
    j = get_j(308)

    # retrive Record by id '5ba45e3f01762c474340e4de'
    r = get_r('5ba45e3f01762c474340e4de')

    # confirm these retrievals
    '''
    In [2]: org
    Out[2]: <Organization: Organization: SuperOrg>
    In [5]: rg
    Out[5]: <RecordGroup: Record Group: TurboRG>
    In [8]: j
    Out[8]: <Job: TransformJob @ May. 30, 2018, 4:10:21 PM, Job #308, from Record Group: TurboRG>
    In [10]: r
    Out[10]: <Record: Record: 5ba45e3f01762c474340e4de, record_id: 0142feb40e122a7764e84630c0150f67, Job: MergeJob @ Sep. 21, 2018, 2:57:59 AM>
    '''


**Loop through Records in Job and edit Document**

This example shows how it would be possible to:

  - retrieve a Job
  - loop through Records of this Job
  - alter Record, and save

This is not a terribly efficient way to do this, but it demonstrates the data model as accessible via the command line for Combine.  A more efficient method would be to write a custom, Python snippet `Transformation Scenario <configuration.html#transformation-scenario>`_.

.. code-block:: python

    # retrieve Job model instance
    In [3]: job = get_j(563)

    # loop through records via get_records() method, updating record.document (replacing 'foo' with 'bar') and saving
    In [5]: for record in job.get_records():
       ...:     record.document = record.document.replace('foo', 'bar')
       ...:     record.save()



Combine Django Commands
=======================


Combine Update
--------------

It's possible to perform an update of Combine either by pulling changes to the current version (works best with ``dev`` and ``master`` branches), or by passing a specific release to update to (e.g. ``v0.3.2``).  **Note:** This command must be run with **sudo**, as it performs some OS level operations.

The bash script that is run is called ``update.sh``, but this will run ``manage.py update`` in the background.

To update the current branch/release:

.. code-block:: bash

    sudo update.sh

To update to another branch / release tag, e.g. ``v0.3.2``:

.. code-block:: bash

    sudo update.sh v0.3.2


Full State Export
-----------------

One pre-configured ``manage.py`` command is ``exportstate``, which will trigger a full Combine state export (`you can read more about those here <exporting.html#state-export-and-import>`_).  Though this could be done via the Django python shell, it was deemed helpful to expose an OS level, bash command such it could be fired via cron jobs, or other scripting.  It makes for a convenient way to backup the majority of important data in a Combine instance.

Without any arguments, this will export *all* Organizations, Record Groups, Jobs, Records, and Configuration Scenarios (think OAI Endpoints, Transformations, Validations, etc.); effectively anything stored in databases.  This does *not* include conigurations to ``localsettings.py``, or other system configurations, but is instead meant to really export the current state of the application.

.. code-block:: bash

    ./manage.py exportstate

Users may also provide a string of JSON to skip specific model instances.  This is somewhat experimental, and currently **only works for Organizations**, but it can be helpful if a particular Organization need not be exported.  This ``skip_json`` argument is expecting Organization ids as integers; the following is an example if skipping Organization with id == ``4``:

.. code-block:: bash

    ./manage.py exportstate --skip_json '{"orgs":[4]}'


Pyspark Shell
=============

The pyspark shell is an instance of Pyspark, with some configurations that allow for loading models from Combine.

**Note:**

The pyspark shell requires the Hadoop Datanode and Namenode to be active.  These are likely running by defult, but in the event they are not, they can be started with the following (Note: the trailing ``:`` is required, as that indicates a group of processes in `Supervisor <http://supervisord.org/>`_):

.. code-block:: bash

    sudo supervisorctl restart hdfs:

**Note:**

The pyspark shell when invoked as described below, will be launched in the same Spark cluster that Combine's Livy instance uses.  Depending on avaialble resources, it's likely that users will need to **stop** any active Livy sessions as `outlined here <spark_and_livy.html#manage-livy-sessions>`_ to allow this pyspark shell the resources to run. 


Starting
--------

From the location ``/opt/combine`` run the following:

.. code-block:: bash

    ./pyspark_shell.sh


Useful and Example Commands
---------------------------

**Open Records from a Job as a Pyspark DataFrame**

.. code-block:: python

    # import some convenience variables, classes, and functions from core.spark.console
    from core.spark.console import *

    # retrieve Records from MySQL as pyspark DataFrame
    '''
    In this example, retrieving records from Job #308
    Also of note, must pass spark instance as first argument to convenience method,
    which is provided by pyspark context
    '''
    job_df = get_job_as_df(spark, 308)

    # confirm retrieval okay
    job_df.count()
    ...
    ...
    Out[5]: 250

    # look at DataFrame columns
    job_df.columns
    Out[6]: 
    ['id',
     'combine_id',
     'record_id',
     'document',
     'error',
     'unique',
     'unique_published',
     'job_id',
     'published',
     'oai_set',
     'success',
     'valid',
     'fingerprint']


