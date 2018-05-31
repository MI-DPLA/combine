************
Command Line
************

Though Combine is designed primarily as a GUI interface, the command line provides a powerful and rich interface to the models and methods that make up the Combine data model.  This documentation is meant to expose some of those patterns and conventions.

There are two primary command line contexts:

  - **Django shell**: A shell that loads all Django models, with some additional methods for interacting with Jobs, Records, etc.
  - **Pyspark shell**: A pyspark shell that is useful for interacting with Jobs and Records via a spark context.  

These are described in more detail below.

**Note:** For both, the Combine `Miniconda <https://conda.io/miniconda.html>`__ python environement must be used, which can be activated from any filepath location by
typing:

.. code-block:: bash

    source activate combine


Django Python Shell
===================


Starting
--------

From the location ``/opt/combine`` run the following:

.. code-block:: bash

    ./runconsole.py


Useful Commands
---------------

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

    # retrive Record #1756676
    r = get_r(1756676)

    # confirm these retrievals
    '''
    In [2]: org
    Out[2]: <Organization: Organization: SuperOrg>
    In [5]: rg
    Out[5]: <RecordGroup: Record Group: TurboRG>
    In [8]: j
    Out[8]: <Job: TransformJob @ May. 30, 2018, 4:10:21 PM, Job #308, from Record Group: TurboRG>
    In [10]: r
    Out[10]: <Record: Record: #1756676, record_id: 175c099be37b52c4b278400fb64e738d, job_id: 308, job_type: TransformJob>
    '''

Pyspark Shell
=============

The pyspark shell is an instance of Pyspark, with some configurations that allow for loading models from Combine.

The pyspark shell requires the Hadoop Datanode and Namenode to be active.  These are likely running by defult, but in the event they are not, they can be started with the following (Note: the trailing ``:`` is required, as that indicates a group of processes in `Supervisor <http://supervisord.org/>`_):

.. code-block:: bash

    sudo supervisorctl restart hdfs:


Starting
--------

From the location ``/opt/combine`` run the following:

.. code-block:: bash

    ./pyspark_shell.sh


Useful Commands
---------------

**Open Records from a Job as a Pyspark DataFrame**

.. code-block:: python

    # import Combine module utils from core.spark
    from core.spark import utils

    # retrieve Records from MySQL as pyspark DataFrame
    '''
    In this example, retrieving records from Job #308
    Also of note, must pass spark instance as first argument to convenience method,
    which is provided by pyspark context
    '''
    job_df = utils.get_job_as_df(spark, 308)

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


