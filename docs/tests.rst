*****
Tests
*****

Though Combine is by and large a Django application, it has characteristics that do not lend themselves towards using the built-in Django unit tests.  Namely, DB tables that are not managed by Django, and as such, would not be created in the test DB scaffolding that Django tests usually use.

Instead, Combine uses out-of-the-box `pytest <https://docs.pytest.org/en/latest/>`_ for unit tests.


Demo data
=========

In the directory `/tests`, some demo data is provided for simulating harvest, transform, merge, and publishing records.  

* ``mods_250.xml`` - 250 MODS records, as returned from an OAI-PMH response
	* during testing this file is parsed, and 250 discrete XML files are written to a temp location to be used for a test static XML harvest
* ``mods_transform.xsl`` - XSL transformation that performs transformations on the records from ``mods_250.xml``
	* during transformation, this XSL file is added as a temporary transformation scenario, then removed post-testing


Running tests
=============

**Note:** *Because Combine currently only allows one job to run at a time, and these tests are essentially small jobs that will be run, it is important that no other jobs are running in Combine while running tests.*

Tests should be run from the root directory of Combine, if Ansible or Vagrant builds, likely at ``/opt/combine``.  Also requires sourcing the anaconda Combine environment with `source activate combine`.

It is worth noting whether or not there is an active Livy session already for Combine, or if one should be created and destroyed for testing.  Combine, at least at the time of this writing, operates only with a single Livy instance.  By default, tests will create and destroy a Livy session, but this can be skipped in favor of using an active session by including the flag ``--use_active_livy``.

Testing creates a test ``Organization``, ``RecordGroup``, and ``Job``'s during testing. By default, these are removed after testing, but can be kept for viewing or analysis by including the flag ``--keep_records``.


Examples
========

**run tests, no output, create Livy session, destroy records**

.. code-block:: bash

	pytest


**run tests, see output, use active Livy session, keep records after test**

.. code-block:: bash

	pytest -s --use_active_livy --keep_records