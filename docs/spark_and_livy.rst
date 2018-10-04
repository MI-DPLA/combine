**************
Spark and Livy
**************

Combine was designed to provide a single point of interaction for metadata harvesting, transformation, analysis, and publishing.  Another guiding factor was a desire to utilize `DPLA's Ingestion 3 <https://github.com/dpla/ingestion3>`_ codebase where possible, which itself, uses `Apache Spark <https://spark.apache.org/>`_ for processing large numbers of records.  The decision to use Ingestion 3 drove the architecture of Combine to use Apache Spark as the primary, background context and environment for processing Records.

This is well and good from a command line, issuing individual tasks to be performed, but how would this translate to a GUI that could be used to initiate tasks, queue them, and view the results?  It became evident that an intermediary piece was needed to facilitate running Spark "jobs" from a request/response oriented front-end GUI.  `Apache Livy <https://livy.incubator.apache.org/>`_ was suggested as just such a piece, and fit the bill perfectly.  Livy allows for the submission of jobs to a running Spark context via JSON, and the subsequent ability to "check" on the status of those jobs.

As Spark natively allows python code as a language for submitting jobs, Django was chosen as the front-end framework for Combine, to have some parity between the language of the GUI front-end and the language of the actual code submitted to Spark for batch processing records.

This all conspires to make Combine relatively fast and efficient, but adds a level of complexity.  When Jobs are run in Combine, they are submitted to this running, background Spark context via Livy.  While Livy is utilized in a similar fashion at scale for large enterprise systems, it is often obfuscated from users and the front-end GUI.  This is partially the case for Combine.


Livy Sessions
=============

Livy creates Spark contexts that can receive jobs via what it calls "sessions".  In Combine, only one active Livy session is allowed at a time.  This is partially for performance reasons, to avoid gobbling up all of the server's resources, and partially to enforce a sequential running of Spark Jobs that avoids many of the complexities that would be introduced if Jobs -- that require input from the output of one another -- were finishing at different times.


Manage Livy Sessions
--------------------

Navigate to the "System" link at the top-most navigation of Combine.  If no Livy sessions are found or active, you will be presented with a screen that looks like this:

.. figure:: img/livy_session_none.png
   :alt: Livy sessions management: No Livy sessions found
   :target: _images/livy_session_none.png

   Livy sessions management: No Livy sessions found

To begin a Livy session, click "Start New Livy Session".  The page will refresh and you should see a screen that shows the Livy session is ``starting``:

.. figure:: img/livy_session_starting.png
   :alt: Livy sessions management: Livy sesesion starting
   :target: _images/livy_session_starting.png

   Livy sessions management: Livy sesesion starting

After 10-20 seconds, the page can be refreshed and it should show the Livy session as ``idle``, meaning it is ready to receive jobs:

.. figure:: img/livy_session_idle.png
   :alt: Livy sessions management: Livy sesesion idle
   :target: _images/livy_session_idle.png

   Livy sessions management: Livy sesesion idle

Barring any errors with Livy, this is the only interaction with Livy that a Combine user needs to concern themselves with.  If this Livy Session grows stale, or is lost, Combine will attempt to automatically restart when it's needed.  This will actually remove and begin a new session, but this should remain invisible to the casual user.  

However, a more advanced user may choose to **remove** an active Livy session from Combine from this screen.  When this happens, Combine cannot automatically refresh the Livy connection when needed, and all work requiring Spark will fail.  To begin using Livy/Spark again, a new Livy session will need to be manually started per the instructions above.

