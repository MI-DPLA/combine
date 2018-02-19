# Quickstart

Sometimes you can't beat kicking the tires to see how an application works.  Demo data from unit tests will be reused here to avoid the need to provide actual OAI-PMH endpoints, transformation XSLT, or other configurations unique to a DPLA Service Hub.  

This quickstart guide will walk through:

  * starting / stopping Combine
  * data model (Organizations, RecordGroups, Jobs, Records)
  * configuration
    * setting up OAI-PMH endpoints
    * creating Transformation Scenarios
    * creating Validation Scenarios
  * harvesting records
  * transforming records
  * duplicating / merging jobs
  * validation of records
    * running reports
  * publishing records

For simplicity's sake, we will assume Combine is installed on a server with the domain name of `combine`, though likely running at the IP `192.168.45.10`, which the Ansible/Vagrant install from [Combine-Playbook](https://github.com/WSULib/combine-playbook) defaults to.  On most systems you can point that IP to a domain name like `combine` by modifying your `/etc/hosts` file on your local machine.  **Just fair warning:** `combine` and `192.168.45.10` will be used interchangeably throughout.
  

## Starting / Stopping Combine

### Gunicorn

For normal operations, Combine is run using [Supervisor](http://supervisord.org/), a python based application for running system processes.  Specifically, it's running under the Python WSGI server [Gunicorn](http://gunicorn.org/), under the supervisor program named `gunicorn`.

Start Combine:
```
sudo supervisorctl start gunicorn
```

Stop Combine:
```
sudo supervisorctl stop gunicorn
```

You can confirm that Combine is running by visiting [`http://192.168.45.10/combine`](http://combine-vm/combine), where you should be prompted to login with a username and password.  For default/testing installations, you can use `combine` / `combine` for these credentials.

### Django runserver

You can also run Combine via Django's built-in server.

Convenience script, from `/opt/combine`:
```
./runserver.sh
```

Or, you can run the Django command explicitly from `/opt/combine`:
```
./manage.py runserver --noreload 0.0.0.0:8000
```

You can confirm that Combine is running by visiting [`http://192.168.45.10:8000/combine`](http://combine-vm/combine) (note the `8000` port number).

### Livy Sessions

To run any Jobs, Combine relies on an active (idle) Apache Livy session.  Livy is what makes running Spark jobs possible via the familiar request/response cycle of a Django application.  

Currently, users are responsible for determining if the Livy session is ready, though there are plans to have this automatically handled.  

To check and/or start new Livy session, navigate to: [http://192.168.45.10/combine/livy_sessions](http://192.168.45.10/combine/livy_sessions).  The important column is `status` which should read `idle`.  If not, click `Stop` or `Remove` under the `actions` column, and once stopped, click the `start new session` link.  Takes anywhere from 10-20 seconds to become `idle`.  

You can check the status of the Livy session at a glance from the Combine navigation, which should read `Livy/Spark Session (idle)` and be green if active.


## Combine Data Model 

### Organization

The highest level of organization in Combine is an **Organization**.  Organizations are intended to group and organize records at the level of an institution or organization, hence the name!

You can create a new Organization from the Organizations page at [http://192.168.45.10/combine/organizations](http://192.168.45.10/combine/organizations), or by clicking "Organizations" from Combine navigation.

An example, we can create one with the name "Amazing University".  Only the `name` field is required, others are optional.

### RecordGroup

Within Organizations are **RecordGroups**.  RecordGroups are a "bucket" at the level of a bunch of intellectually similar records.  It is worth noting now that a single RecordGroup can contain multiple **Jobs**, whether they are failed or incomplete attempts, or across time.  Suffice it to say for now that RecordGroups may contain lots of Jobs, which we will create here in a minute through harvests, transforms, etc.

For our example Organization, "Amazing University", an example of a reasonable RecordGroup might be their Fedora Commons based digital repository.  To create a new one, from the [Organizations page](http://192.168.45.10/combine/organizations), click on "Amazing University".  From the Organiation page for "Amazing University" you can create a new RecordGroup.  Let's call it "Fedora Repository"; again, no other fields are required beyond `name`.

Finally, click into the newly created RecordGroup "Fedora Repository" to see the RecordGroup's page, where we can begin to run Jobs.

### Jobs

Central to Combine's workflow philosophy are the ideas of **Jobs**.  Jobs include any of the following:

  * **Harvest** (OAI-PMH, static XML, and others to come)
  * **Transform**
  * **Merge/Duplicate**
  * **Publish**
  * **Analysis**

Within the context of a RecordGroup, one can think of Jobs as "stages" of a group of records, one Job serving as the **input** for the next Job run on those records. i.e.

```
OAI-PMH Harvest Job ---> XSLT Transform Job --> Publish Job
```

### Record

Lastly, the most granular major entity in Combine is an individual **Record**.  Records exist within a Job.  When a Job is deleted, so are the Records (the same can be said for any of these hierarchies moving up).  Records will be created in the course of running Jobs, so won't dwell on them much now.


## Configuration



## Harvesting Records































