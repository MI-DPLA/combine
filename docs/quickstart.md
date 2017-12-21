# Quickstart

Sometimes you can't beat kicking the tires to see how an application works.  Demo data from unit tests will be reused here to avoid the need to provide actual OAI-PMH endpoints, transformation XSLT, or other configurations unique to a DPLA Service Hub.  

This quickstart guide will walk through:

  * starting / stopping Combine
  * harvesting records
  * transforming records
  * duplicating / merging groups of records
  * brief look at validation of records
  * publishing records

For simplicity's sake, we will assume the Aggregator-in-a-Box server has a domain name of `combine-vm`.  It may likely be running at an IP like `192.168.45.10`, which the Vagrant install from [AgBox](https://github.com/WSULib/AgBox) defaults to, but on most systems you can point that IP to a domain name like `combine-vm` by modifying your `/etc/hosts` file on your local machine.
  
## Starting / Stopping Combine

For normal operations, Combine is run using [Supervisor](http://supervisord.org/), a python based application for running system processes.

Start Combine (if not already started):
```
sudo supervisorctl start combine
```

Stop Combine:
```
sudo supervisorctl start combine
```

You can confirm that Combine is running by visiting [`http://combine-vm/combine`](http://combine-vm/combine), where you should be prompted to login with a username and password.  For default/testing installations, you can use `combine` / `combine` for these credentials.