Guide on using aCT
==================




Dependencies
------------

* MySQL
* ARC Client
* Python 2 (with pip)




Installation
------------

Install dependencies.

Unpack aCT archive wherever you want. Location of unpacked archive is from
now on refered to as **_aCT-root_**.

Navigate to _aCT-root/bin_:

```bash
$ cd aCT-root/bin
```

Run setup:

```bash
$ ./setup.sh
```

Configure clusters in **_aCT-root/bin/sites.json_**. Clusters have to be organized
using __sites__. A site is an array of clusters. When job is submitted, site
has to be provided. That job will then be submitted to the "best" cluster in
given site.
To configure clusters, add an array to _"sites"_ JSON object, name of array
should equal to site name and cluster URLs are array elements. For more info on
syntax, take a look at JSON documentation or tutorials.




Starting aCT
------------

Activate python virtual environment. _"(venv)"_ will be added to your
shell prompt, meaning that virtual environment is active.

```bash
$ source aCT-root/bin/venv/bin/activate
```

Start aCT:

```bash
(venv) $ start.sh
```



Using aCT
---------

Make sure to setup and start aCT first, and that virtual environment
is activated.

Before submitting jobs to aCT, proxy certificates have to be created. __arcproxy__
should be used for that. Then, created proxy certificates have to be added to
aCT using _actproxy.py_. To create proxy certificate with default settings and
add it to aCT, run:

```bash
(venv) $ arcproxy
(venv) $ actproxy.py
```

You can use aCT with **_act*.py_** commands which are added to _PATH_ and can
be used from anywhere as long as virtual environment is active. You
can get basic usage info by using _-h_ switch with command:

```bash
(venv) $ actsub.py -h
```

Commands currently available:

* actbulksub.py	- submit multiple xrsl jobs given as arguments
* actclean.py	- clean finished or failed jobs
* actfetch.py	- fetch job results from ARC CE
* actget.py	- get fetched job results from aCT
* actkill.py	- kill jobs
* actproxy.py	- add proxy to aCT
* actresub.py	- resubmit failed jobs
* actstat.py	- get job status
* actsub.py	- submit a job




Stopping aCT
------------

Run script to stop aCT:

```bash
(venv) $ stop.sh
```

Finally, deactivate virtual enironment. The _"(venv)"_ will disappear from prompt.

```bash
(venv) $ deactivate
```



Uninstallation
--------------

When you want to remove aCT from system, first make sure that it's stopped,
then just remove _aCT-root_. If you
configured any external log, tmp, config locations etc., you have to
remove those manually.
