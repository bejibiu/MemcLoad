MemcLoad
======
an app for honing the skill of converting an app into a multithreaded app

INSTALL
------
On linux:
```shell script
git clone https://github.com/bejibiu/MemcLoad
python -m venv venv
source venv/bin/active
pip install -r requrements.txt
```
on Windows:
```shell script
git clone https://github.com/bejibiu/MemcLoad
python -m venv venv
source venv\Script\active
pip install -r requrements.txt
```

Options
------

| Options | Default | Description |
| ------- | ----- |-------------|
`timeout`|3|timeout in seconds for all calls to a server memcached. Defaults to 3 seconds.|
`retry`|3|retry connection to set value to memcached. Defaults to 3 attempts
`num-workers`|5|count num of workers
`chunck-size`|10|number of lines to process per thread
`test`| False| 
`log`| None| 
`dry`| False| 
`pattern`| "/data/appsinstalled/*.tsv.gz"| 
`idfa`| "127.0.0.1:33013"| 
`gaid`| "127.0.0.1:33014"| 
`adid`| "127.0.0.1:33015"| 
`dvid`| "127.0.0.1:33016"| 

For test in docker
-------------
```shell script
docker-compose up
```