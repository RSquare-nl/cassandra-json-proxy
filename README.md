# cassandra-json-proxy
Makes a connection bewteen cassandra db and php

When you connect to cassandra the first time it can be a bit slow.
This is a problem with quick movinsingle connections like php website where every page is a new connection.
The solution i made is a daemon with is a proxy between cassandra and any json connection.

What you need to do:
-compile the proxyserver
-Start the proxyserver 
-Connect to the port 
-Send a json cql
-receive the result

#Possible commands:
{"action":"execute","cql":"SELECT * FROM system.schema_keyspaces ;"}

{"action":"next"}

{"action":"execute","cql":"USE somekeyspace ;"}

{"action":"execute","cql":"SELECT * FROM sometable ;"}

{"action":"next"}

{"action":"quit"}


#Prepare the server for compilation
1. git submodule init
2. git submodule update
3. aptitude install libjson0-dev libuv-dev
4. mkdir cpp-driver/build
5. cd cpp-driver
6. git checkout master
7. cd cpp-driver/build
8. cmake ..
9. make
10. sudo make install
11. cd ../..
12. make

The last make will Compile the proxy server

#Example use
Start the server:
./cassandra-json-proxy -c cloud1.rsquare.nl,cloud2.rsquare.nl -h 127.0.0.1 -p 21121 -w5 -d

This will start the proxy with 5 childs listening to port 21121 on localhost in daemon-mode and connecting to the cloud servers cloud1.rsquare.nl and cloud2.rsquare.nl

If you get an error like:
"error while loading shared libraries: libcassandra.so.2: cannot open shared object file: No such file or directory"

Edit the file and add in the file :
sudo vi /etc/ld.so.conf

/usr/local/lib

Run ldconfig to update your system

sudo ldconfig


#Connections
connect to the port:
commandline:
telnet localhost 21121

{"action":"execute","cql":"SELECT * FROM system.schema_keyspaces ;"}
 
{"action":"next"}

{"action":"next"}

{"action":"quit"}

#To use it in php
```c
require_once 'cassandraproxy.lib.php';
use RSquare\Cassandra;

$cassandraProxy= new RSquare\Cassandra\Proxy('127.0.0.1',21121,'KEYSPACE');
$connection=$cassandraProxy->connectProxy();

$value=$cassandraProxy->escape($value);
$cql="SELECT * FROM system.schema_keyspaces ;";
$cassandraProxy->executeCQL($cql);
if (null === $cassandraProxy->getError()) {
    $ret['rows']=$cassandraProxy->getRowCount();
    while ($cassandraProxy->next()) {
	    $parameters[$cassandraProxy->get('columnfamily_name')]=$cassandraProxy->get('columnfamily_name');
    }
}
//Do some code

//Closing the connetion
if(isset($cassandraProxy)) $cassandraProxy=null;
```
