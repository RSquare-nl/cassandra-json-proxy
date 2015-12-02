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

Possible commands:
{"action":"execute","cql":"SELECT * FROM system.schema_keyspaces ;"}

{"action":"next"}

{"action":"quit"}


Compile the server:
make


Example use
Start the server:
./cassandra-json-proxy -c cloud1.rsquare.nl,cloud2.rsquare.nl -h 127.0.0.1 -p 21121 -w5 -d

connect to the port:
commandline:
telnet localhost 21121
 {"action":"execute","cql":"SELECT * FROM system.schema_keyspaces ;"}
 
{"action":"next"}

{"action":"next"}

{"action":"quit"}

