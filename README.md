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

{"action":"quit"}


#Prepare the server for compilation
git submodule init
git submodule update
aptitude install libjson0-dev libuv-dev
mkdir cpp-driver/build
cd cpp-driver/build
cmake ..
make
make install
cd ../..

#Compile the server
make


#Example use
Start the server:
./cassandra-json-proxy -c cloud1.rsquare.nl,cloud2.rsquare.nl -h 127.0.0.1 -p 21121 -w5 -d

connect to the port:
commandline:
telnet localhost 21121
 {"action":"execute","cql":"SELECT * FROM system.schema_keyspaces ;"}
 
{"action":"next"}

{"action":"next"}

{"action":"quit"}

#To use it in php
$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
$connection = socket_connect($socket,'localhost', 21121);
$ar['action']="execute";
$ar['cql']="SELECT * FROM system.schema_keyspaces ;";
socket_write($socket,json_encode($ar));
if($buffer = socket_read($socket,2048)){
	$ar=json_decode($buffer,true);
}

for($i=0;$i<$ar['row_count'];$i++){
	$s['action']="next";
	socket_write($socket,json_encode($s));
	if($buffer = socket_read($socket,2048)){
		$a=json_decode($buffer,true);
		vardump($a);
	}
}

socket_close($socket);
