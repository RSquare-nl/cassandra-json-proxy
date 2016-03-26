/*
	This is free and unencumbered software released into the public domain.

	Anyone is free to copy, modify, publish, use, compile, sell, or
	distribute this software, either in source code form or as a compiled
	binary, for any purpose, commercial or non-commercial, and by any
	means.

	In jurisdictions that recognize copyright laws, the author or authors
	of this software dedicate any and all copyright interest in the
	software to the public domain. We make this dedication for the benefit
	of the public at large and to the detriment of our heirs and
	successors. We intend this dedication to be an overt act of
	relinquishment in perpetuity of all present and future rights to this
	software under copyright law.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
	EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
	IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
	OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
	ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
	OTHER DEALINGS IN THE SOFTWARE.

	For more information, please refer to <http://unlicense.org/>
	*/

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <json/json.h>
#include <string.h>
#include <syslog.h>

#include <cassandra.h>
#include "thread.h"
#include "wqueue.h"
#include "tcpacceptor.h"

#define NUM_CONCURRENT_REQUESTS 1000
#define BUFFERSIZE 100000

#define ACTION_SQL 1
#define ACTION_QUIT 2

#define DAEMON_NAME "cassandra-proxy"

union lengthSplit{
	   unsigned long length;
		unsigned char bytes[4];
};

int isDaemon;

struct Basic_ {
	cass_bool_t bln;
	cass_float_t flt;
	cass_double_t dbl;
	cass_int32_t i32;
	cass_int64_t i64;
	const char *pstr;
};
typedef struct Basic_ Basic;

void print_error(CassFuture* future) {
	const char* message;
	size_t message_length;
	cass_future_error_message(future, &message, &message_length);
	fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
	syslog(LOG_INFO, message);
}

CassCluster* create_cluster(char *cassandrahosts) {
	CassCluster* cluster = cass_cluster_new();
	cass_cluster_set_contact_points(cluster, cassandrahosts);
	return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
	CassError rc = CASS_OK;
	CassFuture* future = cass_session_connect(session, cluster);

	cass_future_wait(future);
	rc = cass_future_error_code(future);
	if (rc != CASS_OK) {
		print_error(future);
	}
	cass_future_free(future);

	return rc;
}

void print_usage() {
	printf("\nUsage: cassandra-proxy -c 'cassandra1.nl,cassandra2.nl' -p <portnumber> -w 5\n");
	printf("\n\nOptions:");
	printf("\n-d ; make it a daemon, output goes in syslog");
	printf("\n-c cassandrahosts ; comma seperated list of hosts");
	printf("\n-i ipnumber ; to listen on");
	printf("\n-p portnumber ; to listen on");
	printf("\n-w workers ; amount of workers to startup, determines how manny concurrent users can connect");
	printf("\n\nExample:");
	printf("\ncassandra-proxy -c 'cloud1.cassandra.nl,cloud2.cassandra.nl' -h 127.0.0.1 -p 21121 -w 5\n");
	printf("\n");
}

class WorkItem
{
	TCPStream* m_stream;

	public:
	WorkItem(TCPStream* stream) : m_stream(stream) {}
	~WorkItem() { delete m_stream; }

	TCPStream* getStream() { return m_stream; }
};

class ConnectionHandler : public Thread
{
	wqueue<WorkItem*>& m_queue;
	char *cassandrahosts;
	char *message=NULL;

	public:
	ConnectionHandler(wqueue<WorkItem*>& queue,char *casshosts) : m_queue(queue) {
		cassandrahosts=casshosts;
	}

	void* run() {
		int action;
		size_t columnNr=0;
		const char *name;//,*value;
		size_t nameLength,valueLength;
		char buffer[BUFFERSIZE];
		Basic value;
		enum json_type type;
		//char *key; struct json_object *val;
		lengthSplit sendlength;


		for (int i = 0;; i++) {

			CassUuidGen* uuid_gen = cass_uuid_gen_new();
			CassCluster* cluster = create_cluster(cassandrahosts);
			CassSession* session = cass_session_new();
			CassFuture* close_future = NULL;
			CassError rc = CASS_OK;
			CassFuture* future = NULL;
			CassStatement* statement = NULL;
			const CassResult* result= NULL;
			CassIterator* iterator = NULL;

			if (connect_session(session, cluster) != CASS_OK) {
				//cass_cluster_free(cluster);
				//cass_session_free(session);
				sprintf(buffer,"\nCassandra session error");
				syslog(LOG_INFO, buffer);
				if(!isDaemon) printf("%s",buffer);
				//return NULL;
			}else{

				// Remove 1 item at a time and process it. Blocks if no items are 
				// available to process.
				sprintf(buffer,"thread %lu, loop %d - waiting for item...\n", (long unsigned int)self(), i);
				syslog(LOG_INFO, buffer);
				if(!isDaemon) printf("%s",buffer);
				WorkItem* item = m_queue.remove();
				sprintf(buffer,"thread %lu, loop %d - got one item\n", (long unsigned int)self(), i);
				syslog(LOG_INFO, buffer);
				if(!isDaemon) printf("%s",buffer);
				TCPStream* stream = item->getStream();

				char input[BUFFERSIZE];
				int len;
				while ((len = stream->receive(input, sizeof(input)-1)) > 0 ){
					action=0;	
					json_object *inputjobj = json_tokener_parse(input);
					if(inputjobj!=NULL){
						json_object_object_foreach(inputjobj, key, val) { //Passing through every array element
							if(strcasecmp(key,"cql")==0){
								type = json_object_get_type(val);
								if(!isDaemon) printf("\ntype: %d",type);
								if(type==json_type_string){
									statement = cass_statement_new(json_object_get_string(val), 0);
									if(!isDaemon) printf("  %s: %s",key,json_object_get_string(val));
								}
							}
							if(strcasecmp(key,"action")==0){
								type = json_object_get_type(val);
								if(!isDaemon) printf("\ntype: %d",type);
								if(type==json_type_string){
									if(strcasecmp(json_object_get_string(val),"execute")==0){
										action=1;
									}
									if(strcasecmp(json_object_get_string(val),"next")==0){
										action=2;
									}
									if(strcasecmp(json_object_get_string(val),"quit")==0){
										action=99;
									}
									if(!isDaemon) printf("%s: %s",key,json_object_get_string(val));
								}
							}
						}
					}

					//statement = cass_statement_new((input), 0);
					//action=1;

					switch(action){
						case 1 :	{ //excecute cql
										json_object *jobj = json_object_new_object();
										json_object *jarray = json_object_new_array();
										future = cass_session_execute(session, statement);
										cass_future_wait(future);

										rc = cass_future_error_code(future);
										if (rc != CASS_OK) {
											print_error(future);
										}else{
											result = cass_future_get_result(future);
											iterator = cass_iterator_from_result(result);

											json_object_object_add(jobj,"row_count", json_object_new_int((int)cass_result_row_count(result)));

											input[len] = 0;
											message = new char[strlen(json_object_to_json_string(jobj))+1+4];
											bzero(message,strlen(json_object_to_json_string(jobj))+1+4);//1 for terminating, 4 for adding length
											//strcpy(message,json_object_to_json_string(jobj));
											//strncpy(message,json_object_to_json_string(jobj),strlen(json_object_to_json_string(jobj)));
											sendlength.length=strlen(json_object_to_json_string(jobj))+4;
											message[0]=sendlength.bytes[3];
											message[1]=sendlength.bytes[2];
											message[2]=sendlength.bytes[1];
											message[3]=sendlength.bytes[0];//copy message length
											strncpy(&message[4],json_object_to_json_string(jobj),strlen(json_object_to_json_string(jobj)));

											stream->send(message, sendlength.length);

											if(!isDaemon) printf("\nsend:%lX (0x%X 0x%X 0x%X 0x%X)%s",sendlength.length,(char)message[0],(char)message[1],(char)message[2],(char)message[3], &message[4]);
											if(!isDaemon) printf("\nthread %lu, input '%s' back to the client", (long unsigned int)self(), input);
											delete[] message;
										}
										json_object_put(jarray);
										json_object_put(jobj);

									}
									break;
						case 2 : { //next
										json_object *jobj = json_object_new_object();
										json_object *record = json_object_new_object();
										rc = cass_future_error_code(future);
										if (rc != CASS_OK) {
											print_error(future);
										}else{
											if (cass_iterator_next(iterator)) {
												const CassRow* row = cass_iterator_get_row(iterator);
												for(columnNr=0;columnNr<cass_result_column_count(result);columnNr++){

													cass_result_column_name(result,columnNr,&name,&nameLength);
													//CassValue *CValue= cass_row_get_column(row,columnNr);
													switch(cass_result_column_type(result,columnNr)){
														case CASS_VALUE_TYPE_ASCII:
														case CASS_VALUE_TYPE_VARCHAR:
														case CASS_VALUE_TYPE_TEXT:
															cass_value_get_string(cass_row_get_column(row,columnNr),&value.pstr,&valueLength);
															snprintf(buffer,valueLength+1,"%s",value.pstr);
															break;
														case CASS_VALUE_TYPE_COUNTER:
														case CASS_VALUE_TYPE_BIGINT:
															cass_value_get_int64(cass_row_get_column(row,columnNr),&value.i64);
															sprintf(buffer,"%ld",value.i64);
															break;
														case CASS_VALUE_TYPE_FLOAT:
															cass_value_get_float(cass_row_get_column(row,columnNr),&value.flt);
															sprintf(buffer,"%f",value.flt);
															break;
														case CASS_VALUE_TYPE_DOUBLE:
															cass_value_get_double(cass_row_get_column(row,columnNr),&value.dbl);
															sprintf(buffer,"%lf",value.dbl);
															break;
														case CASS_VALUE_TYPE_INT:
															cass_value_get_int32(cass_row_get_column(row,columnNr),&value.i32);
															sprintf(buffer,"%d",value.i32);
															break;

													}
													if (!isDaemon) printf("\n type: column==%d => %X %s",(int)columnNr,cass_result_column_type(result,columnNr),buffer);
													json_object_object_add(record,name, json_object_new_string(buffer));

												}
											}
											json_object_object_add(jobj,"result", record);

											input[len] = 0;
											message = new char[strlen(json_object_to_json_string(jobj))+1+4];
											bzero(message,strlen(json_object_to_json_string(jobj))+1+4);//1 for terminating, 4 for adding length
											sendlength.length=strlen(json_object_to_json_string(jobj))+4;
											message[0]=sendlength.bytes[3];
											message[1]=sendlength.bytes[2];
											message[2]=sendlength.bytes[1];
											message[3]=sendlength.bytes[0];//copy message length
											strncpy(&message[4],json_object_to_json_string(jobj),strlen(json_object_to_json_string(jobj)));
											stream->send(message, sendlength.length);

											if(!isDaemon) printf("\nsend:%lX %s",sendlength.length, &message[4]);
											if(!isDaemon) printf("\nthread %lu, input '%s' back to the client", (long unsigned int)self(), input);
											delete[] message;
										}
										json_object_put(record);
										json_object_put(jobj);
									}
									break;
						default:
									sprintf(buffer,"\nUnknown action");
									syslog(LOG_INFO, buffer);
									action=99;//Quit connection
									break;
					}

					if(action==99){ //quit
						break;
					}
					if(inputjobj!=NULL)
						json_object_put(inputjobj);
				}
				if(future!=NULL)
					cass_future_free(future);
				if(statement!=NULL)
					cass_statement_free(statement);
				if(result!=NULL)
					cass_result_free(result);
				if(iterator!=NULL)
					cass_iterator_free(iterator);

				delete item; 

				close_future = cass_session_close(session);
				cass_future_wait(close_future);
				cass_future_free(close_future);

				cass_uuid_gen_free(uuid_gen);
			}
			cass_cluster_free(cluster);
			cass_session_free(session);

		}

		// Should never get here
		return NULL;
	}
};

int main(int    argc, char** argv) {
	int portnumber = 0;
	char *cassandrahosts = NULL;
	char *ipnumber = NULL;
	//int index;
	int option;
	int workers=5;
	pid_t pid,sid;
	char buffer[100];

	isDaemon=0;


	while ((option = getopt (argc, argv, "dh:w:p:c:")) != -1){
		switch (option)
		{
			case 'd':
				isDaemon = 1;
				break;
			case 'w':
				workers = atoi(optarg);
				break;
			case 'h':
				ipnumber = optarg;
				break;
			case 'p':
				portnumber = atoi(optarg);
				break;
			case 'c':
				cassandrahosts = optarg;
				break;
			case '?':
				if (optopt == 'c')
					fprintf (stderr, "Option -%c requires an argument.\n", optopt);
				print_usage ();
				return 1;
			default:
				print_usage ();
				return 1;
		}
	}
	if((cassandrahosts==NULL) || (strlen(cassandrahosts)==0)){
		print_usage ();
		return 1;
	}

	//Set our Logging Mask and open the Log
	setlogmask(LOG_UPTO(LOG_NOTICE));
	openlog(DAEMON_NAME, LOG_CONS | LOG_NDELAY | LOG_PERROR | LOG_PID, LOG_USER);

	syslog(LOG_INFO, "cassandra-proxy start as Daemon");


	if(isDaemon){
		// Fork off the parent process
		pid = fork();
		if (pid < 0) {
			exit(EXIT_FAILURE);
		}
		// If we got a good PID, then we can exit the parent process. 
		if (pid > 0) {
			exit(EXIT_SUCCESS);
		}
		// Create a new SID for the child process 
		sid = setsid();
		if (sid < 0) {
			// Log any failure 
			exit(EXIT_FAILURE);
		}
		// Close out the standard file descriptors
		close(STDIN_FILENO);
		close(STDOUT_FILENO);
		close(STDERR_FILENO);
	}

	// Create the queue and consumer (worker) threads
	wqueue<WorkItem*>  queue;
	for (int i = 0; i < workers; i++) {
		ConnectionHandler* handler = new ConnectionHandler(queue,cassandrahosts);
		if (!handler) {
			sprintf(buffer,"Could not create ConnectionHandler %d\n", i);
			syslog(LOG_INFO, buffer);
			exit(1);
		} 
		handler->start();
	}

	// Create an acceptor then start listening for connections
	WorkItem* item;
	TCPAcceptor* connectionAcceptor;
	if (strlen(ipnumber) > 0) {
		connectionAcceptor = new TCPAcceptor(portnumber, ipnumber);
	}
	else {
		connectionAcceptor = new TCPAcceptor(portnumber);        
	}                                        
	if (!connectionAcceptor || connectionAcceptor->start() != 0) {
		sprintf(buffer,"Could not create an connection acceptor\n");
		syslog(LOG_INFO, buffer);
		exit(1);
	}

	// Add a work item to the queue for each connection
	while (1) {
		TCPStream* connection = connectionAcceptor->accept(); 
		if (!connection) {
			sprintf(buffer,"Could not accept a connection\n");
			syslog(LOG_INFO, buffer);
			continue;
		}
		item = new WorkItem(connection);
		if (!item) {
			sprintf(buffer,"Could not create work item a connection\n");
			syslog(LOG_INFO, buffer);
			continue;
		}
		queue.add(item);
	}

	return 0;
}
