#
CC		= g++
CFLAGS		= -c -Wall
LDFLAGS		= -lpthread -ljson
SOURCES		= main.cpp threads/thread.cpp tcpsockets/tcpacceptor.cpp tcpsockets/tcpstream.cpp
INCLUDES	= -Iwqueue -Itcpsockets -Ithreads -Ijson -ljson
OBJECTS		= $(SOURCES:.cpp=.o)
TARGET		= cassandra-json-proxy

CXXFLAGS = -g3 -O3 -Wall -static 
LDFLAGS = -L/usr/local/lib/ -lpthread -lcassandra -ljson -Ithreads -Iwqueue -Itcpsockets


# -------------

#all: clean $(EXE)                                                                                                                                                                                          
all:  $(SOURCES) $(TARGET)                                                                                                                                                                                             
$(TARGET): $(OBJECTS) 
		$(CC)  $(OBJECTS) -o $@ $(LDFLAGS)
		strip $(TARGET)                                                                                                                                                                                        
.cpp.o:
		$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
		rm -rf $(OBJECTS) $(TARGET)
                                                                                                                                                                                                            
