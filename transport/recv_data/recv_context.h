#ifndef RECV_CONTEXT_H
#define RECV_CONTEXT_H

#include <stdint.h>
#include "../../ring_buffer/ring_buffer.h"
#include "../../common.h"

#define MAX_PAYLOAD_SIZE 4096

typedef struct recv_context{
    
    ring_buffer *ring_buffer;
    
    SOCKET connection_socket;
    
    char sender_ip[INET_ADDRSTRLEN];
    int sender_port;

    bool FIN;

    bool conn_terminated;

} recv_context;


#endif