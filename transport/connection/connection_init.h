#ifndef CONNECTION_INIT_H
#define CONNECTION_INIT_H

#include <winsock2.h>
#include "../../common.h"

typedef struct connection_context {

    char bind_address[INET_ADDRSTRLEN];
    int bind_port;
    int endpoint_type;

    char connection_address[INET_ADDRSTRLEN];
    int connection_port;
    
    SOCKET sock;

} connection_context;

int init_connection(connection_context *ctx);

#endif