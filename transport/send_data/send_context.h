#ifndef SEND_CONTEXT_H
#define SEND_CONTEXT_H

#include <stdint.h>

#include "../../ring_buffer/ring_buffer.h"
#include "../../common.h"

typedef struct send_context {

    ring_buffer *ring_buffer;

    SOCKET connection_socket;
    char* recvr_ip;

    bool FIN;

    bool conn_terminated;

} send_context;

#endif