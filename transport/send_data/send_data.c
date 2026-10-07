#include "../../common.h"
#include "../msg_type.h"
#include "send_data.h"
#include "send_context.h"
#include "../msg_type.h"
#include "../../ring_buffer/ring_buffer.h"
#include "../../error_handling/error_print.h"
#include <stdio.h>

#define SEND_DATA_ERR "Failure - send_data.c"

DWORD WINAPI send_thread(LPVOID thread_args) {

    send_context *ctx = (send_context *)thread_args;
    
    while (!ctx->FIN) {
        
        void *raw_msg; 
        
        rb_dequeue_status dq_status = dequeue(ctx->ring_buffer, &raw_msg);

        if (dq_status == DEQUEUE_PRDCR_FAIL) {
            error_printer(SEND_DATA_ERR, "Producer thread failed, entire file might not have sent.\n");
            return EXIT_FAILURE;
        }
        
        if (send_data(raw_msg, ctx) != 0) {
            error_printer(SEND_DATA_ERR, "send_data() function failed.\n");

            ring_buffer_consumer_failure(ctx->ring_buffer);

            ctx->FIN = true;
        }

        free(raw_msg);

    }

    return EXIT_SUCCESS;

}


int send_data(void *raw_msg, send_context *send_ctx) {

    SOCKET send_socket = send_ctx->connection_socket;
    
    send_msg_hdr *hdr = raw_msg;

    size_t msg_length = sizeof(send_msg_hdr) + hdr->payload_length; 

    switch(hdr->msg_type) {
        case FILE_START_MSG:
        {   
            int send_result = send(send_socket, (const char *)hdr, msg_length, 0);
            if(send_result == SOCKET_ERROR) {
                error_printer(SEND_DATA_ERR, "send() failed in case FILE_START_MSG.\n");
                return -1;
            }
            break;
        }
        case FILE_DATA_MSG:
        {   

            int send_result = send(send_socket, (const char *)hdr, msg_length, 0);
            if(send_result == SOCKET_ERROR) {
                error_printer(SEND_DATA_ERR, "send() failed in case FILE_DATA_MSG.\n");
                return -1;
            }
            break;
        }
        case FILE_END_MSG:
        {
            printf("Sending FILE_END_MSG\n");
            int send_result = send(send_socket, (const char *)hdr, msg_length, 0);
            if(send_result == SOCKET_ERROR) {
                error_printer(SEND_DATA_ERR, "send() failed in case FILE_END_MSG.\n");
                return -1;
            }
            send_ctx->FIN = true;
            break;    
        }
    }

    return 0;

}
