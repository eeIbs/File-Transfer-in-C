#include "../../common.h"
#include "../../ring_buffer/ring_buffer.h"
#include "recv_data.h"
#include "recv_context.h"
#include "../../error_handling/error_print.h"
#include "../msg_type.h"
#include <stdio.h>

#define RECV_DATA_FAILURE_MSG "Failure - recv_data.c"


DWORD WINAPI recv_thread(LPVOID thread_arg) {

 recv_context *recv_ctx = (recv_context *)thread_arg;
    
    while (recv_ctx->FIN == false) {
        if (recv_data(recv_ctx) != 0) {
            error_printer(RECV_DATA_FAILURE_MSG, "recv_data thread failed!\n");
            recv_ctx->FIN = true;
        }
    }

    return 0;

}

int recv_data(recv_context *recv_ctx) {

    recvd_msg_hdr msg_hdr;

    // size_t is used as sizeof() return size_t.
    size_t hdr_bytes_recvd = 0;
                
    size_t msg_hdr_size = sizeof(msg_hdr);

    uint8_t hdr_buffer[sizeof(msg_hdr)];

    // Get enough bytes to check payload length.
    while (hdr_bytes_recvd < msg_hdr_size) {

        // Write recv data into payload_length from the given index point.
        // Also receive only the remaining amount as index represents how much data is already received.
        int recv_result = recv(recv_ctx->connection_socket, 
            hdr_buffer + hdr_bytes_recvd, 
            msg_hdr_size - hdr_bytes_recvd, 
            0);
                    
        if (recv_result == 0) {
            error_printer(RECV_DATA_FAILURE_MSG, "Connection was terminated!\n");
            return -1;
        }

        if (recv_result == SOCKET_ERROR) {
            error_printer(RECV_DATA_FAILURE_MSG, "Lost connection!\n");
            printf("error code: %d\n", WSAGetLastError());
            return -1;
        }

        hdr_bytes_recvd += recv_result;

    }

    // Copy the contents of hdr_bytes_recvd into the msg_header struct.
    memcpy(
        &msg_hdr,
        hdr_buffer,
        sizeof(msg_hdr)
    );

    // Now we have received the entirety of packet header.
    // Interpretation of bytes is different according to different packet types.
    switch (msg_hdr.msg_type) {

        default:
            error_printer(RECV_DATA_FAILURE_MSG, "Packet of unknown `msg_header.msg_type` received.\n");
            printf("%u\n", msg_hdr.msg_type);
            return -1;


        case FILE_START_MSG: {

            recvd_start_msg start_msg;
            start_msg.msg_header = msg_hdr;
            
            // Initializing total_pkt_bytes_recvd with sizeof(msg_hdr) 
            // as msg_hdr is already received properly.
            int start_payload_recvd = 0;
            int expected_payload_size = msg_hdr.payload_length;

            while (start_payload_recvd < expected_payload_size) {

                int recv_result = recv(recv_ctx->connection_socket, 
                    (uint8_t *)&start_msg + sizeof(msg_hdr) + start_payload_recvd, 
                    msg_hdr.payload_length - start_payload_recvd,
                    0);

                if (recv_result == 0) {
                    error_printer(RECV_DATA_FAILURE_MSG, "Connection was terminated!\n");
                    return -1;
                }

                if (recv_result == SOCKET_ERROR) {
                    error_printer(RECV_DATA_FAILURE_MSG, "Lost connection!\n");
                    return -1;
                }

                start_payload_recvd += recv_result;

            }

            recvd_start_msg *start_msg_cpy = malloc(sizeof(*start_msg_cpy));

            if (start_msg_cpy == NULL) {
                error_printer(RECV_DATA_FAILURE_MSG, "Memory allocation for received start packet failed before enqueue!\n");
                return -1;
            }

            *start_msg_cpy = start_msg;
            
            // MAKE SURE CONSUMER ALWAYS USES `free(start_msg_cpy);` AFTER USE.
            enqueue(recv_ctx->ring_buffer, start_msg_cpy);
            
            break;
        }

        case FILE_DATA_MSG: {

            // Ensure length does not exceed max payload size.
            if (msg_hdr.payload_length > MAX_PAYLOAD_SIZE) {
                error_printer(RECV_DATA_FAILURE_MSG, "payload_length exceeds predefined max payload size\n");
                return -1;
            }

            recvd_data_msg recvd_msg;
            recvd_msg.msg_header = msg_hdr;

            int data_payload_recvd = 0;
            int expected_payload_size = msg_hdr.payload_length;

            while (data_payload_recvd < expected_payload_size) {

                int recv_result = recv(recv_ctx->connection_socket, 
                    recvd_msg.payload + data_payload_recvd, 
                    expected_payload_size - data_payload_recvd,
                    0);

                if (recv_result == 0) {
                    error_printer(RECV_DATA_FAILURE_MSG, "Connection was terminated!\n");
                    return -1;
                }

                if (recv_result == SOCKET_ERROR) {
                    error_printer(RECV_DATA_FAILURE_MSG, "Lost connection!\n");
                    return -1;
                }

                data_payload_recvd += recv_result;
            }
            
            recvd_data_msg *data_pkt_cpy = malloc(sizeof(*data_pkt_cpy));

            if (data_pkt_cpy == NULL) {
                error_printer(RECV_DATA_FAILURE_MSG, "Memory allocation for received data packet failed before enqueue!\n");
                return -1;
            }

            *data_pkt_cpy = recvd_msg;

            // MAKE SURE CONSUMER ALWAYS USES `free(data_pkt_cpy);` AFTER USE.
            enqueue(recv_ctx->ring_buffer, data_pkt_cpy);

            break;
        }

        case FILE_END_MSG: {

            // Ensure payload_length is exactly 0, sender will always send 0.
            if (msg_hdr.payload_length != 0) {
                error_printer(RECV_DATA_FAILURE_MSG, "payload_length must be 0\n");
                return -1;
            }

            recvd_data_msg recvd_msg;
            recvd_msg.msg_header = msg_hdr;

            recvd_data_msg *data_pkt_cpy = malloc(sizeof(*data_pkt_cpy));
            if (data_pkt_cpy == NULL) {
                error_printer(RECV_DATA_FAILURE_MSG, "Memory allocation for received end packet failed before enqueue!\n");
                return -1;
            }

            *data_pkt_cpy = recvd_msg;

            enqueue(recv_ctx->ring_buffer, data_pkt_cpy);
            
            recv_ctx->FIN = true;

            break;
        }
    }

    return 0;

}

