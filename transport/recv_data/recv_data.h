#ifndef RECV_DATA_H
#define RECV_DATA_H

#include <stdint.h>

#include "../../common.h"
#include "recv_context.h"

#define MAX_PAYLOAD_SIZE 4096

typedef struct recvd_msg_hdr {

    uint8_t msg_type;
    uint16_t payload_length;

} recvd_msg_hdr;

typedef struct recvd_start_msg {
    
    recvd_msg_hdr msg_header;

    char file_name[256];
    char file_extension[16];
    uint64_t file_size;

} recvd_start_msg;

typedef struct recvd_data_msg {
    
    recvd_msg_hdr msg_header;

    uint8_t payload[MAX_PAYLOAD_SIZE];

} recvd_data_msg;

typedef struct recvd_end_msg {
    
    recvd_msg_hdr msg_header;

} recvd_end_msg;

typedef enum{
    RECV_OK,
    RECV_FAILURE,
    RECV_CNSMR_FAILURE,
}recv_status;

recv_status recv_data(recv_context *recv_ctx);

DWORD WINAPI recv_thread(LPVOID thread_arg);

#endif