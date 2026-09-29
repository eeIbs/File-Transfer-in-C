#ifndef SEND_DATA_H
#define SEND_DATA_H

#include <stdint.h>

#include "../../common.h"
#include "send_context.h"

#define MAX_PAYLOAD_SIZE 4096


typedef struct send_msg_hdr {

    uint8_t msg_type;
    uint16_t payload_length;

} send_msg_hdr;

typedef struct file_start_msg{
    
    send_msg_hdr msg_header;

    char filename[256];
    char extension[16];
    uint64_t filesize;

} file_start_msg;

typedef struct file_data_msg{
    
    send_msg_hdr msg_header;

    uint8_t payload[MAX_PAYLOAD_SIZE];

} file_data_msg;

typedef struct file_end_msg {
    
    send_msg_hdr msg_header;

} file_end_msg;


DWORD WINAPI send_thread(LPVOID thread_args);

int send_data(void *raw_msg, send_context *send_ctx);


#endif