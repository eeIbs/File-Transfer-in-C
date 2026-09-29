#ifndef FILE_READER_H 
#define FILE_READER_H

#include <stdint.h>
#include <stdio.h> 

#include "../transport/send_data/send_data.h"
#include "../ring_buffer/ring_buffer.h"

typedef struct file_reader_ctx{

    ring_buffer *ring_buffer;

    char file_path[512];
    char filename[256];
    char extension[16];

    FILE *file;
    uint64_t filesize;

    bool FIN;

}file_reader_ctx;


DWORD WINAPI file_reader_thread(LPVOID thread_args);

int build_start_msg(const file_reader_ctx *reader_context, file_start_msg *start_msg);

int build_data_msg(const file_reader_ctx *reader_context, file_data_msg *data_msg, size_t *bytes_read);

int file_reader(file_reader_ctx *reader_context);

#endif