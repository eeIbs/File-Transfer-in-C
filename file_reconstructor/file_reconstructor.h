#ifndef FILE_RECONSTRUCTOR_H
#define FILE_RECONSTRUCTOR_H

#include "../transport/recv_data/recv_data.h"
#include "../error_handling/error_print.h"
#include "../ring_buffer/ring_buffer.h"

typedef struct reconstructor_ctx{

    ring_buffer *ring_buffer;

    bool FIN;

}reconstructor_ctx;

DWORD WINAPI reconstructor_thread(LPVOID thread_args);

int reconstruct_file(ring_buffer *ring_buffer, reconstructor_ctx *ctx);

#endif