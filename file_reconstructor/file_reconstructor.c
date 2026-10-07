#include <stdio.h>
#include "file_reconstructor.h"
#include "../transport/recv_data/recv_data.h"
#include "../transport/recv_data/recv_context.h"
#include "../error_handling/error_print.h"
#include "../ring_buffer/ring_buffer.h"
#include "../transport/msg_type.h"


#define FILE_WRITER_FAILURE_MSG "Failure - File reconstructor"

#define FILE_TO_RECV_PATH "./received_files/"


DWORD WINAPI reconstructor_thread(LPVOID thread_args) {

    reconstructor_ctx *ctx = (reconstructor_ctx *)thread_args;

    while (ctx->FIN == false) {
        if (reconstruct_file(ctx->ring_buffer, ctx) != 0) {

            error_printer(FILE_WRITER_FAILURE_MSG, "reconstruct_file() failed. Quitting reconstructor thread.\n");

            ring_buffer_consumer_failure(ctx->ring_buffer);

            ctx->FIN = true;

            return -1;
        }
    }

    printf("Reconstructor thread finished.\n");

    return 0;

}

int reconstruct_file(ring_buffer *ring_buffer, reconstructor_ctx *ctx) {

    static char filename_dot_ext[289];

    // Get the header from the msg that is in queue.
    do {

        void *raw_msg;

        rb_dequeue_status dq_status = dequeue(ring_buffer, &raw_msg);

        if (dq_status == DEQUEUE_PRDCR_FAIL) {
            error_printer(FILE_WRITER_FAILURE_MSG, "producer failed before finishing.\n");
            return -1;
        }
            
        // Interpret the first sizeof(hdr) bytes as header.
        recvd_msg_hdr *pkt_hdr = raw_msg;

        switch (pkt_hdr->msg_type) {
            
            default:

                error_printer(FILE_WRITER_FAILURE_MSG, "Unsupported packet type was received!\n");
                free(raw_msg);
                break;

            case FILE_START_MSG:
            {
            
                recvd_start_msg *start_msg = raw_msg;

                snprintf(filename_dot_ext, 
                    sizeof(filename_dot_ext), 
                    FILE_TO_RECV_PATH "%s.%s", 
                    start_msg->file_name, 
                    start_msg->file_extension
                );

                FILE *file_ptr = fopen(filename_dot_ext, "wb");

                if (file_ptr == NULL) {
                    error_printer(FILE_WRITER_FAILURE_MSG, "File could not be opened!\n");
                    free(raw_msg);
                    return -1;
                }
                
                fclose(file_ptr);
                free(raw_msg);
                break;
            
            }

            case FILE_DATA_MSG:
            {

                recvd_data_msg *data_msg = raw_msg;

                FILE *file_ptr = fopen(filename_dot_ext, "ab");

                if (file_ptr == NULL) {
                    error_printer(FILE_WRITER_FAILURE_MSG, "File could not be opened!\n");
                    free(raw_msg);
                    return -1;
                }

                size_t bytes_written = fwrite(data_msg->payload, 
                    1, 
                    data_msg->msg_header.payload_length, 
                    file_ptr);
                
                if (bytes_written != data_msg->msg_header.payload_length) {
                    error_printer(FILE_WRITER_FAILURE_MSG, "Failed to write complete payload.\n");
                    free(raw_msg);
                    fclose(file_ptr);
                    return -1;
                }


                fclose(file_ptr);
                free(raw_msg);
                
                break;
            }

            case FILE_END_MSG:
            {
                printf("reconstructor found FILE_END_MSG\n");
                ctx->FIN = true;
                free(raw_msg);
                break;
            }

        }
            
    } while (!ctx->FIN);

    return 0;

}

