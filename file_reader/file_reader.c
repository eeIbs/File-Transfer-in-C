/*
 * File reader invariants.
 *
 * 1. Exactly one file exists in FILE_TO_SEND_PATH.
 * 2. file_reader() executes once per transfer.
 * 3. File data is read and enqueued incrementally.
 *    The complete file does not need to fit in the ring buffer.
 *
 * The ring buffer can contain different message sequences
 * during the file transfer.
 *
 * Examples:
 *
 *   [start_msg][data_msg][data_msg]...
 *
 *   [data_msg][data_msg][data_msg]...
 *
 *   [data_msg][end_msg]
 *
 * The exact contents of the ring buffer depend on the
 * producer and consumer execution speed.
 *
 * Each data_msg contains up to MAX_PAYLOAD_SIZE bytes.
 * The final data_msg can contain fewer than MAX_PAYLOAD_SIZE bytes.
 */



#include <stdio.h>
#include <dirent.h>   
#include <string.h>
#include <stdlib.h>

#include "file_reader.h"

#include "../error_handling/error_print.h"
#include "../transport/send_data/send_context.h"
#include "../transport/send_data/send_data.h"
#include "../ring_buffer/ring_buffer.h"
#include "../transport/msg_type.h"

#define FILL_START_MSG_ERR "Failure - File reader - build_start_msg()"
#define FILE_DATA_MSG_ERR "Failure - File reader - build_data_msg()"
#define FILE_END_MSG_ERR "Failure - File reader - build_end_msg()"
#define FILE_READER_ERR "Failure - File reader - File reader cycle"

#define FILE_TO_SEND_PATH "./file_to_send/"


DWORD WINAPI file_reader_thread(LPVOID thread_args) {

    file_reader_ctx *ctx = (file_reader_ctx *)thread_args;

    reader_status status = file_reader(ctx);

    if (status != READER_OK) {

        if (status == READER_CNSMR_FAILURE) {
            error_printer(FILE_READER_ERR, "file_reader_thread forcefully stopped as send_thread failed.\n");
            ctx->FIN = true;

            return EXIT_FAILURE;

        }

        error_printer(FILE_READER_ERR, "file_reader() function failed.\n");

        ctx->FIN = true;

        ring_buffer_producer_failure(ctx->ring_buffer);
        
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;

}

// const is used with file_reader_ctx as we do not want builder func to modify it. 
int build_start_msg(const file_reader_ctx *reader_context, file_start_msg *start_msg) {

    FILE *file_to_send = reader_context->file;
    
    if (!file_to_send) {
        error_printer(FILL_START_MSG_ERR, "File with given filename could not be opened!\n");
        return -1;
    }
        
    start_msg->msg_header.msg_type = FILE_START_MSG;
    
    // Copy filename into start_msg.
    memcpy(start_msg->filename, reader_context->filename, sizeof(start_msg->filename));

    // Copy file externsion into start_msg.
    memcpy(start_msg->extension, reader_context->extension, sizeof(start_msg->extension));
    
    // filesize is not an array so it can be directly assigned.
    start_msg->filesize = reader_context->filesize;

    // payload_length is equal to size of message AFTER the message header.
    start_msg->msg_header.payload_length =
    sizeof(start_msg->filename) +
    sizeof(start_msg->extension) +
    sizeof(start_msg->filesize);

    return 0;

}

// const is used with file_reader_ctx as we do not want builder func to modify it.
int build_data_msg(const file_reader_ctx *reader_context, file_data_msg *data_msg, size_t *bytes_read) {

    FILE *file_to_send = reader_context->file;

    if (!file_to_send) {
        error_printer(FILE_DATA_MSG_ERR, "File with given filename could not be opened!\n");
        return -1;
    }

    data_msg->msg_header.msg_type = FILE_DATA_MSG;

    *bytes_read = fread (
        data_msg->payload,
        1,
        MAX_PAYLOAD_SIZE,
        file_to_send
    );

    if (ferror(file_to_send)) {
        error_printer(FILE_DATA_MSG_ERR, "fread() failed to read file.\n");
        return -1;
    }

    // payload_length is equal to size of message AFTER the message header.
    data_msg->msg_header.payload_length = *bytes_read;

    return 0;

}

// const is used with file_reader_ctx as we do not want builder func to modify it.
int build_end_msg(const file_reader_ctx *reader_context, file_end_msg *end_msg) {

    end_msg->msg_header.msg_type = FILE_END_MSG;
    end_msg->msg_header.payload_length = 0;
    
    printf("End message built\n");
    return 0;
}

/* file_reader() function calls
 * 1. build_start_msg()
 * 2. build_data_msg() (loops through until end of file is reached)
 * 3. build_end_msg() (NOT YET IMPLEMENTED)
 */
reader_status file_reader(file_reader_ctx *reader_context) {

    ring_buffer *rb = reader_context->ring_buffer;

    // Open directory containing file that is to be sent.
    DIR *send_dir = opendir(FILE_TO_SEND_PATH);

    if (send_dir == NULL) {
        error_printer(FILL_START_MSG_ERR, "Unable to open file_to_send directory!\n");
        return READER_FAILURE;
    }

    struct dirent *send_file;

    // readdir() often returns "." and ".." at the beginning.
    // Skip "." and ".." before going forward.
    while ((send_file = readdir(send_dir)) != NULL) {

        if (strcmp(send_file->d_name, ".") == 0 ||
            strcmp(send_file->d_name, "..") == 0)
            continue;
        
        break;
    }

    if (send_file == NULL) {
        error_printer(FILL_START_MSG_ERR, "No files to send found!\n");
        closedir(send_dir);
        return READER_FAILURE;
    }

    const char *file_name_ext = send_file->d_name;
    
    // Get file ext from file_name_ext.
    const char *dot = strrchr(file_name_ext, '.');
    
    if(dot == NULL || dot == file_name_ext) {
        error_printer(FILL_START_MSG_ERR, "Unable to find file extension!\n");
        closedir(send_dir);
        return READER_FAILURE;
    }

    const char *file_ext_ptr = dot + 1;

    // Get file name from file_name_ext.
    char file_name[256];

    size_t name_len = dot - file_name_ext;

    if (name_len >= sizeof(file_name)) 
        name_len = sizeof(file_name) - 1;


    memcpy(file_name, file_name_ext, name_len);
    // NULL-terminate file_name.
    file_name[name_len] = '\0';

    // Wrap filename into reader_context for builder functions.
    strncpy(
        reader_context->filename, 
        file_name, 
        sizeof(reader_context->filename)
    );

    // Ensure filename is NULL-terminated as strncpy does not guarantee termination.
    reader_context->filename[sizeof(reader_context->filename) - 1] = '\0';

    // Wrap extension into the struct for file_start_msg. 
    strncpy(
        reader_context->extension, 
        file_ext_ptr, 
        sizeof(reader_context->extension)
    );

    // Ensure extension is NULL-terminated as strncpy does not guarantee termination.
    reader_context->extension[sizeof(reader_context->extension) - 1] = '\0';

    /* 
     * To get file size, first create a file pointer.
     * Move file pointer to the end of the file.
     * Get current file pointer position.
     */

    char full_file_path[512];

    snprintf(
        full_file_path,
        sizeof(full_file_path),
        "%s%s",
        FILE_TO_SEND_PATH,
        file_name_ext
    );

    FILE *fptr = fopen(full_file_path, "rb");

    if (!fptr) {
        error_printer(FILL_START_MSG_ERR, "File with given filename could not be opened!\n");
        
        closedir(send_dir);
        return READER_FAILURE;
    }

    // Handle error in case seek fails.
    if (fseek(fptr, 0, SEEK_END) != 0) {
        error_printer(FILL_START_MSG_ERR, "Seeking file error during getting file size!\n");
        
        fclose(fptr);
        closedir(send_dir);
        return READER_FAILURE;
    }

    // Get current position.
    long file_size = ftell(fptr);
    if (file_size == -1L) {
        error_printer(FILL_START_MSG_ERR, "Position error during getting file size!\n");
        
        fclose(fptr);
        closedir(send_dir);
        return READER_FAILURE;
    }

    reader_context->filesize = (uint64_t)file_size;

    // Move pointer back to beginning of file.
    if (fseek(fptr, 0, SEEK_SET) != 0) {
        error_printer(FILE_READER_ERR, "fseek() failed to bring file pointer back to start of file.\n");
        
        fclose(fptr);
        closedir(send_dir);
        return READER_FAILURE;
    }

    // Put file pointer into reader_context->
    reader_context->file = fptr;

    // Call build_start_msg.
    file_start_msg *start_msg = malloc(sizeof(*start_msg));

    if (start_msg == NULL) {
        error_printer(FILE_READER_ERR, "malloc() failed for *start_msg.\n");

        fclose(fptr);
        closedir(send_dir);
        free(start_msg);
        return READER_FAILURE;
    }

    
    if (build_start_msg(reader_context, start_msg) != 0) {
            
        fclose(fptr);
        closedir(send_dir);

        free(start_msg);

        return READER_FAILURE;

    }

    rb_enqueue_status enq_status = enqueue(rb, start_msg);

    if (enq_status == ENQUEUE_CNSMR_FAIL) {
        fclose(fptr);
        closedir(send_dir);
        free(start_msg);
        return READER_CNSMR_FAILURE;
    }

    /*
    * Now file_reader reads the contents of the file and loops over build_data_msg.
    */

    size_t total_bytes_framed = 0;

    // Loop over build_data_msg until entire file framed into message.
    do {
        
        file_data_msg *data_msg = malloc(sizeof(*data_msg));
        if (data_msg == NULL) {

            error_printer(FILE_READER_ERR, "malloc() failed for *data_msg.\n");

            fclose(fptr);
            closedir(send_dir);
            return READER_FAILURE;
        }

        size_t bytes_read;

        if (build_data_msg(reader_context, data_msg, &bytes_read) != 0) {

            error_printer(FILE_READER_ERR, "Error in file_reader()\n");
            error_printer(FILE_DATA_MSG_ERR, "build_data_msg() failed.\n");

            fclose(fptr);
            closedir(send_dir);
            free(data_msg);
            return READER_FAILURE;

        }

        if (bytes_read == 0) {
            free(data_msg);
            break;
        }

        total_bytes_framed += bytes_read;

        rb_enqueue_status enq_status = enqueue(rb, data_msg);

        if (enq_status == ENQUEUE_CNSMR_FAIL) {
            fclose(fptr);
            closedir(send_dir);
            free(data_msg);
            return READER_CNSMR_FAILURE;
        }

        
    }
    while (total_bytes_framed < reader_context->filesize);

    // Call build_end_msg func to enqueue the final msg.
    file_end_msg *end_msg = malloc(sizeof(*end_msg));
    if (end_msg == NULL) {

        error_printer(FILE_READER_ERR, "malloc() failed for *end_msg.\n");

        fclose(fptr);
        closedir(send_dir);
        return READER_FAILURE;

    }

    if (build_end_msg(reader_context, end_msg) != 0) {
        
        error_printer(FILE_READER_ERR, "Error in file_reader()\n");
        error_printer(FILE_END_MSG_ERR, "build_end_msg() failed.\n");

        fclose(fptr);
        closedir(send_dir);
        free(end_msg);
        return READER_FAILURE;

    }

    reader_context->FIN = true;


    rb_enqueue_status enq_status = enqueue(rb, end_msg);

    if (enq_status == ENQUEUE_CNSMR_FAIL) {
        fclose(fptr);
        closedir(send_dir);
        free(end_msg);
        return READER_CNSMR_FAILURE;
    }

    fclose(fptr);

    closedir(send_dir);

    return READER_OK;
}