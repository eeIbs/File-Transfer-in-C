#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <stdbool.h>
#include "../common.h"
#include "../file_reader/file_reader.h"
#include "../file_reconstructor/file_reconstructor.h"
#include "../ring_buffer/ring_buffer.h"
#include "../error_handling/error_print.h"


#define DISKBENCH_FAILURE_MSG "Failure - disk_io_bench.c"

#define FILE_TO_SEND_PATH "./file_to_send/"


static long long file_size(const char *path)
{
    WIN32_FILE_ATTRIBUTE_DATA info;
    if (!GetFileAttributesExA(path, GetFileExInfoStandard, &info))
        return -1;
    LARGE_INTEGER sz;
    sz.HighPart = info.nFileSizeHigh;
    sz.LowPart  = info.nFileSizeLow;
    return sz.QuadPart;
}


int main() {

    ring_buffer rb;

    ring_buffer_init(&rb);

    DIR *send_dir = opendir(FILE_TO_SEND_PATH);

    if (send_dir == NULL) {
        error_printer(DISKBENCH_FAILURE_MSG, "Unable to open file_to_send directory!\n");
        return EXIT_FAILURE;
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
        error_printer(DISKBENCH_FAILURE_MSG, "No files to send found!\n");
        closedir(send_dir);
        return EXIT_FAILURE;
    }

    printf("Found file: %s\n", send_file->d_name);
        
    file_reader_ctx reader_ctx = {0};

    reader_ctx.ring_buffer = &rb;

    // File reader thread.
    HANDLE reader_thread_handle = CreateThread(
        NULL,
        0,
        file_reader_thread,
        &reader_ctx,
        0,
        NULL
    );

    if (reader_thread_handle == NULL) {
        error_printer(DISKBENCH_FAILURE_MSG, "reader_thread_handle returned NULL.\n");
        closedir(send_dir);
        return EXIT_FAILURE;
    }

    LARGE_INTEGER frequency, t0, t1;
    QueryPerformanceFrequency(&frequency);

    reconstructor_ctx recons_ctx = {0};
    recons_ctx.ring_buffer = &rb;

    QueryPerformanceCounter(&t0);
    while (!recons_ctx.done) {
        reconstruct_file(&rb, &recons_ctx);
    }
    QueryPerformanceCounter(&t1);

    WaitForSingleObject(reader_thread_handle, INFINITE);

    CloseHandle(reader_thread_handle);

    // Get file size using file name.
    char path[MAX_PATH];
    snprintf(path, sizeof(path), "%s%s", FILE_TO_SEND_PATH, send_file->d_name);

    long long bytes = file_size(path);

    if (bytes == -1) {
        error_printer(DISKBENCH_FAILURE_MSG, "file_size(path) returned an error.\n");
        closedir(send_dir);
        return EXIT_FAILURE;
    }

    closedir(send_dir);

    // Calculate results.
    double sec = (double)(t1.QuadPart - t0.QuadPart) / frequency.QuadPart;
    double mb_s = bytes / sec / 1e6;

    long long recv_file = file_size("received_files\\bench.bin");

    printf("sent=%lld recv=%lld  %.1f MB/s\n", bytes, recv_file, mb_s);

    if (bytes > 0 && bytes == recv_file) {
        printf("PASS\n");
        return 0;
    }

    else{
        printf("FAIL\n");
        return -1;
    }
    return 0;


}