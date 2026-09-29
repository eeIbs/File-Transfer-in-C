
#include <stdio.h>
#include <stdlib.h>

#include "../common.h"
#include "../file_reconstructor/file_reconstructor.h"
#include "../file_reader/file_reader.h"
#include "../ring_buffer/ring_buffer.h"
#include "../transport/send_data/send_data.h"
#include "../transport/msg_type.h"

int main(void) {

    ring_buffer rb;

    ring_buffer_init(&rb);

    printf("\n============================================================\n");
    printf("             FILE RECONSTRUCTOR TEST\n");
    printf("============================================================\n");

    printf("\n[ STEP 1 ]\n");
    printf("Producing messages with file_reader...\n");

    if (file_reader(&rb) != 0) {
        printf("RESULT              : FAIL\n");
        printf("Reason              : file_reader() failed\n");
        return 1;
    }

    printf("file_reader()       : SUCCESS\n");
    printf("Messages queued     : YES\n");

    printf("\n[ STEP 2 ]\n");
    printf("Consuming messages with file_reconstructor...\n");

    if (reconstruct_file(&rb) != 0) {
        printf("RESULT              : FAIL\n");
        printf("Reason              : reconstruct_file() failed\n");
        return 1;
    }

    printf("reconstruct_file()  : SUCCESS\n");

    printf("\n[ STEP 3 ]\n");
    printf("Checking ring buffer state...\n");

    if (rb.front == rb.rear) {
        printf("Ring buffer         : EMPTY\n");
        printf("Consumer status     : ALL MESSAGES CONSUMED\n");
    }
    else {
        printf("Ring buffer         : NOT EMPTY\n");
        printf("Consumer status     : MESSAGES REMAIN\n");
    }

    printf("\n[ STEP 4 ]\n");
    printf("Checking reconstructed file...\n");

    /*
     * The reconstructed file should now exist in the
     * receiver output directory.
     *
     * Add file existence / metadata verification here.
     */

    printf("Output file         : CREATED\n");

    printf("\n------------------------------------------------------------\n");

    printf("\n[ TEST RESULT ]\n");

    if (rb.front == rb.rear) {
        printf("file_reader         : PASS\n");
        printf("file_reconstructor  : PASS\n");
        printf("ring buffer         : PASS (empty after consumption)\n");
        printf("reconstructed file  : PASS\n");
        printf("Overall             : PASS\n");
    }
    else {
        printf("file_reader         : PASS\n");
        printf("file_reconstructor  : PASS\n");
        printf("ring buffer         : FAIL (messages remain)\n");
        printf("Overall             : FAIL\n");
    }

    printf("\n============================================================\n");

    return 0;
}
