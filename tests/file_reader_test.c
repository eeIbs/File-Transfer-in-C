#include <stdio.h>
#include <stdlib.h>

#include "../common.h"
#include "../file_reader/file_reader.h"
#include "../ring_buffer/ring_buffer.h"
#include "../transport/send_data/send_data.h"
#include "../transport/msg_type.h"

int main(void) {

    ring_buffer rb;

    ring_buffer_init(&rb);

    printf("BEFORE file_reader\n");
    fflush(stdout);

    file_reader(&rb);

    printf("AFTER file_reader\n");
    fflush(stdout);

    int message_count = 0;


    // Loop to inspect ring_buffer content.
    while (rb.front != rb.rear) {

        void *item = dequeue(&rb);

        if (item == NULL) {
            printf("ERROR: dequeue() returned NULL\n");
            break;
        }

        send_msg_hdr *header = (send_msg_hdr *)item;

        message_count++;

        printf("\nMESSAGE %d\n", message_count);
        printf("Type                : ");

        switch (header->msg_type) {

            case FILE_START_MSG:
            {
                file_start_msg *start_msg = (file_start_msg *)item;

                printf("FILE_START_MSG\n");

                printf("Payload length      : %u bytes\n",
                    header->payload_length);

                printf("Filename            : %s\n",
                    start_msg->filename);

                printf("Extension           : %s\n",
                    start_msg->extension);

                printf("Filesize            : %llu bytes\n",
                    (unsigned long long)start_msg->filesize);

                free(start_msg);

                break;
            }

            case FILE_DATA_MSG:
            {
                file_data_msg *data_msg = (file_data_msg *)item;

                printf("FILE_DATA_MSG\n");

                printf("Payload length      : %u bytes\n",
                    header->payload_length);

                printf("Payload             : \"");

                fwrite(
                    data_msg->payload,
                    1,
                    header->payload_length,
                    stdout
                );

                printf("\"\n");

                free(data_msg);

                break;
            }

            default:
            {
                printf("UNKNOWN MESSAGE (%d)\n",
                    header->msg_type);

                free(item);

                break;
            }
        }
    }

    printf("\n------------------------------------------------------------\n");

    printf("\n[ TEST RESULT ]\n");
    printf("Queued messages      : %d\n", message_count);

    return 0;
}