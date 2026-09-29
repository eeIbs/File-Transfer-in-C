#include <stdio.h>
#include "../ring_buffer/ring_buffer.h"
#include "../transport/send_data/send_data.h"
#include "../transport/recv_data/recv_data.h"
#include "../transport/recv_data/recv_context.h"
#include "../transport/msg_type.h"


void sender_cycle_test() {
    recv_context context;

    SOCKET sock;
    unsigned short echoServPort = 7;
    char *server_IP = "127.0.0.1";
    struct sockaddr_in echoServAddr;
    WSADATA wsaData;

    ring_buffer rb;

    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);

    if (result != 0) {
        printf("WSAStartup() failed: %d\n", result);
        return;
    }

    sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

    if(sock == INVALID_SOCKET){
        printf("socket() error: %d\n", WSAGetLastError());
    }

    memset(&echoServAddr, 0, sizeof(echoServAddr));
    echoServAddr.sin_family = AF_INET;
    echoServAddr.sin_addr.s_addr = inet_addr(server_IP);
    echoServAddr.sin_port = htons(echoServPort);

    if(connect(sock, (struct sockaddr *)&echoServAddr, sizeof(echoServAddr)) == SOCKET_ERROR){
        printf("connect() failed: %d\n", WSAGetLastError());
        closesocket(sock);
        WSACleanup();
    }

    ring_buffer_init(&rb);

    context.connection_socket = sock;
    context.ring_buffer = &rb;
    context.sender_ip = server_IP;

    CreateThread(
        NULL,
        0,
        recv_thread,
        &context,
        0,
        NULL
    );

    file_start_msg start_msg;
    // Populating start_msg

    start_msg.msg_header.msg_type = FILE_START_MSG;
    start_msg.msg_header.payload_length = sizeof(start_msg) - sizeof(send_msg_hdr);

    strncpy(start_msg.filename, "echo_test_file", sizeof(start_msg.filename) - 1);
    start_msg.filename[sizeof(start_msg.filename) - 1] = '\0';

    strncpy(start_msg.extension, ".txt", sizeof(start_msg.extension) - 1);
    start_msg.extension[sizeof(start_msg.extension) - 1] = '\0';

    start_msg.filesize = 11;

    file_data_msg data_msg;
    // Populating data_msg
    
    const char *test_data = "Hello World";

    data_msg.msg_header.msg_type = FILE_DATA_MSG;
    data_msg.msg_header.payload_length = strlen(test_data);

    strncpy(data_msg.payload, test_data, strlen(test_data));

    // Second data message.
    file_data_msg data_msg2;
    // Populating data_msg
    
    const char *test_data2 = "Second Line!";

    data_msg2.msg_header.msg_type = FILE_DATA_MSG;
    data_msg2.msg_header.payload_length = strlen(test_data2);

    strncpy(data_msg2.payload, test_data2, strlen(test_data2));

    file_end_msg end_msg;
    //Populating end_msg

    end_msg.msg_header.msg_type = FILE_END_MSG;
    end_msg.msg_header.payload_length = 0;


    Sleep(100);   // temporary, just so receiver thread is running

    send(sock, (char *)&start_msg, sizeof(start_msg), 0);

    send(sock,
        (char *)&data_msg,
        sizeof(send_msg_hdr) + data_msg.msg_header.payload_length,
        0);

    send(sock,
        (char *)&data_msg2,
        sizeof(send_msg_hdr) + data_msg2.msg_header.payload_length,
        0);

    send(sock, (char *)&end_msg, sizeof(end_msg), 0);

    while (!context.FIN) {
        Sleep(10);
    }

    printf("\n");
    printf("============================================================\n");
    printf("                  RECEIVE DATA TEST\n");
    printf("============================================================\n");

    printf("\n[ RECEIVE STATUS ]\n");
    printf("FIN                 : %s\n",
        context.FIN ? "TRUE" : "FALSE");

    printf("\n[ RING BUFFER ]\n");
    printf("Front               : %d\n", rb.front);
    printf("Rear                : %d\n", rb.rear);

    printf("\n------------------------------------------------------------\n");
    printf("                    RECEIVED MESSAGES\n");
    printf("------------------------------------------------------------\n");

    int message_count = 0;

    while (rb.front != rb.rear) {

        void *item = NULL;

        item = dequeue(&rb);

        if (item == NULL) {
            printf("ERROR: dequeue() returned NULL\n");
            break;
        }

        recvd_msg_hdr *header = (recvd_msg_hdr *)item;

        message_count++;

        printf("\nMESSAGE %d\n", message_count);
        printf("Type                : ");

        switch (header->msg_type) {

            case FILE_START_MSG:
            {
                recvd_start_msg *start_msg = (recvd_start_msg *)item;

                printf("FILE_START_MSG\n");
                printf("Payload length      : %u bytes\n",
                    header->payload_length);
                printf("Filename            : %s\n",
                    start_msg->file_name);
                printf("Extension           : %s\n",
                    start_msg->file_extension);
                printf("Filesize            : %llu bytes\n",
                    (unsigned long long)start_msg->file_size);

                printf("Status              : RECEIVED\n");

                free(start_msg);
                break;
            }

            case FILE_DATA_MSG:
            {
                recvd_data_msg *data_msg = (recvd_data_msg *)item;

                printf("FILE_DATA_MSG\n");
                printf("Payload length      : %u bytes\n",
                    header->payload_length);

                printf("Payload             : \"");
                fwrite(data_msg->payload,
                    1,
                    header->payload_length,
                    stdout);
                printf("\"\n");

                printf("Status              : RECEIVED\n");

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
    printf("Expected sequence    : FILE_START -> FILE_DATA -> ...\n");
    printf("Queued messages      : %d\n", message_count);
    printf("FILE_END queued      : NO\n");
    printf("FIN triggered        : %s\n",
        context.FIN ? "YES" : "NO");

    if (context.FIN && message_count == 2) {
        printf("\nReceive cycle        : PASS\n");
    } else {
        printf("\nReceive cycle        : FAIL\n");
    }

    printf("\n============================================================\n");

}

int main() {

    sender_cycle_test();

}

/*
Python ECHO server command.
============================

python -c "
import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 7))
s.listen(5)
print('Echo server running on port 7')
while True:
    conn, addr = s.accept()
    while True:
        data = conn.recv(4096)
        if not data:
            break
        conn.sendall(data)
    conn.close()
"


*/