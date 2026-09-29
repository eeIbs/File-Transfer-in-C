#include <stdio.h>
#include "../ring_buffer/ring_buffer.h"
#include "../transport/send_data/send_data.h"
#include "../transport/send_data/send_context.h"
#include "../transport/msg_type.h"

#include "../file_reader/file_reader.h"


void sender_cycle_test() {

    WSADATA wsaData;

    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);

    if (result != 0) {
        printf("WSAStartup() failed: %d\n", result);
        return;
    }

    SOCKET send_sock;
    unsigned short recvr_port = 27015;
    char *recvr_IP = "127.0.0.1";
    struct sockaddr_in recvr_addr;
    

    send_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

    if(send_sock == INVALID_SOCKET){
        printf("socket() error: %d\n", WSAGetLastError());
    }

    memset(&recvr_addr, 0, sizeof(recvr_addr));
    recvr_addr.sin_family = AF_INET;
    recvr_addr.sin_addr.s_addr = inet_addr(recvr_IP);
    recvr_addr.sin_port = htons(recvr_port);

    if(connect(send_sock, (struct sockaddr *)&recvr_addr, sizeof(recvr_addr)) == SOCKET_ERROR){
        printf("connect() failed: %d\n", WSAGetLastError());
        closesocket(send_sock);
        WSACleanup();
    }

    ring_buffer rb;
    ring_buffer_init(&rb);

    file_reader_ctx reader_ctx;
    reader_ctx.ring_buffer = &rb;

    CreateThread(
        NULL,
        0,
        file_reader_thread,
        &reader_ctx,
        0,
        NULL
    );

    send_context context;

    context.connection_socket = send_sock;
    context.ring_buffer = &rb;
    context.recvr_ip = recvr_IP;

    CreateThread(
        NULL,
        0,
        send_thread,
        &context,
        0,
        NULL
    );

    while (!context.FIN) {
        Sleep(10);
    }

    printf("\n");
    printf("============================================================\n");
    printf("                       SEND TEST\n");
    printf("============================================================\n");

    printf("\n[ CONNECTION ]\n");
    printf("Receiver IP          : %s\n", recvr_IP);
    printf("Receiver port        : %u\n", recvr_port);
    printf("Connection status    : CONNECTED\n");

    printf("\n[ THREADS ]\n");
    printf("File reader          : STARTED\n");
    printf("Send thread          : STARTED\n");

    printf("\n[ RING BUFFER ]\n");
    printf("Front                : %d\n", rb.front);
    printf("Rear                 : %d\n", rb.rear);

    printf("\n[ TRANSFER ]\n");
    printf("Transfer status      : %s\n",
        context.FIN ? "COMPLETED" : "FAILED");

    printf("\n------------------------------------------------------------\n");

    printf("\n[ TEST RESULT ]\n");
    printf("FIN triggered        : %s\n",
        context.FIN ? "YES" : "NO");

    if (context.FIN) {
        printf("\nSend cycle           : PASS\n");
    } else {
        printf("\nSend cycle           : FAIL\n");
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