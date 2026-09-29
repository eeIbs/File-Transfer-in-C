#include <stdio.h>
#include <stdlib.h>

#include "../common.h"
#include "../file_reconstructor/file_reconstructor.h"
#include "../ring_buffer/ring_buffer.h"
#include "../transport/recv_data/recv_data.h"
#include "../transport/msg_type.h"

int main() {

    WSADATA wsaData;

    int sock_startup_result = WSAStartup(MAKEWORD(2, 2), &wsaData);

    if (sock_startup_result != 0) {
        printf("WSAStartup() failed: %d\n", sock_startup_result);
        return -1;
    }

    unsigned short recv_port = 27015;
    char *server_IP = "127.0.0.1";
    struct sockaddr_in echoServAddr;

    SOCKET listen_sock;

    recv_context recv_ctx;

    listen_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

    if(listen_sock == INVALID_SOCKET){
        printf("socket() error: %d\n", WSAGetLastError());
        WSACleanup();
        return -1;
    }

    memset(&echoServAddr, 0, sizeof(echoServAddr));
    echoServAddr.sin_family = AF_INET;
    echoServAddr.sin_addr.s_addr = inet_addr(server_IP);
    echoServAddr.sin_port = htons(recv_port);
    
    if(bind(listen_sock, (struct sockaddr*)&echoServAddr, sizeof(echoServAddr)) == SOCKET_ERROR) {
        printf("bind() error: %d\n", WSAGetLastError());
        closesocket(listen_sock);
        WSACleanup();
        return -1;
    }

    if(listen(listen_sock, 5) == SOCKET_ERROR) {
        printf("listen() error: %d\n", WSAGetLastError());
        closesocket(listen_sock);
        WSACleanup();
        return -1;
    }

    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);

    SOCKET client_sock = accept(listen_sock, (struct sockaddr*)&client_addr, &client_addr_len);
    if (client_sock == INVALID_SOCKET) {
        printf("accept() error: %d\n", WSAGetLastError());
        closesocket(listen_sock);
        WSACleanup();
        return 1;
    }

    printf("Server listening on port 27015...\n");

    ring_buffer recv_rb;
    ring_buffer_init(&recv_rb);

    recv_ctx.connection_socket = client_sock;
    recv_ctx.ring_buffer = &recv_rb;
    strcpy(recv_ctx.sender_ip, server_IP);

    HANDLE recv_thread_handle = CreateThread(
        NULL,
        0,
        recv_thread,
        &recv_ctx,
        0,
        NULL
    );

    reconstructor_ctx recons_ctx;
    recons_ctx.ring_buffer = &recv_rb;

    HANDLE recons_thread_handle = CreateThread(
        NULL,
        0,
        reconstructor_thread,
        &recons_ctx,
        0,
        NULL
    );

    WaitForSingleObject(recv_thread_handle, INFINITE);
    WaitForSingleObject(recons_thread_handle, INFINITE);

}