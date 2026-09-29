#include <stdio.h>
#include <winsock2.h>
#include "connection_init.h"
#include "../../error_handling/error_print.h"


#define INITIATE_CONNECTION_ERR "Failure - connection_init.c"


int init_connection(connection_context *ctx) {

    switch(ctx->endpoint_type) {
        
        default:
        {
            error_printer(INITIATE_CONNECTION_ERR, "Connection requested was neither sender or receiver side.\n");
            return -1;
        }

        case 0:
        {
            printf("Your client: sender.\n");

            WSADATA wsaData;

            int result = WSAStartup(MAKEWORD(2, 2), &wsaData);

            if (result != 0) {
                error_printer(INITIATE_CONNECTION_ERR, "WSAStartup failed.");
                printf("error: %d\n", WSAGetLastError());
                return -1;
            }

            struct sockaddr_in recvr_addr;

            ctx->sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

            if(ctx->sock == INVALID_SOCKET){
                error_printer(INITIATE_CONNECTION_ERR, "socket() failed.");
                printf("error: %d\n", WSAGetLastError());
                return -1;
            }

            memset(&recvr_addr, 0, sizeof(recvr_addr));
            recvr_addr.sin_family = AF_INET;
            recvr_addr.sin_addr.s_addr = inet_addr(ctx->connection_address);
            recvr_addr.sin_port = htons(ctx->connection_port);

            if(connect(ctx->sock, (struct sockaddr *)&recvr_addr, sizeof(recvr_addr)) == SOCKET_ERROR){
                error_printer(INITIATE_CONNECTION_ERR, "connect() failed.");
                printf("error: %d\n", WSAGetLastError());
                closesocket(ctx->sock);
                WSACleanup();
                return -1;
            }

            printf("Connected! Preparing to send now.\n");

            return 0;

        }

        case 1:
        {
            printf("Your client: receiver.\n");

            WSADATA wsaData;

            int startup_result = WSAStartup(MAKEWORD(2, 2), &wsaData);

            if (startup_result != 0) {
                error_printer(INITIATE_CONNECTION_ERR, "WSAStartup failed.");
                printf("WSAStartup result: %d\n", startup_result);
                return -1;
            }

            SOCKET listen_sock;
            struct sockaddr_in local_addr;

            listen_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

            if(listen_sock == INVALID_SOCKET){
                error_printer(INITIATE_CONNECTION_ERR, "socket() failed.");
                printf("error: %d\n", WSAGetLastError());
                WSACleanup();
                return -1;
            }

            memset(&local_addr, 0, sizeof(local_addr));
            local_addr.sin_family = AF_INET;
            local_addr.sin_addr.s_addr = inet_addr(ctx->bind_address);
            local_addr.sin_port = htons(ctx->bind_port);
            
            if(bind(listen_sock, (struct sockaddr*)&local_addr, sizeof(local_addr)) == SOCKET_ERROR) {
                error_printer(INITIATE_CONNECTION_ERR, "bind() failed.");
                printf("error: %d\n", WSAGetLastError());
                closesocket(listen_sock);
                WSACleanup();
                return -1;
            }

            if(listen(listen_sock, 5) == SOCKET_ERROR) {
                error_printer(INITIATE_CONNECTION_ERR, "listen() failed.");
                printf("error: %d\n", WSAGetLastError());
                closesocket(listen_sock);
                WSACleanup();
                return -1;
            }

            printf("Listening on IP:port %s:%d\n", ctx->bind_address, ctx->bind_port);

            struct sockaddr_in client_addr;
            int client_addr_len = sizeof(client_addr);

            ctx->sock = accept(listen_sock, (struct sockaddr*)&client_addr, &client_addr_len);
            if (ctx->sock == INVALID_SOCKET) {
                error_printer(INITIATE_CONNECTION_ERR, "accept() failed.");
                printf("error: %d\n", WSAGetLastError());
                closesocket(listen_sock);
                WSACleanup();
                return -1;
            }

            // Close the listen socket.
            closesocket(listen_sock);

            // Convert IP from bytes into IP form. Put it into connection_address.
            inet_ntop(AF_INET, &client_addr.sin_addr, ctx->connection_address, 64); // 64 is the buffer size of ctx->connection_address.

            ctx->connection_port = ntohs(client_addr.sin_port);

            printf("Connection estabilished with: \n");
            printf("IP:port =  %s:%d\n", ctx->connection_address, ctx->connection_port);

            return 0;

        }

    }

    return 0;
}