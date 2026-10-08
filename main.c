/*
 *
 *
 * 
 * 
 * 
 * */


#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include "common.h"
#include "ring_buffer/ring_buffer.h"
#include "file_reader/file_reader.h"
#include "file_reconstructor/file_reconstructor.h"
#include "transport/recv_data/recv_data.h"
#include "transport/recv_data/recv_context.h"
#include "transport/send_data/send_data.h"
#include "transport/send_data/send_context.h"
#include "transport/connection/connection_init.h"
#include "error_handling/error_print.h"


#define MAIN_FAILURE_MSG "Failure - main.c"
#define MAIN_SEND_FAILURE_MSG "Failure - main.c - sender side"
#define MAIN_RECV_FAILURE_MSG "Failure - main.c - receiver side"

#define FILE_TO_SEND_PATH "./file_to_send/"


int main() {

    printf(
        "**\n"
        " *      _____ _ _                _____                     __           \n"
        " *     |  ___(_) | ___          |_   _| __ __ _ _ __  ___ / _| ___ _   __  \n"
        " *     | |_  | | |/ _ \\   _____   | || '__/ _` | '_ \\/ __| |_ / _ \\\\ '__| \n"
        " *     |  _| | | |  __/  |_____|  | || | | (_| | | | \\__ \\  || __/  | |  \n"
        " *     |_|   |_|_|\\___|           |_||_|  \\__,_|_| |_|___/_|  \\___| |_|    \n"
        " *                                                                      \n"
        "**\n\n\n"
    );

    printf("Do you want to send a file or receive a file?\n");
    printf("1. Send a file.\n");
    printf("2. Receive a file.\n");

    int user_choice;
    scanf("%d", &user_choice);

    switch(user_choice) {
        
        default:
            printf("Please select a valid option from the specified options.\n");
            return EXIT_FAILURE;

        case 1:
        {
            printf("Scanning directory: ./file_to_send.\n");

            DIR *send_dir = opendir(FILE_TO_SEND_PATH);
            if (send_dir == NULL) {
                error_printer(MAIN_FAILURE_MSG, "Unable to open file_to_send directory!\n");
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
                error_printer(MAIN_FAILURE_MSG, "No files to send found!\n");
                closedir(send_dir);
                return EXIT_FAILURE;
            }

            printf("Found file: %s\n", send_file->d_name);
            printf("Continue to send? [y/n]\n");
            
            char send_confirmation;
            scanf(" %c", &send_confirmation);

            if (send_confirmation == 'y' || send_confirmation == 'Y') {

                printf("The system will now attempt a connection with the receiver.\nIP address and Port must be provided by user.\n");

                char recvr_address[64];
                printf("Enter receivers address: ");
                scanf("%s", &recvr_address);

                int recvr_port;
                printf("Enter receivers port: ");
                scanf("%d", &recvr_port);
                
                // Create and populate connection_ctx.
                connection_context connection_ctx = {0};
                strcpy(connection_ctx.connection_address, recvr_address);
                connection_ctx.connection_port = recvr_port;
                connection_ctx.endpoint_type = 0;

                if(init_connection(&connection_ctx) != 0) {
                    error_printer("Connection initiation failed.\n", "init_connection()");
                    closedir(send_dir);
                    return EXIT_FAILURE;
                }

                SOCKET send_socket = connection_ctx.sock;

                // Begin file_read and send.

                printf("Beginning connection.\n");

                ring_buffer send_rb = {0};
                ring_buffer_init(&send_rb);

                file_reader_ctx reader_ctx = {0};
                reader_ctx.ring_buffer = &send_rb;

                HANDLE reader_thread_handle = CreateThread(
                    NULL,
                    0,
                    file_reader_thread,
                    &reader_ctx,
                    0,
                    NULL
                );

                send_context send_ctx = {0};

                send_ctx.connection_socket = send_socket;
                send_ctx.ring_buffer = &send_rb;
                send_ctx.recvr_ip = recvr_address;
                
                HANDLE send_thread_handle = CreateThread(
                    NULL,
                    0,
                    send_thread,
                    &send_ctx,
                    0,
                    NULL
                );

                WaitForSingleObject(reader_thread_handle, INFINITE);
                WaitForSingleObject(send_thread_handle, INFINITE);

                DWORD reader_exit = 0, send_exit = 0;
                GetExitCodeThread(reader_thread_handle, &reader_exit);
                GetExitCodeThread(send_thread_handle, &send_exit);

                if (reader_exit != EXIT_SUCCESS || send_exit != EXIT_SUCCESS) {
                    error_printer(MAIN_SEND_FAILURE_MSG, "Sender side failed.\n");
                    
                    printf("Stopping sender side.\n");

                    CloseHandle(reader_thread_handle);
                    CloseHandle(send_thread_handle);

                    closesocket(send_socket);
                    WSACleanup();

                    closedir(send_dir);

                    ring_buffer_destroy(&send_rb);
                    
                    return EXIT_FAILURE;
                }

                printf("Sender finished!\n");

                CloseHandle(reader_thread_handle);
                CloseHandle(send_thread_handle);

                closesocket(send_socket);
                WSACleanup();

                closedir(send_dir);

                ring_buffer_destroy(&send_rb);

                return EXIT_SUCCESS;

            }

            else {
                printf("User entered a character other than `y` or `Y`. Exiting.\n");
                closedir(send_dir);
                return EXIT_SUCCESS;
            }
        }

        case 2:
        {
            printf("Initializing receiver.\n");
            
            connection_context recv_connection_ctx = {0};

            recv_connection_ctx.endpoint_type = 1;
            
            printf("Would you like to provide an address for connection? [y/n]\n");

            char recvr_address_confirmation;
            scanf(" %c", &recvr_address_confirmation);

            if(recvr_address_confirmation == 'y' || recvr_address_confirmation == 'Y') {
                printf("Enter the address: \n");
                scanf(" %s", &recv_connection_ctx.bind_address);
            }
            else {
                strcpy(recv_connection_ctx.bind_address, "127.0.0.1");
            }
            
            recv_connection_ctx.bind_port = 27015;


            if(init_connection(&recv_connection_ctx) != 0) {
                error_printer(MAIN_FAILURE_MSG, "Connection initiation failed.\n");
                return EXIT_FAILURE;
            }

            ring_buffer recv_rb = {0};
            ring_buffer_init(&recv_rb);
                
            recv_context recv_ctx = {0};

            // Populate recv_ctx.
            recv_ctx.connection_socket = recv_connection_ctx.sock;
            recv_ctx.ring_buffer = &recv_rb;
            strcpy(recv_ctx.sender_ip, recv_connection_ctx.connection_address);
            recv_ctx.sender_port = recv_connection_ctx.connection_port;
                
            // Receiver thread.
            HANDLE recv_thread_handle = CreateThread(
                NULL,
                0,
                recv_thread,
                &recv_ctx,
                0,
                NULL
            );

            reconstructor_ctx recons_ctx = {0};
            recons_ctx.ring_buffer = &recv_rb;

            // File reconstructor thread.
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
            
            DWORD recv_exit = 0, recons_exit = 0;
            GetExitCodeThread(recv_thread_handle, &recv_exit);
            GetExitCodeThread(recons_thread_handle, &recons_exit);

            if (recv_exit != EXIT_SUCCESS || recons_exit != EXIT_SUCCESS) {
                error_printer(MAIN_RECV_FAILURE_MSG, "Receiver side failed.\n");
                
                printf("Stopping receiver side.\n");

                CloseHandle(recv_thread_handle);
                CloseHandle(recons_thread_handle);

                closesocket(recv_connection_ctx.sock);
                WSACleanup();

                ring_buffer_destroy(&recv_rb);
                
                return EXIT_FAILURE;
            }


            printf("Receiver finished.\n");
            printf("Received file is in ./received_files \n");

            CloseHandle(recv_thread_handle);
            CloseHandle(recons_thread_handle);

            closesocket(recv_connection_ctx.sock);
            WSACleanup();

            ring_buffer_destroy(&recv_rb);

            return EXIT_SUCCESS;

        }

    }

}