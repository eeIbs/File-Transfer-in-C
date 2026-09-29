cmake_minimum_required(VERSION 3.10.0)
project(Socket-File-Transfer VERSION 0.1.0 LANGUAGES C)

#[[
add_executable(Socket-File-Transfer
    
    #main.c

    error_handling/error_print.c

    ring_buffer/ring_buffer.c

    file_reader/file_reader.c

    file_reconstructor/file_reconstructor.c

    transport/send_data/send_data.c
    transport/recv_data/recv_data.c

    tests/recv_data_test.c
    common.h
)

target_link_libraries(Socket-File-Transfer

    ws2_32

)
]]


# For recv_data_test
add_executable(recv_data_test
    tests/recv_data_test.c
    transport/recv_data/recv_data.c
    error_handling/error_print.c
    ring_buffer/ring_buffer.c
)
target_link_libraries(recv_data_test PRIVATE ws2_32)


# For file_reader_test
add_executable(file_reader_test
    tests/file_reader_test.c
    file_reader/file_reader.c
    error_handling/error_print.c
    ring_buffer/ring_buffer.c
)
target_link_libraries(file_reader_test PRIVATE ws2_32)


# For file_reconstructor_test
add_executable(file_reconstructor_test
    tests/file_reconstructor_test.c
    file_reconstructor/file_reconstructor.c
    file_reader/file_reader.c
    error_handling/error_print.c
    ring_buffer/ring_buffer.c
)
target_link_libraries(file_reconstructor_test PRIVATE ws2_32)

# For end-to-end receiver_test
add_executable(full_e2e_sender_test
    tests/full_e2e_sender_test.c
    file_reader/file_reader.c
    error_handling/error_print.c
    ring_buffer/ring_buffer.c
    transport/send_data/send_data.c
)
target_link_libraries(full_e2e_sender_test PRIVATE ws2_32)

# For end-to-end receiver_test
add_executable(full_e2e_receiver_test
    tests/full_e2e_receiver_test.c
    file_reconstructor/file_reconstructor.c
    error_handling/error_print.c
    ring_buffer/ring_buffer.c
    transport/recv_data/recv_data.c
)
target_link_libraries(full_e2e_receiver_test PRIVATE ws2_32)

# For main file_transfer
add_executable(file_transfer
    main.c
    ring_buffer/ring_buffer.c
    file_reader/file_reader.c
    file_reconstructor/file_reconstructor.c
    transport/send_data/send_data.c
    transport/recv_data/recv_data.c
    transport/connection/connection_init.c
    error_handling/error_print.c    
)
target_link_libraries(file_transfer PRIVATE ws2_32)