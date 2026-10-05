#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include "../common.h"
#include "stdbool.h"

#define QUEUE_CAPACITY 32

typedef struct{

    void *event_pointer_buffer[QUEUE_CAPACITY];
    
    int front;
    int rear;
    
    CONDITION_VARIABLE not_full;
    CONDITION_VARIABLE not_empty;    
    CRITICAL_SECTION lock;

    bool producer_failure;

} ring_buffer;

typedef enum{
    DEQUEUE_OK, // Returns this value when there is no issue.
    DEQUEUE_PROD_FAIL, // Returns this value when producer fails.
} rb_dequeue_status;


void ring_buffer_destroy(ring_buffer* q);

void ring_buffer_init(ring_buffer* q);

bool isEmpty(ring_buffer* q);

bool isFull(ring_buffer* q);

void enqueue(ring_buffer *q, void *data_pointer);

rb_dequeue_status dequeue(ring_buffer *q, void **output);

void ring_buffer_producer_failure(ring_buffer *q);


#endif