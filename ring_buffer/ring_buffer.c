/*
 * Ring Buffer Convention
 *
 * front points to the oldest element currently stored in the buffer.
 * rear points to the next index available for enqueue.
 *
 * Enqueue:
 *   - If the buffer is full, the producer waits on the not_full
 *     condition variable.
 *   - The producer continues when space is available.
 *   - The new element is stored at the index pointed to by rear.
 *   - rear is then advanced to the next available index.
 *   - The producer wakes a consumer that waits on the not_empty
 *     condition variable.
 *
 * Dequeue:
 *   - If the buffer is empty, the consumer waits on the not_empty
 *     condition variable.
 *   - The consumer continues when an element is available.
 *   - The element at front is returned.
 *   - front is then advanced.
 *   - This removes the oldest element from the buffer.
 *   - The consumer wakes a producer that waits on the not_full
 *     condition variable.
 *
 * Condition variables:
 *   - not_empty indicates that a consumer can check for available data.
 *   - not_full indicates that a producer can check for available space.
 *   - A condition variable does not store the buffer state.
 *   - isEmpty() and isFull() check the actual buffer state.
 *   - WakeConditionVariable() only wakes a waiting thread.
 *   - The thread checks the buffer state again after it wakes.
 *
 * Circular indexing:
 *   - The modulo (%) operator is used when front or rear is advanced.
 *   - This returns the index to 0 after the last buffer index.
 *   - This makes the array operate as a circular buffer.
 *
 * Example (QUEUE_CAPACITY = 32):
 *   rear = 31
 *   (rear + 1) % 32 = 0
 *
 *   The next enqueue therefore uses index 0.
 *
 * Buffer behavior:
 *   - The buffer does not discard elements when it is full.
 *   - The producer waits until the consumer removes an element.
 *   - The consumer waits until the producer adds an element.
 */


#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include "ring_buffer.h"


void ring_buffer_init(ring_buffer* q){
    
    q->front = 0;
    q->rear = 0;

    InitializeConditionVariable(&q->not_empty);
    InitializeConditionVariable(&q->not_full);
    InitializeCriticalSection(&q->lock);

    q->producer_failure = false;
    q->consumer_failure = false;

}


// Frees any items that are still in the queue, then releases the lock.
// destroy function must only be called when no thread is using the ring buffer.
void ring_buffer_destroy(ring_buffer* q) {
    
    void *trash;

    while(!isEmpty(q)) {
        dequeue(q, &trash);
        free(trash);
    }

    DeleteCriticalSection(&q->lock);

}


bool isEmpty(ring_buffer* q){

    return q->front == q->rear;

}

bool isFull(ring_buffer* q){

    return (q->rear + 1)%QUEUE_CAPACITY == q->front;

}

/*
 * enqueue()
 *
 * Stores the supplied pointer.
 *
 * Ownership of the pointed-to object is transferred to the consumer.
 *
 * enqueue() never copies the object.
 *
 * Rear is incremented.
 */
rb_enqueue_status enqueue(ring_buffer *q, void *data_pointer){

    EnterCriticalSection(&q->lock);

    while(isFull(q) && !q->consumer_failure){

        SleepConditionVariableCS(
            &q->not_full,
            &q->lock, 
            INFINITE);
        
    }

    // If consumer fails, no point in enqueueing more messages, immediately report.
    if (q->consumer_failure) {
        LeaveCriticalSection(&q->lock);
        return ENQUEUE_CNSMR_FAIL;
    }

    q->event_pointer_buffer[(q->rear)%(QUEUE_CAPACITY)] = data_pointer;
    q->rear = (q->rear + 1) % QUEUE_CAPACITY;

    WakeConditionVariable(&q->not_empty);

    LeaveCriticalSection(&q->lock);

    return ENQUEUE_OK;

}

/*
 * Ownership of the pointed-to object is transferred to the consumer.
 */
rb_dequeue_status dequeue(ring_buffer *q, void **output){

    EnterCriticalSection(&q->lock);

    while(isEmpty(q) && !q->producer_failure){

        SleepConditionVariableCS(
            &q->not_empty, 
            &q->lock, 
            INFINITE);

    }

    if (q->producer_failure && isEmpty(q)) {
        LeaveCriticalSection(&q->lock);
        return DEQUEUE_PRDCR_FAIL;
    }
    
    void *data = q->event_pointer_buffer[(q->front)%(QUEUE_CAPACITY)];
    q->front = (q->front + 1)%QUEUE_CAPACITY;

    *output = data;

    WakeConditionVariable(&q->not_full);

    LeaveCriticalSection(&q->lock);

    return DEQUEUE_OK;

}

void ring_buffer_producer_failure(ring_buffer *q) {
    
    EnterCriticalSection(&q->lock);

    q->producer_failure = true;

    WakeAllConditionVariable(&q->not_empty);
    WakeAllConditionVariable(&q->not_full);

    LeaveCriticalSection(&q->lock);

}

void ring_buffer_consumer_failure(ring_buffer *q) {

    EnterCriticalSection(&q->lock);

    q->consumer_failure = true;

    WakeAllConditionVariable(&q->not_empty);
    WakeAllConditionVariable(&q->not_full);

    LeaveCriticalSection(&q->lock);

}