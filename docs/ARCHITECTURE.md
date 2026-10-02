#  A Multithreaded File-Transfer System in C

A multithreaded file-transfer system over TCP, written in C on Winsock2, utilizing producer-consumer pipelines and a bounded ring buffer which handles backpressure.

Core Idea: A single executable that behaves as either end-point and allows files to transfer over TCP while keeping disk I/O separate from network I/O.

Scope: Single file per transfer, Single sender and receiver, No encryption, No pause or resume.

(This document describes the intended design. See the README's Known Issues for where the current code differs.)


```mermaid
flowchart LR
    SF[("./file_to_send/")] --> A["Sender<br/>(mode 1)"]
    A -- "TCP byte stream<br/>connect() to :27015" --> B["Receiver<br/>(mode 2, listens)"]
    B --> RF[("./received_files/")]
```


### Sender Side
Arrows show which thread calls into which component.

```mermaid
flowchart TB
    subgraph P["Sender Side Diagram"]
        A["File reader thread"]
        B[("Sender ring buffer (32 slots)")]
        C["Send thread"]
    end
    D["TCP Socket"]
    E[("./file_to_send/")]
    A -- "fread()" --> E
    A -- "enqueue" --> B
    C -- "dequeue" --> B 
    C -- "send() calls" --> D
```


### Receiver Side
Arrows show which thread calls into which component.

```mermaid
flowchart TB
    subgraph P["Receiver Side Diagram"]
        A["Recv thread"]
        B[("Receiver ring buffer (32 slots)")]
        C["File reconstructor thread"]
    end
    D["TCP Socket"]
    E[("./received_files/")]
    A -- "recv() calls" --> D
    A -- "enqueue" --> B
    C -- "dequeue" --> B
    C -- "fwrite()" --> E
```


### Threads and responsibilities

1. File reader thread:
    
    It reads file in 4KB chunks and wraps them into messages, in order: start, data x N, end. 

    Blocks when ring buffer is full since enqueue waits.

    Allocates each message with malloc and passes pointer into ring buffer.

    Exits once file_end_msg is enqueued.


2. Send thread:

    It dequeues messages from the ring buffer and sends each one over the TCP socket, header followed by payload.

    Blocks when ring buffer is empty since dequeue waits. Also blocks in send() when the TCP window is full.

    Takes ownership of each message pointer on dequeue and is responsible for freeing it after sending.

    Exits once file_end_msg is sent, or if send() fails.

3. Recv thread:

    It reads the fixed-size message header first, loops until all header bytes arrive, then reads payload_length bytes of payload. Since TCP is a byte stream, one recv() call can return less than requested.

    Blocks in recv() when no data has arrived. Also blocks when ring buffer is full since enqueue waits. This is where backpressure reaches the network.

    Allocates each received message with malloc and passes pointer into ring buffer.

    Exits once file_end_msg is received, or if the connection drops or a message is invalid.

4. File reconstructor thread:

    It dequeues messages and rebuilds the file on disk. On file_start_msg it creates file from the filename and extension, on each file_data_msg it appends the payload, on file_end_msg it stops.

    Blocks when ring buffer is empty since dequeue waits.

    Takes ownership of each message pointer on dequeue and is responsible for freeing it after use.

    Exits once file_end_msg is processed.


### Why split disk and network I/O

I split the system into separate threads because I wanted a cleanly separated, modular design instead of a script that does everything in one loop. It was also good practice for systems engineering, and it exposed me to problems that real systems run into.

The practical benefit is that disk and network work independently. The threads never talk to each other directly. They only meet at the ring buffer, so a slow disk does not stall the socket, and a slow network does not stall the file reader.

### Backpressure

Backpressure is an inevitable problem with a TCP stream. Producers and consumers never run at exactly the same speed, so the system needs a way to slow the faster side down instead of letting it run ahead.

The ring buffer is bounded at 32 slots. When it is full, the producer blocks until the consumer makes room. When it is empty, the consumer blocks until the producer adds something. I implemented this with condition variables and blocking waits instead of polling, so a waiting thread sleeps and uses no CPU, then wakes only when the state it needs has changed.

On the receiver, the effect travels all the way back to the sender. A slow disk makes the reconstructor dequeue more slowly, the buffer fills, `recv_thread` blocks in `enqueue` and stops calling `recv()`, the receiver's TCP window fills, and the sender's `send()` blocks. That in turn fills the sender's ring buffer and throttles the file reader. Nothing is dropped and nothing grows without limit.



### Data flow of one transfer

```mermaid
sequenceDiagram
    participant S as Sender
    participant R as Receiver
    S->>R: FILE_START_MSG (name, ext, size)
    loop until whole file sent
        S->>R: FILE_DATA_MSG (up to 4096 bytes)
    end
    S->>R: FILE_END_MSG
```

### Message types
1.  START 
    
    Carries filename, extension and file size. The receiver creates the file in ./received_files/.

2. DATA

    Up to 4096 payload bytes each, and the last one is usually smaller. The receiver appends each one to the file.

3. END

    No payload (payload_length = 0). It tells the receiver to stop, and the receiver's threads shut down.

### Message framing
Every message starts with a small header holding msg_type and payload_length. The receiver reads the header first, then reads exactly payload_length more bytes. This is why it works on a byte stream where message boundaries don't exist.