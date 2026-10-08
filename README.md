# A Multithreaded File-Transfer System in C

A multithreaded system for transferring files over TCP made in C. It decouples disk I/O and Network I/O using bounded ring buffers that handle backpressure. Producer-Consumer pipelines are used to separate subsystems cleanly.


```mermaid
flowchart LR
    SF[("./file_to_send/")]
    RF[("./received_files/")]

    subgraph S["Sender"]
        A["File reader thread"]
        B[("Sender ring buffer")]
        C["Send thread"]
    end

    subgraph R["Receiver"]
        D["Recv thread"]
        E[("Receiver ring buffer")]
        F["File reconstructor thread"]
    end

    SF -- "fread()" --> A
    A -- "enqueue" --> B
    B -- "dequeue" --> C
    C -- "TCP byte stream" --> D
    D -- "enqueue" --> E
    E -- "dequeue" --> F
    F -- "fwrite()" --> RF
```


### Problems the design has to solve

A naive file-transfer system using a looped send and receive cycle has multiple problems
    
1. Disk I/O and network I/O are coupled, the network sits idle while the disk reads, and the disk sits idle while the network sends. The time per chunk is disk time plus network time, where a pipeline should get it down to roughly the slower of the two.
    
2. Decoupling the stages with an unbounded queue only moves the problem. When one side is faster, the queue grows without limit and memory is exhausted. The system needs a bound that forces the fast side to wait.

3. TCP is a byte stream with no message boundaries. A single `recv()` can return half a header or several messages joined together, so the receiver needs a framing scheme to know where each message starts and ends.

4. Separation of concerns. A single loop mixes disk, network and framing logic in one place. Splitting them into subsystems with narrow interfaces makes each one testable and replaceable on its own.


### Approach 

The system is a four-stage pipeline split across two ends of a TCP connection.

On the sender, a file reader thread reads the file in chunks and wraps them into messages, and a send thread transmits them.

On the receiver, a recv thread reassembles messages from the byte stream, and a file reconstructor thread writes them to disk. 

Neighbouring stages never call each other directly. They meet only at a bounded ring buffer, which answers the problems above: disk and network work in parallel instead of taking turns, and memory use is capped. Each buffer is guarded by a lock and two condition variables. When a buffer is full, the producer sleeps until the consumer frees a slot, and when it is empty, the consumer sleeps until data arrives. Because a waiting thread sleeps rather than polls, it uses no CPU, and a buffer can neither overflow nor underflow. 

Ownership of each heap-allocated message passes from producer to consumer through the buffer, so the consumer frees what the producer allocated. 

To get message boundaries out of the byte stream, every message starts with a fixed-size header holding the message type (`FILE_START_MSG`, `FILE_DATA_MSG` or `FILE_END_MSG`) and a payload length that counts the bytes after the header. The receiver reads the full header, then loops until it has exactly that many payload bytes, which makes partial `recv()` calls harmless. 

See docs/ARCHITECTURE.md for the full design.


### How backpressure propagates

Backpressure is not local to one buffer. It travels from the receiver's disk back to the sender's file reader. A slow disk makes the file reconstructor dequeue more slowly, so the receiver's buffer fills and the recv thread blocks in enqueue and stops calling `recv()`. The receiver's TCP window then fills, and the sender's `send()` blocks. That fills the sender's buffer and finally stalls the file reader. Nothing is dropped and memory never grows without bound.

reconstructor slows → receiver buffer full → recv blocks → TCP window full → send blocks → sender buffer full → file reader blocks


### Design decisions and trade-offs

**32 slots, 4 KB chunks.** These are reasonable defaults, not tuned values. The chunk size means
a file never has to fit in memory or in the buffer: it streams through in pieces, so file size and
memory use are independent. The slot count only bounds how far the fast side can run ahead
(32 × 4 KB = 128 KB in flight per buffer). I haven't benchmarked other values yet.

**One blocking thread per stage.** I chose this to keep the subsystems cleanly separated and to
learn how they interact, not because it is the most scalable design. The cost is more threads
and more context switches than an event loop (`select`, `IOCP`) would use. For one
connection and one file that cost is negligible. It would matter with many concurrent
transfers, and an event-driven design would be the better fit there.

**Mutex and two condition variables.** The ring buffer needs to be safe
under concurrent access and to block producers and consumers instead of failing or spinning.
A lock with `not_full` and `not_empty` condition variables does that simply and correctly.

**Length-prefixed framing.** Every message carries its payload length in a fixed-size header.
I took this from a packet sniffer I wrote earlier, where I wrote parsers for several EtherTypes
and followed the same principle: read the fixed header, learn the length, read exactly that
many bytes. It is simple, it handles binary payloads without escaping, and the receiver always
knows how much to read. Delimiter-based framing would need escaping, because file data can contain any byte.

**What the bound gives you, and what it doesn't.**
Bounding the buffers caps memory and propagates backpressure. The ring buffer also propagates
failure between the two threads that share it.
If a consumer fails, `ring_buffer_consumer_failure()`
wakes every blocked thread, and the producer's `enqueue()` returns `ENQUEUE_CNSMR_FAIL` instead of
blocking on a full queue.
If a producer fails, `ring_buffer_producer_failure()` makes `dequeue()` return `DEQUEUE_PRDCR_FAIL`
once the queue has drained.
Failure does not cross the network by itself, but it is now detected at the socket. A `recv()` that
returns 0 or `SOCKET_ERROR`, or a `send()` that fails, is treated as a failed transfer. The thread
returns `EXIT_FAILURE`, the ring buffer wakes its peer thread, and `main` reads the thread exit
codes after joining, reports which side failed, cleans up and exits with a failure code. A peer
that disappears without closing the connection (power loss, cable pulled) can still leave a thread
blocked in `recv()` until TCP gives up.


### Benchmarks

#### Disk I/O pipeline (`tests/disk_io_bench.c`)

This benchmark isolates the disk side of the system. It runs the file reader thread and the
file reconstructor over a single ring buffer, with no network involved. The reader takes the
first file in `./file_to_send/`, so bench.bin must be the only file there (delete the dummy
file first). The reconstructor writes `./received_files/bench.bin`, which is the name the final
size check looks for. The timer wraps the reconstructor loop, from the first dequeue to the
FIN message, and throughput is `file size / elapsed time` in decimal MB/s (1 MB = 1,000,000
bytes). The test then compares the sent and received sizes and prints `PASS` or `FAIL`.

Because the reader and reconstructor run concurrently, the result is limited by the slower
stage. In the baseline, the limiting stage was the reconstructor, which reopened the output
file for every chunk. The fixed version keeps one FILE* open from FILE_START_MSG to FILE_END_MSG.

**Setup:** 268,435,456-byte (256 MiB) `.bin` file, 4 KB chunks, 32-slot ring buffer, 
Intel i5-11400F, Toshiba HDWD110 SATA HDD, Windows 11.

To create a zero-filled file of 256 MiB:
```powershell
fsutil file createnew file_to_send\bench.bin 268435456
```

| Version | Output file handling | Throughput (MB/s) |
|---|---|---|
| Baseline | `fopen`/`fclose` per 4 KB chunk | 22–27 typical, 19.6 worst run |
| After fix | One fopen at START, one fclose at END | 450–480 typical, 547.9 peak, 55.7 worst run |

**Results:**
Opening the file again and again with `fopen()` called for every chunk, throughput ranged from
19.6 to 27 MB/s across repeated runs.

Keeping the output file open for the whole transfer raised typical throughput from about 22–27 MB/s
to about 450–480 MB/s, roughly a 20x improvement. The cost it removed was 65,536 open/close cycles
per 256 MiB file, not the writes themselves.

**Caveats:** this measures the disk pipeline only, not the network. The timer stops when the last
chunk is handed to the OS, not when it reaches the disk. The fixed version's numbers are far above
what a SATA HDD can sustain (roughly 150–200 MB/s), so they mostly reflect the Windows write cache
absorbing the 256 MiB file. The before/after comparison is still fair because both runs had the same
caching, but the absolute figures are not drive speed. I did not control for file caching, and runs
vary because of it.


### Systems concepts covered
- Producer-consumer pipelines.
- Bounded ring buffer with lock and condition variables. (blocking, not polling)
- End-to-end backpressure through TCP flow control.
- Message framing over a byte stream, including partial `recv()` handling.
- Ownership transfer of heap messages between threads.
- Thread lifecycle and shutdown via an END message, with failures reported to `main` through thread exit codes.
- Detecting a dead peer from `recv()` returning 0 or `SOCKET_ERROR`, versus a clean close after `FILE_END_MSG`.
- Lifetime and cleanup of a long-lived resource (FILE*) across threads and every error path.


### Build and run

Requirements: Windows, MinGW-w64 (GCC), CMake.

```powershell
cmake --preset "GCC 15.1.0 x86_64-w64-mingw32"
cmake --build --preset "GCC 15.1.0 x86_64-w64-mingw32"
```

The build produces a single executable, file_transfer, which acts as either end of the transfer. It is chosen at runtime from a menu. Run it from the project root, because the `./file_to_send/` and `./received_files/` paths are relative.

**First-time setup:** `file_to_send/` and `received_files/` each contain a small dummy file,
because git doesn't track empty directories and they wouldn't exist after cloning otherwise.
Delete the dummy files after you clone, then run your own test.

Receiving: start the program, choose 2, and optionally enter a bind address (default 127.0.0.1). It listens on port 27015 and waits for a sender.

Sending: put the file you want to send in `./file_to_send/`, start the program, and choose 1.
The program shows the name of the first file it finds and asks `Continue to send? [y/n]`.
Enter `y` to continue, then enter the receiver's IP and port. Only one file is sent per run,
so keep a single file in `./file_to_send/`. The received file appears in `./received_files/.`


### Project layout

| Folder              | Purpose                                                                                                                    |
|---------------------|----------------------------------------------------------------------------------------------------------------------------|
| ring_buffer/        | The bounded producer-consumer buffer. One instance on the sender side and one on the receiver side.                        |
| transport/          | Framing, sending and receiving of messages, plus connection handling                                                       |
| file_reader/        | Reads the file, frames it into messages and enqueues them on the sender's ring buffer.                                     |
| file_reconstructor/ | Dequeues from the receiver's ring buffer and rebuilds the file with fwrite() according to each message type.               |
| error_handling/     | Minimal error reporting: names the failing module and a WSAGetLastError() where it applies.                                |
| tests/              | Small per-subsystem test programs plus an end-to-end sender and receiver pair, written to exercise each part in isolation. |


## Status

Work in progress. The core pipeline works end to end: a file moves from disk to the
sender's ring buffer, over TCP, through the receiver's ring buffer, and back onto disk, with
backpressure propagating across the whole chain. Shutdown and error reporting now work for the
common failure cases (peer disconnects, disk or allocation errors). Known Issues are mentioned.

### Known issues

**Shutdown and error handling**
- A peer that vanishes without closing the connection can leave a thread blocked in `recv()` until
  TCP times out. There is no keepalive, timeout or cancellation flag that reaches a blocked thread.
- A recv_thread whose reconstructor has failed notices only at its next `enqueue()`, which may be
  after another `recv()` call returns.
- Completion flags (FIN, conn_terminated, and the reconstructor's done) are plain bools shared
  between threads, not atomic. `main` only reads them after joining, but they are not safe to read
  while threads run.
- The sender cannot tell whether the receiver finished writing the file. A successful send means the
  bytes were handed to the OS. There is no end-of-transfer acknowledgement.

**Transport**
- The header is sent in host byte order with no explicit packing.
- The START message length is not validated against its struct size, and the received
  filename is not sanitized. **Do not use this on untrusted networks.**

**Limits**
- Files over 2 GB are not supported (32-bit `long` on Windows).
- No integrity check: the receiver does not verify the final size or a checksum.
- Windows only (Winsock2 and Win32 threads), one file per run, IPv4 addresses only.

### Roadmap
- [x] Failure signaling between producer and consumer through the ring buffer
- [x] Report thread failures to `main` through exit codes and handle `recv() == 0` as a lost connection
- [x] Audit and fix every allocation and free path
- [x] Add `ring_buffer_destroy` and call it on both ends at shutdown
- [x] Loop send() until all bytes are sent
- [ ] Clean shutdown, remaining work: a cancellation flag that reaches threads blocked in `recv()` or
  `send()`, atomic completion flags, and an end-of-transfer acknowledgement
- [ ] Fixed-width, packed, network-byte-order header with bounds checks and filename sanitizing
- [ ] SHA-256 checksum verified by the receiver
- [ ] Keep the output file open for the whole transfer
- [ ] Benchmarks: pipelined vs. naive loop, and the effect of buffer size and chunk size