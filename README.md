# sing-mux

Simple multiplex library.
## Herta-Network packet progress

Bound and full-cone UDP connections expose `PacketBatchWriterWithCount` in both
directions. At most 64 datagrams share a vector write, and only complete encoded
frames contribute to the returned prefix. Lazy stream request/response headers
belong to the first frame. Scalar and batch writes share synchronization and a
terminal error state; Close interrupts blocked I/O independently of that lock.
All input buffers are consumed on success and failure. Empty first datagrams
carry an explicit address/length frame rather than only opening the stream.

The shared sing dependency is pinned to Herta-Network/sing. Main modules must
repeat that replacement. Local tests cover both packet modes/directions,
60 KB and empty datagrams, partial writes, chunk boundaries, concurrent writes,
validation, and shutdown.
