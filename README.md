## About

Distributed key value store built on top of a Raft implementation with [eio](https://github.com/ocaml-multicore/eio) from scratch.  I started working on this to practice OCaml. I'll stop working on it as soon as I lose interest.  

Not every module uses eio because I started without it.

## Running locally

Start the replicas.

```
# Terminal 1
ID=0 dune exec server

# Terminal 2
ID=1 dune exec server

# Terminal 3
ID=2 dune exec server
```

Sends requests

```
# Replica 1
curl -XPOST http://localhost:8100 -d '{"key": "a", "value": "1"}'
curl http://localhost:8100/key

# Replica 2
curl -XPOST http://localhost:8101 -d '{"key": "a", "value": "1"}'
curl http://localhost:8101/key

# Replica 3
curl -XPOST http://localhost:8102 -d '{"key": "a", "value": "1"}'
curl http://localhost:8102/key
```

## TODO

- After an user request is received and replicated to a quorum, a response must be sent to the client.
- Check that the replica append one entry to its log when it becomes a leader
- Use eio in the network and storage layer
- Check that data is correctly stored in the state file (look at the file contents and ensure there's nothing weird stored in there)
- Heartbeat timeout handling. Reset timeout
- Persist last applied index to avoid reapplying the whole log
- Handle exceptions
- Stop using lists
- Remove completed todos
- Clean up inflight requests, add request queue
