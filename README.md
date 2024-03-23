## About

Distributed key value store built on top of a Raft implementation with [eio](https://github.com/ocaml-multicore/eio) from scratch.  I started working on this to practice OCaml while interviewing for an OCaml role. I'll stop working on it as soon as I lose interest.

## Running locally

```
# Terminal 1
ID=0 dune exec server

# Terminal 2
ID=1 dune exec server

# Terminal 3
ID=2 dune exec server
```

## TODO

- Check that the replica append one entry to its log when it becomes a leader
- Use eio in the network and storage layer
- Persist last applied index to avoid reapplying the whole log
- Heartbeat timeout handling. Reset timeout
- Handle exceptions
- Stop using lists
- Check that data is correctly stored in the state file (look at the file contents and ensure there's nothing weird stored in there)
- Remove completed todos
