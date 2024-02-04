## Running locally

```
# Terminal 1
ID=0 dune exec ocaml_raft

# Terminal 2
ID=1 dune exec ocaml_raft

# Terminal 3
ID=2 dune exec ocaml_raft
```

## TODO

- Use eio in the network and storage layer
- Persist last applied index to avoid reapplying the whole log
- Heartbeat timeout handling. Reset timeout
- Handle exceptions
- Stop using lists
- Check that data is correctly stored in the state file (look at the file contents and ensure there's nothing weird stored in there)

## Where i stopped last time

- Was trying to used the tcp connection to read messages from other replicas (there's a Eio.Net reading example in the ocaml eio repo.)