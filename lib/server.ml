open Eio.Std

let traceln fmt = traceln ("server: " ^^ fmt)

let handle_client (replica : Replica.replica) flow addr =
  traceln "client connected. addr=%a" Eio.Net.Sockaddr.pp addr;
  (* TODO: set max size to max the we may need (can the max append entries size in bytes be used here?) *)
  let buf_reader = Eio.Buf_read.of_flow flow ~max_size:1_000_000_000 in
  let rec loop () =
    match Tcp_transport.receive buf_reader with
    | None ->
        traceln "client connection closed. addr=%a" Eio.Net.Sockaddr.pp addr
    | Some message ->
        let message : Replica.input_message =
          match message with
          | Tcp_transport.RequestVote message -> Replica.RequestVote message
          | Tcp_transport.RequestVoteOutput message ->
              Replica.RequestVoteOutput message
          | Tcp_transport.AppendEntries message -> Replica.AppendEntries message
          | Tcp_transport.AppendEntriesOutput message ->
              Replica.AppendEntriesOutput message
        in
        Replica.handle_message replica message;
        loop ()
  in

  loop ()

let start ~socket ~(replica : Replica.replica) =
  (* TODO: accept tcp connections from clients *)
  assert false 
  traceln "starting TCP server";
  Eio.Net.run_server socket (handle_client replica)
    ~on_error:(Eio.Std.traceln "Error handling connection: %a" Fmt.exn)
    ~max_connections:1000
