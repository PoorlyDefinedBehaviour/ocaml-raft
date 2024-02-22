open Eio.Std

let traceln fmt = traceln ("server: " ^^ fmt)

let handle_replica_conn (replica : Replica.replica) flow addr =
  traceln "replica connected. addr=%a" Eio.Net.Sockaddr.pp addr;
  (* TODO: set max size to max the we may need (can the max append entries size in bytes be used here?) *)
  let buf_reader = Eio.Buf_read.of_flow flow ~max_size:1_000_000_000 in
  let rec loop () =
    match Tcp_transport.receive buf_reader with
    | None ->
        traceln "replica connection closed. addr=%a" Eio.Net.Sockaddr.pp addr
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

let handle_client_conn (replica : Replica.replica) flow addr =
  traceln "client connected. addr=%a" Eio.Net.Sockaddr.pp addr;
  (* TODO: set max size to max the we may need (can the max append entries size in bytes be used here?) *)
  let buf_reader = Eio.Buf_read.of_flow flow ~max_size:1_000_000_000 in
  let rec loop () =
    match Tcp_transport.receive_client_request buf_reader with
    | None ->
        traceln "client connection closed. addr=%a" Eio.Net.Sockaddr.pp addr
    | Some payload ->
        Replica.handle_message replica
          (Replica.ClientRequest
             {
               payload;
               send_response =
                 (fun response ->
                   let encoded_message =
                     Tcp_transport.encode_client_request_response response
                   in
                   Eio.Buf_write.with_flow flow (fun to_client ->
                       Eio.Buf_write.string to_client encoded_message));
             });

        loop ()
  in
  loop ()

let start ~replicas_socket ~clients_socket ~(replica : Replica.replica) =
  Eio.Fiber.both
    (fun () ->
      traceln "starting TCP server for replica connections";
      Eio.Net.run_server replicas_socket
        (handle_replica_conn replica)
        ~on_error:
          (Eio.Std.traceln "Error handling replica connection: %a" Fmt.exn)
        ~max_connections:1000)
    (fun () ->
      traceln "starting TCP server for client connections";
      Eio.Net.run_server clients_socket
        (handle_client_conn replica)
        ~on_error:
          (Eio.Std.traceln "Error handling client connection: %a" Fmt.exn)
        ~max_connections:1000)
