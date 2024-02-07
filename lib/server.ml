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

let start ~(sw : Eio.Switch.t) ~socket
    ~(clock : Mtime.t Eio.Time.clock_ty Eio.Resource.t)
    ~(config : Replica.config) ~(transport : Tcp_transport.t)
    ~(storage : Disk_storage.t) ~(random : Rand.t) ~fsm_apply =
  let replica =
    Replica.create ~sw ~clock ~config ~transport ~storage ~random
      ~initial_state:(storage.initial_state ()) ~fsm_apply
  in
  traceln "starting TCP server";
  Eio.Net.run_server socket (handle_client replica)
    ~on_error:(Eio.Std.traceln "Error handling connection: %a" Fmt.exn)
    ~max_connections:1000
