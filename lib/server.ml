open Eio.Std

let traceln fmt = traceln ("server: " ^^ fmt)

let handle_client (replica : Replica.replica) flow addr =
  traceln "client connected. addr=%a" Eio.Net.Sockaddr.pp addr;
  let rec loop () =
    let message : Replica.input_message =
      traceln "replica %ld is waiting for a message from %a" replica.config.id
        Eio.Net.Sockaddr.pp addr;
      match Tcp_transport.receive flow with
      | RequestVote message -> Replica.RequestVote message
      | RequestVoteOutput message -> Replica.RequestVoteOutput message
      | AppendEntries message -> Replica.AppendEntries message
      | AppendEntriesOutput message -> Replica.AppendEntriesOutput message
    in
    Replica.handle_message replica message;
    ()
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
