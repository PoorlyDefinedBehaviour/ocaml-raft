open Eio.Std

let traceln fmt = traceln ("server: " ^^ fmt)
let handle_client flow addr = assert false

let start ~(sw : Eio.Switch.t) ~socket
    ~(clock : Mtime.t Eio.Time.clock_ty Eio.Resource.t) ~config ~transport
    ~storage ~fsm_apply =
  let replica =
    Replica.create ~sw ~clock ~config ~transport ~storage
      ~initial_state:(storage.initial_state ()) ~fsm_apply
  in
  traceln "starting TCP server";
  Eio.Net.run_server socket handle_client
    ~on_error:(Eio.Std.traceln "Error handling connection: %a" Fmt.exn)
    ~max_connections:1000
