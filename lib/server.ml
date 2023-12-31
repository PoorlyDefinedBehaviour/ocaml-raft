open Eio.Std

let traceln fmt = traceln ("server: " ^^ fmt)
let handle_client flow addr = assert false

let start ~socket =
  traceln "starting TCP server";
  Eio.Net.run_server socket handle_client
    ~on_error:(Eio.Std.traceln "Error handling connection: %a" Fmt.exn)
    ~max_connections:1000
