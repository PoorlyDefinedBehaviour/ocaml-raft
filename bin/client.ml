open Eio.Std

let traceln fmt = traceln ("bin/client: " ^^ fmt)

let server_addresses =
  [
    `Tcp (Eio.Net.Ipaddr.V4.loopback, 9000);
    `Tcp (Eio.Net.Ipaddr.V4.loopback, 9001);
    `Tcp (Eio.Net.Ipaddr.V4.loopback, 9003);
  ]

let connect_to_any_server ~sw ~net ~server_addresses =
  List.find_map
    (fun addr -> try Some (Eio.Net.connect ~sw net addr) with _ -> None)
    server_addresses

let main ~net ~stdin =
  Eio.Switch.run @@ fun sw ->
  let rec loop () =
    let line = Eio.Buf_read.line stdin in
    let parts = String.split_on_char ' ' line in
    let cmd = List.nth parts 0 in
    match cmd with
    | "SET" ->
        let key = List.nth parts 1 in
        let value = List.nth parts 2 in
        let encoded = Raft.Kv.encode_set_command key value in
        (* TODO: connect to a replica and send request *)
        assert false
    | "GET" -> assert false
    | _ ->
        raise
          (Invalid_argument
             (Format.sprintf "unknown command: %s in %s" cmd line))
  in

  loop ()
(* match connect_to_any_server ~sw ~net ~server_addresses with
   | None -> traceln "unable to connect to a server"
   | Some conn -> traceln "connected to server" *)

let () =
  Eio_main.run (fun env ->
      main ~net:(Eio.Stdenv.net env)
        ~stdin:(Eio.Buf_read.of_flow ~max_size:1024 (Eio.Stdenv.stdin env)))
