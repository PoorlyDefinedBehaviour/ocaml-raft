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

let main ~net =
  Eio.Switch.run @@ fun sw ->
  match connect_to_any_server ~sw ~net ~server_addresses with
  | None -> traceln "unable to connect to a server"
  | Some conn -> traceln "connected to server"

let () = Eio_main.run (fun env -> main ~net:(Eio.Stdenv.net env))
