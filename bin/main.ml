open Eio.Std

let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 8080)

let main ~net ~clock =
  Switch.run ~name:"main" (fun sw ->
      let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:10 addr in
      let transport = assert false in
      let storage = assert false in
      let fsm_apply = assert false in
      let config = assert false in
      Raft.Server.start ~sw ~clock ~socket ~transport ~storage ~fsm_apply
        ~config)

let () =
  Eio_main.run (fun env ->
      main ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.mono_clock env))
