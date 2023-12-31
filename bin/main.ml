open Eio.Std

let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 8080)

let main ~net =
  Switch.run ~name:"main" (fun sw ->
      let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:10 addr in
      Raft.Server.start ~socket)

let () = Eio_main.run (fun env -> main ~net:(Eio.Stdenv.net env))
