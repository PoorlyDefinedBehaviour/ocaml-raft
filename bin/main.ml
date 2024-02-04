open Eio.Std

let traceln fmt = traceln ("bin/main: " ^^ fmt)

let main ~net ~clock =
  Switch.run ~name:"main" (fun sw ->
      let replica_id =
        match Sys.getenv_opt "ID" with
        | None ->
            raise
              (Invalid_argument "ID env variable must be an int between 0 and 2")
        | Some v -> v |> int_of_string |> Int32.of_int
      in
      let addr =
        `Tcp (Eio.Net.Ipaddr.V4.loopback, 8000 + Int32.to_int replica_id)
      in
      let random = Raft.Rand.create () in
      let cluster_members = [ 0l; 1l; 2l ] in
      let cluster_members_with_addresses :
          (Raft.Protocol.replica_id * Eio.Net.Sockaddr.stream) list =
        [
          (0l, `Tcp (Eio.Net.Ipaddr.V4.loopback, 8000));
          (1l, `Tcp (Eio.Net.Ipaddr.V4.loopback, 8001));
          (2l, `Tcp (Eio.Net.Ipaddr.V4.loopback, 8003));
        ]
      in
      let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:10 addr in
      let transport =
        Raft.Tcp_transport.create ~sw ~net
          ~config:{ cluster_members = cluster_members_with_addresses }
      in
      let storage =
        Raft.Disk_storage.create
          {
            dir = Printf.sprintf "./ocaml_raft_dev/replica_%ld/data" replica_id;
          }
      in
      let kv = Raft.Kv.create () in
      let config : Raft.Replica.config =
        {
          id = replica_id;
          cluster_members;
          heartbeat_interval = 50;
          election_timeout = { min = 150; max = 250 };
          append_entries_max_batch_size_in_bytes = 4096;
        }
      in
      traceln "starting raft server. replica_id=%ld addr=%a" replica_id
        Eio.Net.Sockaddr.pp addr;
      Raft.Server.start ~sw ~clock ~socket ~transport ~storage ~random
        ~fsm_apply:(Raft.Kv.apply kv) ~config)

let () =
  Eio_main.run (fun env ->
      main ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.mono_clock env))
