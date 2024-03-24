open Eio.Std

let traceln fmt = traceln ("bin/server: " ^^ fmt)

type set_command = { key : string; value : string } [@@deriving show, yojson]

let json_from_string s =
  try Ok (Yojson.Safe.from_string s)
  with exn -> Error (Printexc.to_string exn)

let handler ~replica _socket request body =
  match Http.Request.resource request with
  | "/" -> (
      match request.meth with
      | `GET -> assert false
      | `POST -> (
          let flow = Eio.Buf_read.of_flow body ~max_size:1024 in
          let body = Eio.Buf_read.take_all flow in

          match Result.bind (json_from_string body) set_command_of_yojson with
          | Error _ ->
              ( Http.Response.make (),
                Cohttp_eio.Body.of_string "Unexpected request body" )
          | Ok command -> (
              let promise, resolver = Eio.Promise.create () in
              let () =
                Raft.Replica.handle_message replica
                  (Raft.Replica.ClientRequest
                     {
                       payload =
                         Raft.Kv.encode_set_command command.key command.value;
                       send_response =
                         (fun response -> Eio.Promise.resolve resolver response);
                     })
              in
              match Eio.Promise.await promise with
              | Raft.Protocol.UnknownLeader ->
                  ( Http.Response.make (),
                    Cohttp_eio.Body.of_string
                      "Unknown leader. Ensure there's a leader." )
              | Raft.Protocol.RedirectToLeader leader_id ->
                  ( Http.Response.make (),
                    Cohttp_eio.Body.of_string
                      (Printf.sprintf
                         "Not the leader. Send the request to replica %ld"
                         leader_id) )
              | Raft.Protocol.ReplicationComplete ->
                  (Http.Response.make (), Cohttp_eio.Body.of_string "OK")))
      | _ ->
          ( Http.Response.make (),
            Cohttp_eio.Body.of_string
              (Printf.sprintf "Unexpected HTTP request method: %s"
                 (Http.Method.to_string request.meth)) ))
  | _ -> (Http.Response.make ~status:`Not_found (), Cohttp_eio.Body.of_string "")

let log_warning ex = Logs.warn (fun f -> f "%a" Eio.Exn.pp ex)

let main ~net ~clock =
  Switch.run ~name:"main" (fun sw ->
      let replica_id =
        match Sys.getenv_opt "ID" with
        | None ->
            raise
              (Invalid_argument "ID env variable must be an int between 0 and 2")
        | Some v -> v |> int_of_string |> Int32.of_int
      in
      let port = int_of_string (Printf.sprintf "810%ld" replica_id) in
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
      let replicas_socket =
        Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:10
          (`Tcp (Eio.Net.Ipaddr.V4.loopback, 8000 + Int32.to_int replica_id))
      in
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
          heartbeat_interval = 3000;
          election_timeout = { min = 3500; max = 4500 };
          append_entries_max_batch_size_in_bytes = 4096;
        }
      in
      let replica =
        Raft.Replica.create ~sw ~clock ~config ~transport ~storage ~random
          ~initial_state:(storage.initial_state ())
          ~fsm_apply:(Raft.Kv.apply kv)
      in
      traceln "http server listening on port %d" port;
      let http_server_socket =
        Eio.Net.listen net ~sw ~backlog:128 ~reuse_addr:true
          (`Tcp (Eio.Net.Ipaddr.V4.loopback, port))
      and server = Cohttp_eio.Server.make ~callback:(handler ~replica) () in
      Eio.Fiber.both
        (fun () ->
          Cohttp_eio.Server.run http_server_socket server ~on_error:log_warning)
        (fun () -> Raft.Server.start ~replicas_socket ~replica);
      assert false)

let () =
  Eio_main.run (fun env ->
      main ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.mono_clock env))
