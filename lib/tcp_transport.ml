type t = { x : int }

type config = {
  cluster_members : (Protocol.replica_id, Eio.Net.Ipaddr.v4v6) Hashtbl.t;
}

(* TODO: Continue implementing the transport to send messages to replicas. *)
let create ~(config : config) : unit = assert false
let send_request_vote_input replica_id message = assert false
let send_request_vote_output replica_id message = assert false
let send_append_entries_input replica_id message = assert false
let send_append_entries_output replica_id message = assert false
