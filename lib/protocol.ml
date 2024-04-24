type replica_id = int32 [@@deriving show, qcheck]
type term = int64 [@@deriving show, qcheck]
type request_id = int64 [@@deriving show, qcheck]

type persistent_state = {
  (* The latest term the server has seen. Initialized to 0 on first boot. Monotonically increasing. *)
  mutable current_term : int64;
  (* Replica that received the vote in the current term *)
  mutable voted_for : replica_id option;
}
[@@deriving show, qcheck]

type state = Follower | Candidate | Leader [@@deriving show, qcheck]

type request_vote_input = {
  (* Candidate's term *)
  term : term;
  (* Candidate requesting vote *)
  candidate_id : replica_id;
  (* Replica receiving the message *)
  replica_id : replica_id;
  (* Index of candidate's last log entry *)
  last_log_index : int64;
  (* Term of candidate's last log entry *)
  last_log_term : term;
}
[@@deriving show, qcheck]

type initial_state = { current_term : term; voted_for : replica_id option }
[@@deriving show, qcheck]

type request_vote_output = {
  (* The current term in the replica that received the message requesting a vote. *)
  term : term;
  (* The replica that sent the message *)
  replica_id : replica_id;
  (* True means candidate received vote *)
  vote_granted : bool;
}
[@@deriving show, qcheck]

type entry = { term : term; data : string } [@@deriving show, qcheck]

type append_entries_input = {
  request_id : request_id;
  (* Leader's term *)
  leader_term : term;
  (* So follower can redirect clients *)
  leader_id : replica_id;
  (* Replica receiving the message. *)
  replica_id : replica_id;
  (* Index of log entry immediately preceding new ones *)
  previous_log_index : int64;
  (* Term of [previous_log_index] entry *)
  previous_log_term : term;
  (* Log entries to store (empty for heartbeat; may send more than one for efficiency) *)
  entries : entry list;
  (* Leader's commit index *)
  leader_commit : int64;
}
[@@deriving show, qcheck]

type append_entries_output = {
  request_id : request_id;
  (* The current term in the replica, for leader to update itself *)
  term : term;
  (* True if follower contained entry matching previous_log_index and previous_log_term *)
  success : bool;
  (* The index of the last log entry where the replica's log and the leader's log match. *)
  last_log_index : int64;
  (* Replica sending the message. *)
  replica_id : replica_id;
}
[@@deriving show, qcheck]

(* Represents a response to a client request. The response is sent using the callback in `client_request` *)
type client_request_response =
  (* The replica is not the leader and doesn't know who the leader is. *)
  | UnknownLeader
  (* The replica is not the leader but knows who the leader is. *)
  | RedirectToLeader of replica_id
  (* The replica is the leader and has replicated the client entry. *)
  | ReplicationSuccess
  (* The replica is the leader and was unable to replicate the client entry. *)
  | ReplicationFailure
[@@deriving show, qcheck]

(* Represents a request received from a client that believes the replica is the leader. *)
type client_request = {
  (* The payload sent by the client. *)
  payload : string;
  (* Callback to send a response to the client. *)
  send_response : client_request_response -> unit;
}
[@@deriving show]
