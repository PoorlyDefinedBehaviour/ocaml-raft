type replica_id = int [@@deriving show]
type term = int64 [@@deriving show]

type persistent_state = {
  (* The latest term the server has seen. Initialized to 0 on first boot. Monotonically increasing.   *)
  mutable current_term : int64;
  (* Replica that received the vote in the current term *)
  mutable voted_for : replica_id option;
}
[@@deriving show]

type state = Follower | Candidate | Leader [@@deriving show]

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
[@@deriving show]

type initial_state = { current_term : term; voted_for : replica_id option }
[@@deriving show]

type request_vote_output = {
  (* The current term in the replica that received the request vote request *)
  term : term;
  (* The replica that sent the message *)
  replica_id : replica_id;
  (* True means candidate received vote *)
  vote_granted : bool;
}
[@@deriving show]

type entry = { term : term; data : string } [@@deriving show]

type append_entries_input = {
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
[@@deriving show]

type append_entries_output = {
  (* The current term in the replica, for leader to update itself *)
  term : term;
  (* True if follower contained entry matching previous_log_index and previous_log_term *)
  success : bool;
  (* The index of the last log entry where the replica's log and the leader's log match. *)
  last_log_index : int64;
}
[@@deriving show]
