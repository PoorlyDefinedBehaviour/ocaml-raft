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

type volatile_state = {
  (* If the replica is a leader/candidate/follower *)
  mutable state : state;
  (* Index of the highest log entry known to be committed. Initialized to 0. Monotonically increasing. *)
  mutable commit_index : int64;
  (* Index of the highest log entry applied to the state machine. Initialized to 0. Monotonically increasing. *)
  mutable last_applied_index : int64;
}
[@@deriving show]

type request_vote_input = {
  (* Candidate's term *)
  term : term;
  (* Candidate requestingf vote *)
  candidate_id : replica_id;
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
  (* The index of the last log entry in the replica *)
  last_log_index : int64;
}
[@@deriving show]
