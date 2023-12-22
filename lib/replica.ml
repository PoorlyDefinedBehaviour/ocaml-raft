open Protocol

type storage = {
  (* Returns the persistent state stored on disk.
     Returns if the default initial state if there's no state stored on disk. *)
  initial_state : unit -> initial_state;
  (* Returns the term of the last log entry. *)
  last_log_term : unit -> term;
  (* Returns the index of the last log entry. *)
  last_log_index : unit -> int64;
  (* Returns the entry at the index. *)
  entry_at_index : int64 -> entry option;
  (* Saves the state on persistent storage. *)
  persist : persistent_state -> unit;
  (* Truncates the log starting from the index. *)
  truncate : int64 -> unit;
  (* Appends entries to the log.  *)
  append_entries : Protocol.entry list -> unit;
}
[@@deriving show]

type transport = {
  (* Sends a request vote response to the replica. *)
  send_request_vote_output : replica_id -> request_vote_output -> unit;
  (* Sends a append entries response to the replica. *)
  send_append_entries_output :
    replica_id -> Protocol.append_entries_output -> unit;
}
[@@deriving show]

type replica = {
  persistent_state : persistent_state;
  volatile_state : volatile_state;
  storage : storage;
  transport : transport;
}
[@@deriving show]

type input_message =
  | RequestVote of Protocol.request_vote_input
  | AppendEntries of Protocol.append_entries_input
[@@deriving show]

(* Returns the term included in the message. *)
let term_of_input_message (message : input_message) : Protocol.term =
  match message with
  | RequestVote message -> message.term
  | AppendEntries message -> message.leader_term

(* Returns the id the replica that sent the message. *)
let replica_id_of_input_message (message : input_message) : Protocol.replica_id
    =
  match message with
  | RequestVote message -> message.candidate_id
  | AppendEntries message -> message.leader_id

type output_message =
  | RequestVote of Protocol.request_vote_output
  | AppendEntries of Protocol.append_entries_output
[@@deriving show]

let create ~(transport : transport) ~(storage : storage)
    ~(initial_state : initial_state) : replica =
  {
    persistent_state =
      {
        current_term = initial_state.current_term;
        voted_for = initial_state.voted_for;
      };
    volatile_state =
      { state = Follower; commit_index = 0L; last_applied_index = 0L };
    storage;
    transport;
  }

let has_voted_for_other_candidate (replica : replica)
    (candidate_id : replica_id) : bool =
  match replica.persistent_state.voted_for with
  | None -> false
  | Some replica_id -> replica_id != candidate_id

(* TODO: reset election timeout when vote is granted *)
let handle_request_vote (storage : storage) (replica : replica)
    (message : request_vote_input) : request_vote_output =
  if message.term > replica.persistent_state.current_term then (
    replica.persistent_state.current_term <- message.term;
    replica.volatile_state.state <- Follower);

  let response : request_vote_output =
    if
      message.term < replica.persistent_state.current_term
      || has_voted_for_other_candidate replica message.candidate_id
      || message.last_log_term < storage.last_log_term ()
      || message.last_log_index < storage.last_log_index ()
    then { term = replica.persistent_state.current_term; vote_granted = false }
    else (
      replica.persistent_state.current_term <- message.term;
      replica.persistent_state.voted_for <- Some message.candidate_id;
      { term = replica.persistent_state.current_term; vote_granted = true })
  in
  storage.persist replica.persistent_state;
  response

let commit (replica : replica) : unit =
  (* TODO: apply to state machine *)
  Printf.printf "commit called\n"

let handle_append_entries (replica : replica) (message : append_entries_input) :
    append_entries_output =
  if message.leader_term > replica.persistent_state.current_term then (
    replica.persistent_state.current_term <- message.leader_term;
    replica.volatile_state.state <- Follower);

  if
    message.leader_term < replica.persistent_state.current_term
    || message.previous_log_index > replica.storage.last_log_index ()
  then
    {
      term = replica.persistent_state.current_term;
      success = false;
      (* TODO: return last log index where logs match *)
      last_log_index = replica.storage.last_log_index ();
    }
  else (
    (if message.previous_log_index > 0L then
       let entry =
         Option.get (replica.storage.entry_at_index message.previous_log_index)
       in

       (* Truncate log if leader the log in this replica does not match the leader's log. *)
       if entry.term != message.previous_log_term then
         (* Truncate to the entry before the starting log index in the message. *)
         replica.storage.truncate (Int64.sub 1L message.previous_log_index));

    (* Append new entries to the log. *)
    replica.storage.append_entries message.entries;

    (* Update the latest known committed index because the leader may have committed some entries. *)
    replica.volatile_state.commit_index <-
      Int64.max replica.volatile_state.commit_index message.leader_commit;

    (* If there are entries to commit, commit them. *)
    if
      replica.volatile_state.commit_index
      > replica.volatile_state.last_applied_index
    then commit replica;

    {
      term = replica.persistent_state.current_term;
      success = true;
      last_log_index = replica.storage.last_log_index ();
    })

let handle_message (replica : replica) (input_message : input_message) =
  match input_message with
  | RequestVote message ->
      let output_message =
        handle_request_vote replica.storage replica message
      in
      replica.transport.send_request_vote_output
        (replica_id_of_input_message input_message)
        output_message
  | AppendEntries message ->
      let output_message = handle_append_entries replica message in
      replica.transport.send_append_entries_output
        (replica_id_of_input_message input_message)
        output_message

(* Returns a Disk_storage instance which writes to a temp folder. *)
let test_disk_storage () : storage =
  let dir = Test_util.temp_dir () in
  Printf.printf "Disk_storage dir = %s\n" dir;
  let disk_storage = Disk_storage.create { dir } in
  {
    initial_state = (fun () -> Disk_storage.initial_state disk_storage);
    last_log_term = (fun () -> Disk_storage.last_log_term disk_storage);
    last_log_index = (fun () -> Disk_storage.last_log_index disk_storage);
    entry_at_index =
      (fun index -> Disk_storage.entry_at_index disk_storage index);
    persist = Disk_storage.persist disk_storage;
    truncate = Disk_storage.truncate disk_storage;
    append_entries = Disk_storage.append_entries disk_storage;
  }

(* Returns a replica that can be used in tests. *)
let test_replica (initial_state : Protocol.initial_state) : replica =
  let noop_transport =
    {
      send_request_vote_output = (fun _ _ -> ());
      send_append_entries_output = (fun _ _ -> ());
    }
  in
  create ~transport:noop_transport ~storage:(test_disk_storage ())
    ~initial_state

let%test_unit "request vote: replica receives message with higher term -> \
               updates term, transitions to follower and resets voted for" =
  let storage = test_disk_storage () in
  let replica = test_replica { current_term = 0L; voted_for = None } in
  replica.volatile_state.state <- Leader;
  let actual =
    handle_request_vote storage replica
      { term = 1L; candidate_id = 1; last_log_index = 1L; last_log_term = 0L }
  in
  assert (actual = { term = 1L; vote_granted = true });
  assert (replica.persistent_state.voted_for = Some 1);
  assert (replica.persistent_state.current_term = 1L);
  assert (replica.volatile_state.state = Follower)

let%test_unit "request vote: replica receives a term greater than its own -> \
               become follower / update term / grants vote" =
  let storage =
    {
      initial_state = (fun () -> assert false);
      last_log_term = (fun () -> 1L);
      last_log_index = (fun () -> 1L);
      persist = (fun _ -> ());
      entry_at_index = (fun _ -> None);
      truncate = (fun _ -> ());
      append_entries = (fun _ -> ());
    }
  in
  let replica = test_replica { current_term = 1L; voted_for = None } in
  replica.volatile_state.state <- Candidate;

  let input : request_vote_input =
    { term = 2L; candidate_id = 1; last_log_index = 1L; last_log_term = 1L }
  in
  let expected : request_vote_output = { term = 2L; vote_granted = true } in
  let actual = handle_request_vote storage replica input in

  assert (replica.volatile_state.state = Follower);
  assert (replica.persistent_state.current_term = 2L);
  assert (actual = expected)

let%test_unit "request vote: candidate term is not up to date -> do not grant \
               vote" =
  let storage =
    {
      initial_state = (fun () -> assert false);
      last_log_term = (fun () -> assert false);
      last_log_index = (fun () -> assert false);
      persist = (fun _ -> ());
      entry_at_index = (fun _ -> None);
      truncate = (fun _ -> ());
      append_entries = (fun _ -> ());
    }
  in
  let replica = test_replica { current_term = 1L; voted_for = None } in

  let input : request_vote_input =
    { term = 0L; candidate_id = 1; last_log_index = 0L; last_log_term = 0L }
  in
  let expected : request_vote_output = { term = 1L; vote_granted = false } in

  assert (handle_request_vote storage replica input = expected)

let%test_unit "request vote: replica voted for someone else -> do not grant \
               vote" =
  let storage =
    {
      initial_state = (fun () -> assert false);
      last_log_term = (fun () -> 0L);
      last_log_index = (fun () -> 0L);
      persist = (fun _ -> ());
      entry_at_index = (fun _ -> None);
      truncate = (fun _ -> ());
      append_entries = (fun _ -> ());
    }
  in
  let replica = test_replica { current_term = 0L; voted_for = Some 10 } in

  let input : request_vote_input =
    { term = 0L; candidate_id = 1; last_log_index = 0L; last_log_term = 0L }
  in
  let expected : request_vote_output = { term = 0L; vote_granted = false } in
  let actual = handle_request_vote storage replica input in

  assert (actual = expected)

let%test_unit "request vote: candidate's last log term is less than replica's \
               term -> do not grant vote" =
  let storage =
    {
      initial_state = (fun () -> assert false);
      last_log_term = (fun () -> 2L);
      last_log_index = (fun () -> 3L);
      persist = (fun _ -> ());
      entry_at_index = (fun _ -> None);
      truncate = (fun _ -> ());
      append_entries = (fun _ -> ());
    }
  in
  let replica = test_replica { current_term = 2L; voted_for = None } in

  let input : request_vote_input =
    { term = 1L; candidate_id = 1; last_log_index = 3L; last_log_term = 1L }
  in
  let expected : request_vote_output = { term = 2L; vote_granted = false } in
  let actual = handle_request_vote storage replica input in

  assert (actual = expected)

let%test_unit "request vote: same last log term but replica's last log index \
               is greater -> do not grant vote" =
  let storage =
    {
      initial_state = (fun () -> assert false);
      last_log_term = (fun () -> 2L);
      last_log_index = (fun () -> 5L);
      persist = (fun _ -> ());
      entry_at_index = (fun _ -> None);
      truncate = (fun _ -> ());
      append_entries = (fun _ -> ());
    }
  in
  let replica = test_replica { current_term = 2L; voted_for = None } in

  let input : request_vote_input =
    { term = 2L; candidate_id = 1; last_log_index = 4L; last_log_term = 2L }
  in
  let expected : request_vote_output = { term = 2L; vote_granted = false } in
  let actual = handle_request_vote storage replica input in

  assert (actual = expected)

let%test_unit "request vote: replica persists decision to storage" =
  let storage = test_disk_storage () in
  let replica = test_replica { current_term = 0L; voted_for = None } in

  (* Replica should grant vote to candidate *)
  let input : request_vote_input =
    { term = 1L; candidate_id = 5; last_log_index = 4L; last_log_term = 2L }
  in
  let expected : request_vote_output = { term = 1L; vote_granted = true } in
  let actual = handle_request_vote storage replica input in
  assert (actual = expected);

  (* Replica should know it voted in the candidate after restarting *)
  let replica = test_replica (storage.initial_state ()) in

  assert (replica.persistent_state.voted_for = Some 5)

let%test_unit "append entries: message.term < replica.term, reject request" =
  let replica = test_replica { current_term = 1L; voted_for = None } in
  let actual =
    handle_append_entries replica
      {
        leader_term = 0L;
        leader_id = 1;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [];
        leader_commit = 0L;
      }
  in
  assert (actual = { term = 1L; success = false; last_log_index = 0L })

let%test_unit "append entries: message.previous_log_index > \
               replica.last_lost_index, reject request" =
  (* Replica with empty log. Last log index is 0. *)
  let replica = test_replica { current_term = 0L; voted_for = None } in
  (* Leader thinks the index of previous log sent to this replica is 1. *)
  let actual =
    handle_append_entries replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 1L;
        previous_log_term = 0L;
        entries = [];
        leader_commit = 0L;
      }
  in
  (* Replica should reject the append entries request. *)
  assert (actual = { term = 1L; success = false; last_log_index = 0L })

let%test_unit "append entries: log entry at index does not match \
               message.previous_log_term, should truncate log" =
  (* Replica with empty log. Last log index is 0. *)
  let replica = test_replica { current_term = 0L; voted_for = None } in

  (* Append one entry to the replica's log. *)
  let _ =
    handle_append_entries replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "hello world" } ];
        leader_commit = 0L;
      }
  in

  (* Get the entry that was just appended. *)
  assert (
    replica.storage.entry_at_index 1L = Some { term = 1L; data = "hello world" });

  (* Another leader tries to append entries but the entry at previous_log_index does not match previous_log_term. *)
  let actual =
    handle_append_entries replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 1L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "hello world 2" } ];
        leader_commit = 0L;
      }
  in

  assert (actual = { term = 1L; success = true; last_log_index = 1L });

  assert (
    replica.storage.entry_at_index 1L
    = Some { term = 1L; data = "hello world 2" })
