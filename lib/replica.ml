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
  append_entries : int64 -> Protocol.entry list -> unit;
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
  (* Function called to apply an entry to the state machine. *)
  fsm_apply : Protocol.entry -> unit;
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
    ~(initial_state : initial_state) ~(fsm_apply : Protocol.entry -> unit) :
    replica =
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
    fsm_apply;
  }

let has_voted_for_other_candidate (replica : replica)
    (candidate_id : replica_id) : bool =
  match replica.persistent_state.voted_for with
  | None -> false
  | Some replica_id -> replica_id <> candidate_id

(* TODO: reset election timeout when vote is granted *)
let handle_request_vote (replica : replica) (message : request_vote_input) :
    request_vote_output =
  if message.term > replica.persistent_state.current_term then (
    replica.persistent_state.current_term <- message.term;
    replica.volatile_state.state <- Follower);

  let response : request_vote_output =
    if
      message.term < replica.persistent_state.current_term
      || has_voted_for_other_candidate replica message.candidate_id
      (* If the candidate's last entry term is less than our last entry term *)
      || message.last_log_term < replica.storage.last_log_term ()
      (* If the candidate's last entry term is the same as our last entry term but our log has more entries *)
      || message.last_log_term = replica.storage.last_log_term ()
         && message.last_log_index < replica.storage.last_log_index ()
    then { term = replica.persistent_state.current_term; vote_granted = false }
    else (
      replica.persistent_state.current_term <- message.term;
      replica.persistent_state.voted_for <- Some message.candidate_id;
      { term = replica.persistent_state.current_term; vote_granted = true })
  in
  replica.storage.persist replica.persistent_state;
  response

(* Applies unapplied entries to the state machine. *)
let commit (replica : replica) : unit =
  let exception Break in
  try
    for
      (* Start applying from entry at index 1 or the last applied index. *)
      i = Int64.to_int (Int64.max 1L replica.volatile_state.last_applied_index)
      to Int64.to_int replica.volatile_state.commit_index
    do
      let i = Int64.of_int i in

      let entry =
        match replica.storage.entry_at_index i with
        | None -> raise Break
        | Some v -> v
      in
      replica.fsm_apply entry;
      replica.volatile_state.last_applied_index <- i
    done
  with
  | Break -> ()
  | exn -> raise exn

let handle_append_entries (replica : replica) (message : append_entries_input) :
    append_entries_output =
  if message.leader_term > replica.persistent_state.current_term then (
    replica.persistent_state.current_term <- message.leader_term;
    replica.volatile_state.state <- Follower);

  if
    message.leader_term < replica.persistent_state.current_term
    (* TODO: what if previous_log_index is 0? *)
    || message.previous_log_index > 0L
       &&
       match replica.storage.entry_at_index message.previous_log_index with
       | None -> true
       | Some entry ->
           Printf.printf "entry.term=%Ld previous_log_term=%Ld\n" entry.term
             message.previous_log_term;
           entry.term <> message.previous_log_term
  then
    {
      term = replica.persistent_state.current_term;
      success = false;
      (* TODO: return last log index where logs match *)
      last_log_index = replica.storage.last_log_index ();
    }
  else (
    (* Append new entries to the log. *)
    replica.storage.append_entries message.previous_log_index message.entries;

    (* Update the latest known committed index because the leader may have committed some entries.
       The commit index is the minimum between the leader commit index and the last log entry index
       because the leader may have committed up to index 5 and have sent just 3 entries so far for example.
    *)
    replica.volatile_state.commit_index <-
      Int64.min message.leader_commit (replica.storage.last_log_index ());

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
      let output_message = handle_request_vote replica message in
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

type test_replica = {
  (* The replica being tested. *)
  replica : replica;
  (* A state machine used in tests. *)
  kv : (string, string) Hashtbl.t;
}

(* Returns a replica that can be used in tests. *)
let test_replica (initial_state : Protocol.initial_state) : test_replica =
  let noop_transport =
    {
      send_request_vote_output = (fun _ _ -> ());
      send_append_entries_output = (fun _ _ -> ());
    }
  in

  let kv = Hashtbl.create 0 in
  {
    replica =
      create ~transport:noop_transport ~storage:(test_disk_storage ())
        ~initial_state ~fsm_apply:(fun entry ->
          Hashtbl.replace kv entry.data entry.data);
    kv;
  }

let%test_unit "request vote: replica receives message with higher term -> \
               updates term, transitions to follower and resets voted for, \
               grants vote" =
  let sut = test_replica { current_term = 0L; voted_for = None } in
  sut.replica.volatile_state.state <- Leader;
  let actual =
    handle_request_vote sut.replica
      { term = 1L; candidate_id = 1; last_log_index = 1L; last_log_term = 0L }
  in
  assert (actual = { term = 1L; vote_granted = true });
  assert (sut.replica.persistent_state.voted_for = Some 1);
  assert (sut.replica.persistent_state.current_term = 1L);
  assert (sut.replica.volatile_state.state = Follower)

let%test_unit "request vote: candidate term is not up to date -> do not grant \
               vote" =
  let sut = test_replica { current_term = 1L; voted_for = None } in

  assert (
    handle_request_vote sut.replica
      { term = 0L; candidate_id = 1; last_log_index = 0L; last_log_term = 0L }
    = { term = 1L; vote_granted = false })

let%test_unit "request vote: replica voted for someone else -> do not grant \
               vote" =
  let sut = test_replica { current_term = 0L; voted_for = Some 10 } in

  assert (
    handle_request_vote sut.replica
      { term = 0L; candidate_id = 1; last_log_index = 0L; last_log_term = 0L }
    = { term = 0L; vote_granted = false })

let%test_unit "request vote: candidate's last log term is less than replica's \
               last log term -> do not grant vote" =
  let sut = test_replica { current_term = 0L; voted_for = Some 10 } in
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 1L });

  assert (
    handle_request_vote sut.replica
      { term = 2L; candidate_id = 2; last_log_index = 1L; last_log_term = 0L }
    = { term = 2L; vote_granted = false })

let%test_unit "request vote: same last log term but replica's last log index \
               is greater -> do not grant vote" =
  let sut = test_replica { current_term = 0L; voted_for = None } in

  (* Append two entries to the replica's log. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" }; { term = 1L; data = "2" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 2L });

  (* Candidate requests vote but its log has only one entry and the replica's has two. *)
  assert (
    handle_request_vote sut.replica
      { term = 2L; candidate_id = 2; last_log_index = 1L; last_log_term = 1L }
    = { term = 2L; vote_granted = false })

let%test_unit "request vote: replica persists decision to storage" =
  let sut = test_replica { current_term = 0L; voted_for = None } in

  (* Replica should grant vote to candidate *)
  assert (
    handle_request_vote sut.replica
      { term = 1L; candidate_id = 5; last_log_index = 4L; last_log_term = 2L }
    = { term = 1L; vote_granted = true });

  (* Replica should know it voted in the candidate after restarting *)
  let sut = test_replica (sut.replica.storage.initial_state ()) in

  assert (sut.replica.persistent_state.voted_for = Some 5)

let%test_unit "append entries: message.term < replica.term, reject request" =
  let sut = test_replica { current_term = 1L; voted_for = None } in
  let actual =
    handle_append_entries sut.replica
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
  let sut = test_replica { current_term = 0L; voted_for = None } in
  (* Leader thinks the index of previous log sent to this replica is 1. *)
  let actual =
    handle_append_entries sut.replica
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

let%test_unit "append entries: truncates log in conflict" =
  let sut = test_replica { current_term = 0L; voted_for = None } in

  (* Leader appends some entries to the replica's log. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries =
          [
            { term = 1L; data = "1" };
            { term = 1L; data = "2" };
            { term = 1L; data = "3" };
          ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 3L });

  (* Another leader overrides the entries starting at index 3. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 2L;
        leader_id = 2;
        previous_log_index = 2L;
        previous_log_term = 1L;
        entries =
          [
            { term = 2L; data = "4" };
            { term = 2L; data = "5" };
            { term = 2L; data = "6" };
          ];
        leader_commit = 1L;
      }
    = { term = 2L; success = true; last_log_index = 5L })

let%test_unit "append entries: log entry at previous_log_index does not match \
               previous_log_term should truncate log" =
  (* Replica with empty log. Last log index is 0. *)
  let sut = test_replica { current_term = 0L; voted_for = None } in

  (* Replica's log is empty. Leader thinks the previous log entry at index 1. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 1L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "hello world" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = false; last_log_index = 0L });

  (* Append one entry to the replica's log. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 1L });

  (* previous_log_term does not match the entry at previous_log_index *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 1L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "2" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = false; last_log_index = 1L })

let%test_unit "append entries: leader commit index > replica commit index, \
               should update commit index" =
  (* Replica with empty log. Last log index is 0. *)
  let sut = test_replica { current_term = 0L; voted_for = None } in
  print_endline "        here";

  (* Append some entries to the log. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" }; { term = 1L; data = "2" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 2L });

  (* Leader has commited entries up to index 2. Replica shoulda apply entries 1 and 2 to the state machine. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 2L;
        previous_log_term = 1L;
        entries = [ { term = 1L; data = "3" } ];
        leader_commit = 2L;
      }
    = { term = 1L; success = true; last_log_index = 3L });

  (* Two entries should have been aplied to the state machine so there's two entries in the kv. *)
  assert (Hashtbl.length sut.kv = 2);
  assert (Hashtbl.mem sut.kv "1");
  assert (Hashtbl.mem sut.kv "2");

  (* Leader has committed up to index 3. Should apply entries up to index 3. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        previous_log_index = 3L;
        previous_log_term = 1L;
        entries = [];
        leader_commit = 3L;
      }
    = { term = 1L; success = true; last_log_index = 3L });

  (* Entries 1 and 2 had already been applied. Should have applied entry 3 now. *)
  assert (Hashtbl.length sut.kv = 3);
  assert (Hashtbl.mem sut.kv "1");
  assert (Hashtbl.mem sut.kv "2");
  assert (Hashtbl.mem sut.kv "3")
