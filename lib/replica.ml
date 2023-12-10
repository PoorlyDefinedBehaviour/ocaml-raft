open Protocol

type replica = {
  persistent_state : persistent_state;
  volatile_state : volatile_state;
}
[@@deriving show, eq]

type transport = {
  send_request_vote_output : replica_id -> request_vote_output -> unit;
}

type storage = {
  initial_state : unit -> initial_state;
  last_log_term : unit -> term;
  last_log_index : unit -> int64;
  persist : persistent_state -> unit;
}

(* type deps = { storage : storage } *)

let create (initial_state : initial_state) : replica =
  {
    persistent_state =
      {
        current_term = initial_state.current_term;
        voted_for = initial_state.voted_for;
      };
    volatile_state =
      { state = Follower; commit_index = 0L; last_applied_index = 0L };
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

  (* TODO: handle case where message.last_log_index > last log index *)
  (* TODO: handle case where message.last_log_term > last log term *)
  assert false
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

let test_disk_storage () : storage =
  let disk_storage = Disk_storage.create { dir = Test_util.temp_dir () } in
  {
    initial_state = (fun () -> Disk_storage.initial_state disk_storage);
    last_log_term = (fun () -> Disk_storage.last_log_term disk_storage);
    last_log_index = (fun () -> Disk_storage.last_log_index disk_storage);
    persist = Disk_storage.persist disk_storage;
  }

let%test_unit "request vote: replica receives message with higher term -> \
               updates term, transitions to follower and resets voted for" =
  let storage = test_disk_storage () in
  let replica = create { current_term = 0L; voted_for = None } in
  replica.volatile_state.state <- Leader;
  let actual =
    handle_request_vote storage replica
      { term = 1L; candidate_id = 1; last_log_index = 1L; last_log_term = 0L }
  in
  let expected = { term = 1L; vote_granted = true } in
  assert (expected = actual);
  (* TODO: check that replica updated term and reset voted for *)
  assert false;
  assert (replica.volatile_state.state = Follower)

let%test_unit "request vote: replica receives a term greater than its own -> \
               become follower / update term / grants vote" =
  let storage =
    {
      initial_state = (fun () -> assert false);
      last_log_term = (fun () -> 1L);
      last_log_index = (fun () -> 1L);
      persist = (fun _ -> ());
    }
  in
  let replica = create { current_term = 1L; voted_for = None } in
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
    }
  in
  let replica = create { current_term = 1L; voted_for = None } in

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
    }
  in
  let replica = create { current_term = 0L; voted_for = Some 10 } in

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
    }
  in
  let replica = create { current_term = 2L; voted_for = None } in

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
    }
  in
  let replica = create { current_term = 2L; voted_for = None } in

  let input : request_vote_input =
    { term = 2L; candidate_id = 1; last_log_index = 4L; last_log_term = 2L }
  in
  let expected : request_vote_output = { term = 2L; vote_granted = false } in
  let actual = handle_request_vote storage replica input in

  assert (actual = expected)

let%test_unit "request vote: replica persists decision to storage" =
  let storage = test_disk_storage () in
  let replica = create { current_term = 0L; voted_for = None } in

  (* Replica should grant vote to candidate *)
  let input : request_vote_input =
    { term = 1L; candidate_id = 5; last_log_index = 4L; last_log_term = 2L }
  in
  let expected : request_vote_output = { term = 1L; vote_granted = true } in
  let actual = handle_request_vote storage replica input in
  assert (actual = expected);

  (* Replica should know it voted in the candidate after restarting *)
  let replica = create (storage.initial_state ()) in

  assert (replica.persistent_state.voted_for = Some 5)
