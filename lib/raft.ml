type replica_id = int [@@deriving show, eq]
type term = int64 [@@deriving show, eq]

type persistante_state = {
  (* The latest term the server has seen. Initialized to 0 on first boot. Monotonically increasing.   *)
  mutable current_term : int64;
  (* Replica that received the vote in the current term *)
  mutable voted_for : replica_id option;
}
[@@deriving show, eq]

type state = Follower | Candidate | Leader [@@deriving show, eq]

type volatile_state = {
  (* If the replica is a leader/candidate/follower *)
  mutable state : state;
  (* Index of the highest log entry known to be committed. Initialized to 0. Monotonically increasing. *)
  mutable commit_index : int64;
  (* Index of the highest log entry applied to the state machine. Initialized to 0. Monotonically increasing. *)
  mutable last_applied_index : int64;
}
[@@deriving show, eq]

type replica = {
  persistante_state : persistante_state;
  volatile_state : volatile_state;
}
[@@deriving show, eq]

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
[@@deriving show, eq]

type initial_state = { current_term : term; voted_for : replica_id option }
[@@deriving show, eq]

type request_vote_output = {
  (* The current term in the replica that received the request vote request *)
  term : term;
  (* True means candidate received vote *)
  vote_granted : bool;
}
[@@deriving show, eq]

type transport = {
  send_request_vote_output : replica_id -> request_vote_output -> unit;
}

type storage = {
  initial_state : unit -> initial_state;
  last_log_term : unit -> term;
  last_log_index : unit -> int64;
  persist : persistante_state -> unit;
}

(* type deps = { storage : storage } *)

let create () : replica =
  {
    persistante_state =
      {
        (* TODO: get from disk *)
        current_term = 0L;
        (* TODO: get from disk *)
        voted_for = None;
      };
    volatile_state =
      { state = Follower; commit_index = 0L; last_applied_index = 0L };
  }

let has_voted_for_other_candidate (replica : replica)
    (candidate_id : replica_id) : bool =
  match replica.persistante_state.voted_for with
  | None -> false
  | Some replica_id -> replica_id != candidate_id

let handle_request_vote (storage : storage) (replica : replica)
    (message : request_vote_input) : request_vote_output =
  if message.term > replica.persistante_state.current_term then (
    replica.persistante_state.current_term <- message.term;
    replica.volatile_state.state <- Follower);

  let response =
    if
      message.term < replica.persistante_state.current_term
      || has_voted_for_other_candidate replica message.candidate_id
      || message.last_log_term < storage.last_log_term ()
      || message.last_log_index < storage.last_log_index ()
    then { term = replica.persistante_state.current_term; vote_granted = false }
    else (
      replica.persistante_state.current_term <- message.term;
      replica.persistante_state.voted_for <- Some message.candidate_id;
      { term = replica.persistante_state.current_term; vote_granted = true })
  in
  storage.persist replica.persistante_state;
  response

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
  let replica = create () in
  replica.volatile_state.state <- Candidate;
  replica.persistante_state.current_term <- 1L;

  let input =
    { term = 2L; candidate_id = 1; last_log_index = 1L; last_log_term = 1L }
  in
  let expected = { term = 2L; vote_granted = true } in
  let actual = handle_request_vote storage replica input in

  assert (replica.volatile_state.state = Follower);
  assert (replica.persistante_state.current_term = 2L);
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
  let replica = create () in
  replica.persistante_state.current_term <- 1L;

  let input =
    { term = 0L; candidate_id = 1; last_log_index = 0L; last_log_term = 0L }
  in
  let expected = { term = 1L; vote_granted = false } in

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
  let replica = create () in

  replica.persistante_state.voted_for <- Some 10;

  let input =
    { term = 0L; candidate_id = 1; last_log_index = 0L; last_log_term = 0L }
  in
  let expected = { term = 0L; vote_granted = false } in
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
  let replica = create () in

  let input =
    { term = 0L; candidate_id = 1; last_log_index = 3L; last_log_term = 1L }
  in
  let expected = { term = 0L; vote_granted = false } in
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
  let replica = create () in

  let input =
    { term = 0L; candidate_id = 1; last_log_index = 4L; last_log_term = 2L }
  in
  let expected = { term = 0L; vote_granted = false } in
  let actual = handle_request_vote storage replica input in

  assert (actual = expected)

let%test_unit "request vote: replica persists decision to storage" =
  let storage =
    {
      initial_state = (fun () -> assert false);
      last_log_term = (fun () -> 2L);
      last_log_index = (fun () -> 5L);
      persist = (fun _ -> ());
    }
  in
  let replica = create () in

  let input =
    { term = 0L; candidate_id = 1; last_log_index = 4L; last_log_term = 2L }
  in
  let expected = { term = 0L; vote_granted = false } in
  let actual = handle_request_vote storage replica input in
  (* TODO *)
  assert false
(* assert (actual = expected) *)
