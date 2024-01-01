open Protocol
module ReplicaIdSet = Set.Make (Int)

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
  (* Sends a request vote request to the replica. *)
  send_request_vote_input : replica_id -> request_vote_input -> unit;
  (* Sends a request vote response to the replica. *)
  send_request_vote_output : replica_id -> request_vote_output -> unit;
  (* Send a append entries request to the replica. *)
  send_append_entries_input :
    replica_id -> Protocol.append_entries_input -> unit;
  (* Sends a append entries response to the replica. *)
  send_append_entries_output :
    replica_id -> Protocol.append_entries_output -> unit;
}
[@@deriving show]

type election = {
  (* A set containing the id of the replicas that voted for the candidate. *)
  mutable votes_received : ReplicaIdSet.t; [@opaque]
}
[@@deriving show]

type timeout = {
  at : Mtime.t;
  (* The version is incremented every time a new timeout is created.
     The version is used to find out if the timeout should be ignored because
      a new timeout has been created. *)
  version : int64;
}
[@@deriving show]

type range = { min : int; max : int } [@@deriving show]

type volatile_state = {
  (* If the replica is a leader/candidate/follower *)
  mutable state : Protocol.state;
  (* Index of the highest log entry known to be committed. Initialized to 0. Monotonically increasing. *)
  mutable commit_index : int64;
  (* Index of the highest log entry applied to the state machine. Initialized to 0. Monotonically increasing. *)
  mutable last_applied_index : int64;
  (* The state of the current election (If there's one) *)
  mutable election : election;
  (* When the next election timeout will fire. *)
  mutable next_election_timeout : timeout;
}
[@@deriving show]

(* The replica configuration. *)
type config = {
  (* The replica id. *)
  id : replica_id;
  (* The ids of the replicas in the cluster. Must include the replica id. *)
  cluster_members : replica_id list;
  (* The range used to decide the election timeout. Min must be < max.*)
  election_timeout : range;
}
[@@deriving show]

type replica = {
  config : config;
  persistent_state : persistent_state;
  volatile_state : volatile_state;
  storage : storage;
  transport : transport;
  (* Function called to apply an entry to the state machine. *)
  fsm_apply : Protocol.entry -> unit;
  (* Must be locked before a message is handled. *)
  mutex : Eio.Mutex.t; [@opaque]
  (* The Eio switch. *)
  sw : Eio.Switch.t; [@opaque]
  (* The Eio clock. *)
  clock : Mtime.t Eio.Time.clock_ty Eio.Resource.t; [@opaque]
  (* A random number generator. *)
  random : Random.t;
}
[@@deriving show]

type input_message =
  (* The election timeout has fired. *)
  | ElectionTimeoutFired of timeout
  (* Received a request vote request. *)
  | RequestVote of Protocol.request_vote_input
  (* Received a request vote response. *)
  | RequestVoteOutput of Protocol.request_vote_output
  (* Received an append entries request. *)
  | AppendEntries of Protocol.append_entries_input
  (* Received an append entries response. *)
  | AppendEntriesOutput of Protocol.append_entries_output
[@@deriving show]

(* Returns the term included in the message. *)
let term_of_input_message (message : input_message) : Protocol.term =
  match message with
  | RequestVote message -> message.term
  | AppendEntries message -> message.leader_term
  | ElectionTimeoutFired _ -> invalid_arg "ElectionTimeoutFired"
  | AppendEntriesOutput _ -> invalid_arg "AppendEntriesOutput"
  | RequestVoteOutput _ -> invalid_arg "RequestVoteOutput"

(* Returns the id the replica that sent the message. *)
let replica_id_of_input_message (message : input_message) : Protocol.replica_id
    =
  match message with
  | RequestVote message -> message.candidate_id
  | AppendEntries message -> message.leader_id
  | ElectionTimeoutFired _ -> invalid_arg "ElectionTimeoutFired"
  | AppendEntriesOutput _ -> invalid_arg "AppendEntriesOutput"
  | RequestVoteOutput _ -> invalid_arg "RequestVoteOutput"

type output_message =
  | RequestVote of Protocol.request_vote_output
  | AppendEntries of Protocol.append_entries_output
[@@deriving show]

let majority (n : int) : int = (n / 2) + 1

let has_voted_for_other_candidate (replica : replica)
    (candidate_id : replica_id) : bool =
  match replica.persistent_state.voted_for with
  | None -> false
  | Some replica_id -> replica_id <> candidate_id

(* Called when a replica with a higher term is found. *)
let transition_to_higher_term (replica : replica) (term : term) : unit =
  replica.persistent_state.current_term <- term;
  replica.volatile_state.state <- Follower;
  replica.persistent_state.voted_for <- None

(* TODO: reset election timeout when vote is granted *)
let handle_request_vote (replica : replica) (message : request_vote_input) :
    request_vote_output =
  if message.term > replica.persistent_state.current_term then
    transition_to_higher_term replica message.term;

  let response : request_vote_output =
    if
      message.term < replica.persistent_state.current_term
      || has_voted_for_other_candidate replica message.candidate_id
      (* If the candidate's last entry term is less than our last entry term *)
      || message.last_log_term < replica.storage.last_log_term ()
      (* If the candidate's last entry term is the same as our last entry term but our log has more entries *)
      || message.last_log_term = replica.storage.last_log_term ()
         && message.last_log_index < replica.storage.last_log_index ()
    then
      {
        term = replica.persistent_state.current_term;
        vote_granted = false;
        replica_id = replica.config.id;
      }
    else (
      replica.persistent_state.current_term <- message.term;
      replica.persistent_state.voted_for <- Some message.candidate_id;
      {
        term = replica.persistent_state.current_term;
        vote_granted = true;
        replica_id = replica.config.id;
      })
  in
  replica.storage.persist replica.persistent_state;
  response

(* Handles a request vote output message.
   Becomes leader and returns heartbeat messages if it received votes from a quorum. *)
let handle_request_vote_output (replica : replica)
    (message : request_vote_output) : append_entries_input list =
  (* Ignore messages that are responses to older request vote requests. *)
  if
    replica.volatile_state.state <> Candidate
    || message.term <> replica.persistent_state.current_term
  then []
  else (
    if message.vote_granted then
      replica.volatile_state.election <-
        {
          votes_received =
            ReplicaIdSet.add message.replica_id
              replica.volatile_state.election.votes_received;
        };

    (* If replica received votes from a quorum. *)
    if
      ReplicaIdSet.cardinal replica.volatile_state.election.votes_received
      >= majority (List.length replica.config.cluster_members)
    then (
      (* Transition to leader. *)
      replica.volatile_state.state <- Leader;

      (* Return heartbeat messages. *)
      List.filter_map
        (fun replica_id ->
          if replica_id != replica.config.id then
            Some
              {
                leader_term = replica.persistent_state.current_term;
                leader_id = replica.config.id;
                replica_id;
                (* TODO: are previous_log_index and previous_log_term correct here? *)
                previous_log_index = replica.storage.last_log_index ();
                previous_log_term = replica.storage.last_log_term ();
                leader_commit = replica.volatile_state.commit_index;
                entries = [];
              }
          else None)
        replica.config.cluster_members)
    else [])

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

(* TODO: reset election timer *)
let handle_append_entries (replica : replica) (message : append_entries_input) :
    append_entries_output =
  if message.leader_term > replica.persistent_state.current_term then (
    replica.persistent_state.current_term <- message.leader_term;
    replica.persistent_state.voted_for <- None;
    replica.volatile_state.state <- Follower;
    replica.storage.persist replica.persistent_state);

  if
    message.leader_term < replica.persistent_state.current_term
    (* TODO: what if previous_log_index is 0? *)
    || message.previous_log_index > 0L
       &&
       match replica.storage.entry_at_index message.previous_log_index with
       | None -> true
       | Some entry -> entry.term <> message.previous_log_term
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

let handle_append_entries_output (replica : replica)
    (message : Protocol.append_entries_output) : unit =
  if message.term > replica.persistent_state.current_term then (
    transition_to_higher_term replica message.term;
    replica.storage.persist replica.persistent_state);

  (* TODO *)
  Printf.printf "got append entries output"

let start_election (replica : replica) : request_vote_input list =
  (* Start a new term. *)
  replica.persistent_state.current_term <-
    Int64.add replica.persistent_state.current_term 1L;
  replica.persistent_state.voted_for <- None;
  replica.storage.persist replica.persistent_state;
  replica.volatile_state.state <- Candidate;

  (* TODO: election timer? *)

  (* votes_received starts with the candidate because it votes for itself. *)
  replica.volatile_state.election <-
    { votes_received = ReplicaIdSet.add replica.config.id ReplicaIdSet.empty };

  (* Return the massages that should be sent to the other members in the cluster. *)
  List.filter_map
    (fun replica_id ->
      if replica_id != replica.config.id then
        Some
          {
            term = replica.persistent_state.current_term;
            candidate_id = replica.config.id;
            replica_id;
            last_log_index = replica.storage.last_log_index ();
            last_log_term = replica.storage.last_log_term ();
          }
      else None)
    replica.config.cluster_members

let handle_message (replica : replica) (input_message : input_message) =
  Eio.Mutex.use_rw ~protect:true replica.mutex (fun () ->
      match input_message with
      | ElectionTimeoutFired timeout ->
          (* The timeout version will not be current timeout version
             when the election timeout has been reset. *)
          if
            timeout.version
            = replica.volatile_state.next_election_timeout.version
          then
            start_election replica
            |> List.iter (fun (message : request_vote_input) ->
                   replica.transport.send_request_vote_input message.replica_id
                     message)
      | RequestVote message ->
          let output_message = handle_request_vote replica message in
          replica.transport.send_request_vote_output
            (replica_id_of_input_message input_message)
            output_message
      | RequestVoteOutput message ->
          handle_request_vote_output replica message
          |> List.iter (fun message ->
                 replica.transport.send_append_entries_input message.replica_id
                   message)
      | AppendEntries message ->
          let output_message = handle_append_entries replica message in
          replica.transport.send_append_entries_output
            (replica_id_of_input_message input_message)
            output_message
      | AppendEntriesOutput message ->
          handle_append_entries_output replica message)

let random_election_timeout (replica : replica) : timeout =
  let now = Mtime_clock.now_ns () in
  let timeout_at =
    Int64.add now
      ((* Convert from ms to ns because Mtime only works with ns. *)
       Int64.mul
         (replica.random.gen_range_int64
            (* The election timeout config is in ms. *)
            (Int64.of_int replica.config.election_timeout.min)
            (Int64.of_int replica.config.election_timeout.max))
         1_000_000L)
  in

  {
    at = Mtime.of_uint64_ns timeout_at;
    version = Int64.add replica.volatile_state.next_election_timeout.version 1L;
  }

let reset_election_timeout (replica : replica) : unit =
  let timeout = random_election_timeout replica in

  replica.volatile_state.next_election_timeout <- timeout;

  Eio.Fiber.fork_daemon ~sw:replica.sw (fun () ->
      (* Sleep until until the moment the timeout should fire. *)
      Eio.Time.Mono.sleep_until replica.clock
        replica.volatile_state.next_election_timeout.at;

      (* Let the replica that the timeout has fired. *)
      handle_message replica (ElectionTimeoutFired timeout);

      `Stop_daemon)

let create ~(sw : Eio.Switch.t) ~clock ~(config : config)
    ~(transport : transport) ~(storage : storage)
    ~(initial_state : initial_state) ~(fsm_apply : Protocol.entry -> unit)
    ~(random : Random.t) : replica =
  let replica =
    {
      config;
      sw;
      clock;
      mutex = Eio.Mutex.create ();
      persistent_state =
        {
          current_term = initial_state.current_term;
          voted_for = initial_state.voted_for;
        };
      volatile_state =
        {
          state = Follower;
          commit_index = 0L;
          last_applied_index = 0L;
          election = { votes_received = ReplicaIdSet.empty };
          next_election_timeout = { at = Mtime_clock.now (); version = 0L };
        };
      storage;
      transport;
      fsm_apply;
      random;
    }
  in
  reset_election_timeout replica;
  replica

(* Returns a Disk_storage instance which writes to a temp folder. *)
let test_disk_storage ~(dir : string) : storage =
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
  (* The directory used to store Raft files. *)
  storage_dir : string;
  (* The replica being tested. *)
  replica : replica;
  (* A state machine used in tests. *)
  kv : (string, string) Hashtbl.t;
  (* The Eio switch. *)
  sw : Eio.Switch.t;
  (* The Eio clock. *)
  clock : Mtime.t Eio.Time.clock_ty Eio.Resource.t;
}

(* Returns a replica that can be used in tests. *)
let test_replica ~(sw : Eio.Switch.t) ~clock
    ?(replica_id : Protocol.replica_id option) ?(config : config option)
    ?(dir : string option) ?(transport : transport option)
    (initial_state : Protocol.initial_state) : test_replica =
  let transport =
    match transport with
    | None ->
        (* no-op transport. *)
        {
          send_request_vote_input = (fun _ _ -> ());
          send_request_vote_output = (fun _ _ -> ());
          send_append_entries_input = (fun _ _ -> ());
          send_append_entries_output = (fun _ _ -> ());
        }
    | Some v -> v
  in

  let id = match replica_id with None -> 0 | Some v -> v in
  let config =
    match config with
    | None ->
        {
          id;
          cluster_members = [];
          election_timeout = { min = 150; max = 300 };
        }
    | Some v -> v
  in
  let dir = match dir with None -> Test_util.temp_dir () | Some v -> v in
  let storage = test_disk_storage ~dir in
  let kv = Hashtbl.create 0 in
  {
    storage_dir = dir;
    replica =
      create ~sw ~clock ~config ~transport ~storage ~initial_state
        ~fsm_apply:(fun entry -> Hashtbl.replace kv entry.data entry.data)
        ~random:(Random.create ());
    kv;
    sw;
    clock;
  }

(* Instantiates a test replica and passes it to [f] *)
let with_test_replica ?(dir : string option)
    (initial_state : Protocol.initial_state) f =
  let dir = match dir with None -> Test_util.temp_dir () | Some v -> v in

  Eio_main.run (fun env ->
      Eio.Switch.run (fun sw ->
          let replica =
            test_replica ~sw
              ~clock:(Eio.Stdenv.mono_clock env)
              ~dir initial_state
          in
          f replica))

(* Contains the replicas used for a cluster used for testing. *)
type test_cluster = { replicas : test_replica list; sw : Eio.Switch.t }

(* Returns a cluster containing test replicas. *)
let test_cluster ~(sw : Eio.Switch.t) ~clock : test_cluster =
  let num_replicas = 3 in

  let replica_ids = List.init num_replicas (fun i -> i) in

  let config =
    {
      id = 0;
      cluster_members = replica_ids;
      election_timeout = { min = 150; max = 300 };
    }
  in

  let replica_map = Hashtbl.create num_replicas in

  let transport =
    {
      send_request_vote_input =
        (fun replica_id message ->
          let replica = Hashtbl.find replica_map replica_id in
          handle_message replica (RequestVote message));
      send_request_vote_output =
        (fun replica_id message ->
          let replica = Hashtbl.find replica_map replica_id in
          handle_message replica (RequestVoteOutput message));
      send_append_entries_input =
        (fun replica_id message ->
          let replica = Hashtbl.find replica_map replica_id in
          handle_message replica (AppendEntries message));
      send_append_entries_output =
        (fun replica_id message ->
          let replica = Hashtbl.find replica_map replica_id in
          handle_message replica (AppendEntriesOutput message));
    }
  in

  let test_replicas =
    replica_ids
    |> List.map (fun replica_id ->
           test_replica ~sw ~clock ~replica_id
             ~config:{ config with id = replica_id }
             ~transport
             { current_term = 0L; voted_for = None })
  in

  List.iter
    (fun test_replica ->
      Hashtbl.replace replica_map test_replica.replica.config.id
        test_replica.replica)
    test_replicas;

  { replicas = test_replicas; sw }

(* Instantiates a test cluster and passes it to [f] *)
let with_test_cluster f =
  Eio_main.run (fun env ->
      Eio.Switch.run (fun sw ->
          let cluster = test_cluster ~sw ~clock:(Eio.Stdenv.mono_clock env) in
          f cluster))

let%test_unit "request vote: replica receives message with higher term -> \
               updates term, transitions to follower and resets voted for, \
               grants vote" =
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  sut.replica.volatile_state.state <- Leader;
  let actual =
    handle_request_vote sut.replica
      {
        term = 1L;
        candidate_id = 1;
        last_log_index = 1L;
        last_log_term = 0L;
        replica_id = 0;
      }
  in
  assert (
    actual
    = { term = 1L; vote_granted = true; replica_id = sut.replica.config.id });
  assert (sut.replica.persistent_state.voted_for = Some 1);
  assert (sut.replica.persistent_state.current_term = 1L);
  assert (sut.replica.volatile_state.state = Follower);

  (* Ensure that the state change has been persisted to storage by restarting the replica. *)
  let sut =
    test_replica ~sw:sut.sw ~clock:sut.clock ~dir:sut.storage_dir
      { current_term = 0L; voted_for = None }
  in
  assert (
    sut.replica.storage.initial_state ()
    = { current_term = 1L; voted_for = Some 1 })

let%test_unit "request vote: candidate term is not up to date -> do not grant \
               vote" =
  with_test_replica { current_term = 1L; voted_for = None } @@ fun sut ->
  assert (
    handle_request_vote sut.replica
      {
        term = 0L;
        candidate_id = 1;
        last_log_index = 0L;
        last_log_term = 0L;
        replica_id = 0;
      }
    = { term = 1L; vote_granted = false; replica_id = sut.replica.config.id })

let%test_unit "request vote: replica voted for someone else -> do not grant \
               vote" =
  with_test_replica { current_term = 0L; voted_for = Some 10 } @@ fun sut ->
  assert (
    handle_request_vote sut.replica
      {
        term = 0L;
        candidate_id = 1;
        last_log_index = 0L;
        last_log_term = 0L;
        replica_id = 0;
      }
    = { term = 0L; vote_granted = false; replica_id = sut.replica.config.id })

let%test_unit "request vote: candidate's last log term is less than replica's \
               last log term -> do not grant vote" =
  with_test_replica { current_term = 0L; voted_for = Some 10 } @@ fun sut ->
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        replica_id = 0;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 1L });

  assert (
    handle_request_vote sut.replica
      {
        term = 2L;
        candidate_id = 2;
        last_log_index = 1L;
        last_log_term = 0L;
        replica_id = 0;
      }
    = { term = 2L; vote_granted = false; replica_id = sut.replica.config.id })

let%test_unit "request vote: same last log term but replica's last log index \
               is greater -> do not grant vote" =
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  (* Append two entries to the replica's log. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        replica_id = 0;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" }; { term = 1L; data = "2" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 2L });

  (* Candidate requests vote but its log has only one entry and the replica's has two. *)
  assert (
    handle_request_vote sut.replica
      {
        term = 2L;
        candidate_id = 2;
        last_log_index = 1L;
        last_log_term = 1L;
        replica_id = 0;
      }
    = { term = 2L; vote_granted = false; replica_id = sut.replica.config.id })

let%test_unit "request vote: replica persists decision to storage" =
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  (* Replica should grant vote to candidate *)
  assert (
    handle_request_vote sut.replica
      {
        term = 1L;
        candidate_id = 5;
        last_log_index = 4L;
        last_log_term = 2L;
        replica_id = 0;
      }
    = { term = 1L; vote_granted = true; replica_id = sut.replica.config.id });

  (* Replica should know it voted in the candidate after restarting *)
  let sut =
    test_replica ~sw:sut.sw ~clock:sut.clock
      (sut.replica.storage.initial_state ())
  in

  assert (sut.replica.persistent_state.voted_for = Some 5)

let%test_unit "request vote output" =
  (* Given a replica that started term 2 and an election. *)
  with_test_cluster @@ fun cluster ->
  let sut = List.hd cluster.replicas in

  sut.replica.persistent_state.current_term <- 2L;

  (* Candidate voted for itself. *)
  sut.replica.volatile_state.election.votes_received <-
    ReplicaIdSet.add sut.replica.config.id ReplicaIdSet.empty;

  (* Replica is not in the Candidate state. Ignores messages when not in the candidate state. *)
  assert (sut.replica.volatile_state.state <> Candidate);
  assert (
    handle_request_vote_output sut.replica
      { term = 2L; replica_id = 2; vote_granted = true }
    = []);

  (* Transition to Candidate state. *)
  sut.replica.volatile_state.state <- Candidate;

  (* Should ignore messages that don't contain the current term. *)
  assert (
    handle_request_vote_output sut.replica
      { term = 1L; replica_id = 1; vote_granted = true }
    = []);

  (* Receives a message from a replica that didn't grant the vote. *)
  assert (
    handle_request_vote_output sut.replica
      { term = 2L; replica_id = 2; vote_granted = false }
    = []);

  (* Receives a message from a replica that did grant the vote.
     Candidate voted for itself and received vote from other replica. Should become leader.
  *)
  let message =
    {
      leader_term = sut.replica.persistent_state.current_term;
      leader_id = sut.replica.config.id;
      replica_id = 0;
      previous_log_index = sut.replica.storage.last_log_index ();
      previous_log_term = sut.replica.storage.last_log_term ();
      leader_commit = sut.replica.volatile_state.commit_index;
      entries = [];
    }
  in
  assert (
    handle_request_vote_output sut.replica
      { term = 2L; replica_id = 30; vote_granted = true }
    = [ { message with replica_id = 1 }; { message with replica_id = 2 } ]);
  assert (sut.replica.volatile_state.state = Leader)

let%test_unit "append entries: message.term < replica.term, reject request" =
  with_test_replica { current_term = 1L; voted_for = None } @@ fun sut ->
  let actual =
    handle_append_entries sut.replica
      {
        leader_term = 0L;
        leader_id = 1;
        replica_id = 0;
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
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  (* Leader thinks the index of previous log sent to this replica is 1. *)
  let actual =
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        replica_id = 0;
        previous_log_index = 1L;
        previous_log_term = 0L;
        entries = [];
        leader_commit = 0L;
      }
  in
  (* Replica should reject the append entries request. *)
  assert (actual = { term = 1L; success = false; last_log_index = 0L })

let%test_unit "append entries: truncates log in conflict" =
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  (* Leader appends some entries to the replica's log. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        replica_id = 0;
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
        replica_id = 0;
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
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  (* Replica's log is empty. Leader thinks the previous log entry at index 1. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        replica_id = 0;
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
        replica_id = 0;
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
        replica_id = 0;
        previous_log_index = 1L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "2" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = false; last_log_index = 1L })

let%test_unit "append entries: leader commit index > replica commit index, \
               should update commit index" =
  (* Replica with empty log. Last log index is 0. *)
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  (* Append some entries to the log. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        replica_id = 0;
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
        replica_id = 0;
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
        replica_id = 0;
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

let%test_unit "append entries: message term > replica term, update own term \
               and transition to follower" =
  (* Given a replica in the Candidate state *)
  with_test_replica { current_term = 0L; voted_for = Some 5 } @@ fun sut ->
  sut.replica.volatile_state.state <- Candidate;

  (* Replica receives a message with a term greater than its own *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 1L;
        leader_id = 1;
        replica_id = 0;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" } ];
        leader_commit = 0L;
      }
    = { term = 1L; success = true; last_log_index = 1L });

  (* Updates term and transitions to Follower *)
  assert (sut.replica.volatile_state.state = Follower);
  assert (sut.replica.persistent_state.current_term = 1L);

  (* Ensure that the state change has been persisted to storage by restarting the replica. *)
  let sut =
    test_replica ~sw:sut.sw ~clock:sut.clock ~dir:sut.storage_dir
      { current_term = 0L; voted_for = None }
  in
  assert (
    sut.replica.storage.initial_state ()
    = { current_term = 1L; voted_for = None })

let%test_unit "start_election: starts new term / transitions to candidate \
               /resets voted_for / returns a list of request vote message that \
               should be sent to other replicas" =
  with_test_cluster @@ fun cluster ->
  let sut = List.hd cluster.replicas in
  let message =
    {
      term = 1L;
      candidate_id = sut.replica.config.id;
      last_log_index = 0L;
      last_log_term = 0L;
      replica_id = 0;
    }
  in
  assert (sut.replica.config.id = 0);
  assert (
    start_election sut.replica
    = [ { message with replica_id = 1 }; { message with replica_id = 2 } ]);
  assert (sut.replica.persistent_state = { current_term = 1L; voted_for = None });
  assert (sut.replica.volatile_state.state = Candidate)

let%test_unit "handle_message: election timeout fired" =
  (* Given a cluster with no leader and replicas at term 0. *)
  with_test_cluster @@ fun cluster ->
  let sut = List.hd cluster.replicas in
  (* A replica's election timeout fires and it sends request vote requests. *)
  handle_message sut.replica
    (ElectionTimeoutFired { at = Mtime_clock.now (); version = 1L });
  (* The replica becomes leader. *)
  assert (sut.replica.volatile_state.state = Leader)
