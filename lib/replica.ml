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
  (* Returns the index of the first log entry with the latest term seen. *)
  first_log_index_with_latest_term : unit -> int64;
  (* Returns the entry at the index. *)
  entry_at_index : int64 -> entry option;
  (* Saves the state on persistent storage. *)
  persist : persistent_state -> unit;
  (* Truncates the log starting from the index. *)
  truncate : int64 -> unit;
  (* Appends entries to the log.  *)
  append_entries : int64 -> Protocol.entry list -> unit;
  (* Returns the max number of entries where their size in bytes is less than or equal to the max size. *)
  get_entry_batch :
    from_index:int64 -> max_size_bytes:int -> Protocol.entry list;
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
  (* For each server, the index of the last log entry to send to that server (initialized to Leader last log index + 1) *)
  mutable next_index : (Protocol.replica_id, int64) Hashtbl.t; [@opaque]
  (* For each server, the index of the last log entry known to be replicated on the server (initialized to 0, increases monotonically) *)
  mutable match_index : (Protocol.replica_id, int64) Hashtbl.t; [@opaque]
}
[@@deriving show]

(* The replica configuration. *)
type config = {
  (* The replica id. *)
  id : replica_id;
  (* The ids of the replicas in the cluster. Must include the replica id. *)
  cluster_members : replica_id list;
  (* How long to wait for between sending append entries requests. *)
  heartbeat_interval : int;
  (* The range used to decide the election timeout. Min must be < max.*)
  election_timeout : range;
  (* The size of the batch that can be sent in each append entries request.
     Say there are 10 entries of 100 bytes each in the leader's log.
     If the batch size is 500, at most 5 entries can be sent in each append entries request. *)
  append_entries_max_batch_size_in_bytes : int;
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
  (* The heartbeat timeout has fired. *)
  | HeartbeatTimeoutFired
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
  | HeartbeatTimeoutFired -> invalid_arg "HeartbeatTimeoutFired"
  | ElectionTimeoutFired _ -> invalid_arg "ElectionTimeoutFired"
  | AppendEntriesOutput _ -> invalid_arg "AppendEntriesOutput"
  | RequestVoteOutput _ -> invalid_arg "RequestVoteOutput"

(* Returns the id the replica that sent the message. *)
let replica_id_of_input_message (message : input_message) : Protocol.replica_id
    =
  match message with
  | RequestVote message -> message.candidate_id
  | AppendEntries message -> message.leader_id
  | HeartbeatTimeoutFired -> invalid_arg "HeartbeatTimeoutFired"
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

let create_next_index_map ~(replica_id : Protocol.replica_id)
    ~(cluster_members : Protocol.replica_id list) ~(last_log_index : int64) :
    (Protocol.replica_id, int64) Hashtbl.t =
  let hash_table = Hashtbl.create 0 in
  List.iter
    (fun id ->
      if id <> replica_id then
        Hashtbl.replace hash_table id (Int64.add 1L last_log_index))
    cluster_members;
  hash_table

(* TODO: is the leader comitting? *)
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

(* TODO: test *)
(* Handles a response to an append entries message. *)
let handle_append_entries_output (replica : replica)
    (message : Protocol.append_entries_output) =
  if
    message.term <> replica.persistent_state.current_term
    || replica.volatile_state.state <> Leader
  then `Ignore
  else if message.success then (
    let current_match_index =
      Hashtbl.find replica.volatile_state.match_index message.replica_id
    in
    (* Take the max of the current and the received log index because the message
       may be a response to a request that is not the latest. *)
    Hashtbl.replace replica.volatile_state.match_index message.replica_id
      (max current_match_index message.last_log_index);

    let current_next_index =
      Hashtbl.find replica.volatile_state.next_index message.replica_id
    in

    Hashtbl.replace replica.volatile_state.next_index message.replica_id
      (max current_next_index (Int64.add 1L message.last_log_index));

    (* The index of the first log entry created in the current term. *)
    let low_watermark = replica.storage.first_log_index_with_latest_term () in

    (* Map from the log index to the number of replicas that have entries up to the index. *)
    let count =
      Hashtbl.create (Hashtbl.length replica.volatile_state.match_index)
    in

    (* Count how many replicas have log entries up to the same index. *)
    Hashtbl.iter
      (fun _ index ->
        let n = Hashtbl.find_opt count index |> Option.value ~default:0 in
        Hashtbl.replace count index (n + 1))
      replica.volatile_state.match_index;

    (* Sort from the greatest log index to the lowest log index. *)
    let match_index =
      Hashtbl.to_seq count |> List.of_seq
      |> List.sort (fun (a, _) (b, _) ->
             if a > b then 1 else if a = b then 0 else -1)
      |> List.rev
    in

    (* Start at 1 because the leader has the log entries.
       Say we have 3 servers:
        The leader has log entries up to index 5 since it is the one sending log entries to other servers.
        Follower A has log entries up to index 5 as well.
        Follower B has log entries up to index 3.
        The leader should commit log entries up to index 5
        because the entries are in 2 out of 3 servers. *)
    let n = ref 1 in

    let exception Break in
    (try
       let quorum = majority (List.length replica.config.cluster_members) in
       List.iter
         (fun (index, count) ->
           if index >= low_watermark then (
             n := !n + count;

             if !n >= quorum then (
               replica.volatile_state.commit_index <- index;
               commit replica;
               raise Break)))
         match_index
     with
    | Break -> ()
    | exn -> raise exn);

    `Ignore)
  else (
    (* AppendEntries request failed because the log entries the leader sent did not match the replica's log.
       Update the index of the next log entry to be sent and try again. *)
    Hashtbl.replace replica.volatile_state.next_index message.replica_id
      (Int64.add 1L message.last_log_index);

    `ResendAppendEntries)

(* Returns a timeout with a new version and the timeout time somewhere in the timeout range. *)
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

(* TODO: continue testing *)
let prepare_append_entries (replica : replica)
    ~(replica_id : Protocol.replica_id) : Protocol.append_entries_input =
  let next_index = Hashtbl.find replica.volatile_state.next_index replica_id in
  let previous_index = Int64.sub 1L next_index in
  let previous_entry =
    if previous_index > 0L then replica.storage.entry_at_index previous_index
    else None
  in
  let entries =
    replica.storage.get_entry_batch ~from_index:next_index
      ~max_size_bytes:replica.config.append_entries_max_batch_size_in_bytes
  in

  {
    leader_term = replica.persistent_state.current_term;
    leader_id = replica.config.id;
    replica_id;
    previous_log_index = previous_index;
    previous_log_term =
      (match previous_entry with None -> 0L | Some entry -> entry.term);
    entries;
    leader_commit = replica.volatile_state.commit_index;
  }

let handle_heartbeat_timeout_fired (replica : replica) :
    Protocol.append_entries_input list =
  if replica.volatile_state.state <> Leader then []
  else
    (* TODO: reset heartbeat timeout? *)
    List.filter_map
      (fun replica_id ->
        if replica_id == replica.config.id then None
        else Some (prepare_append_entries replica ~replica_id))
      replica.config.cluster_members

let rec handle_request_vote (replica : replica) (message : request_vote_input) :
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
  if response.vote_granted then reset_election_timeout replica;
  response

and become_leader (replica : replica) : unit =
  replica.volatile_state.state <- Leader;

  (* Spawning a fiber for each timeout is not necessary but I don't wanna spend more time on this project. *)
  Eio.Fiber.fork_daemon ~sw:replica.sw (fun () ->
      (* Sleep until until the moment the timeout should fire. *)
      Eio.Time.Mono.sleep replica.clock
        (* Divide by 1000 to convert from ms to seconds. *)
        (float_of_int replica.config.heartbeat_interval /. 1000.0);

      (* Let the replica that the timeout has fired. *)
      handle_message replica HeartbeatTimeoutFired;

      `Stop_daemon)

(* Handles a request vote output message.
   Becomes leader and returns heartbeat messages if it received votes from a quorum. *)
and handle_request_vote_output (replica : replica)
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
      become_leader replica;

      (* TODO: commit empty log entry? *)

      (* Return heartbeat messages. *)
      List.filter_map
        (fun replica_id ->
          if replica_id <> replica.config.id then
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

and handle_append_entries (replica : replica) (message : append_entries_input) :
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
      replica_id = replica.config.id;
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

    reset_election_timeout replica;

    {
      term = replica.persistent_state.current_term;
      success = true;
      last_log_index = replica.storage.last_log_index ();
      replica_id = replica.config.id;
    })

and start_election (replica : replica) : request_vote_input list =
  (* Start a new term. *)
  replica.persistent_state.current_term <-
    Int64.add replica.persistent_state.current_term 1L;
  replica.persistent_state.voted_for <- None;
  replica.storage.persist replica.persistent_state;
  replica.volatile_state.state <- Candidate;

  reset_election_timeout replica;

  (* votes_received starts with the candidate because it votes for itself. *)
  replica.volatile_state.election <-
    { votes_received = ReplicaIdSet.add replica.config.id ReplicaIdSet.empty };

  (* Return the massages that should be sent to the other members in the cluster. *)
  List.filter_map
    (fun replica_id ->
      if replica_id <> replica.config.id then
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

and reset_election_timeout (replica : replica) : unit =
  let timeout = random_election_timeout replica in

  replica.volatile_state.next_election_timeout <- timeout;

  (* Spawning a fiber for each timeout is not necessary but I don't wanna spend more time on this project. *)
  Eio.Fiber.fork_daemon ~sw:replica.sw (fun () ->
      (* Sleep until until the moment the timeout should fire. *)
      Eio.Time.Mono.sleep_until replica.clock
        replica.volatile_state.next_election_timeout.at;

      (* Let the replica that the timeout has fired. *)
      handle_message replica (ElectionTimeoutFired timeout);

      `Stop_daemon)

(* TODO: add heartbeat timeout and send heartbeat on timeout *)
and handle_message (replica : replica) (input_message : input_message) =
  Eio.Mutex.use_rw ~protect:true replica.mutex (fun () ->
      match input_message with
      | HeartbeatTimeoutFired ->
          List.iter
            (fun (message : Protocol.append_entries_input) ->
              replica.transport.send_append_entries_input message.replica_id
                message)
            (handle_heartbeat_timeout_fired replica)
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
          |> List.iter (fun (message : Protocol.append_entries_input) ->
                 replica.transport.send_append_entries_input message.replica_id
                   message)
      | AppendEntries message ->
          let output_message = handle_append_entries replica message in
          replica.transport.send_append_entries_output
            (replica_id_of_input_message input_message)
            output_message
      | AppendEntriesOutput message -> (
          match handle_append_entries_output replica message with
          | `Ignore -> ()
          | `ResendAppendEntries ->
              List.iter
                (fun replica_id ->
                  if replica_id <> replica.config.id then
                    replica.transport.send_append_entries_input replica_id
                      (prepare_append_entries replica ~replica_id))
                replica.config.cluster_members))

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
          next_index =
            create_next_index_map ~replica_id:config.id
              ~cluster_members:config.cluster_members
              ~last_log_index:(storage.last_log_index ());
          match_index =
            (let hash_table = Hashtbl.create 0 in
             List.iter
               (fun replica_id ->
                 if replica_id <> config.id then
                   Hashtbl.replace hash_table replica_id 0L)
               config.cluster_members;
             hash_table);
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
    first_log_index_with_latest_term =
      (fun () -> Disk_storage.first_log_index_with_latest_term disk_storage);
    get_entry_batch = Disk_storage.get_entry_batch disk_storage;
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
          append_entries_max_batch_size_in_bytes = 64000;
          heartbeat_interval = 75;
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
type test_cluster = { sw : Eio.Switch.t; replicas : test_replica list }

(* Returns a cluster containing test replicas. *)
let test_cluster ~(sw : Eio.Switch.t) ~clock : test_cluster =
  let num_replicas = 3 in

  let replica_ids = List.init num_replicas (fun i -> i) in

  let config =
    {
      id = 0;
      cluster_members = replica_ids;
      election_timeout = { min = 150; max = 300 };
      append_entries_max_batch_size_in_bytes = 64000;
      heartbeat_interval = 75;
    }
  in

  let replica_map = Hashtbl.create num_replicas in

  let transport =
    {
      send_request_vote_input =
        (fun replica_id message ->
          Eio.Fiber.fork_daemon ~sw (fun () ->
              let replica = Hashtbl.find replica_map replica_id in
              handle_message replica (RequestVote message);
              `Stop_daemon));
      send_request_vote_output =
        (fun replica_id message ->
          Eio.Fiber.fork_daemon ~sw (fun () ->
              let replica = Hashtbl.find replica_map replica_id in
              handle_message replica (RequestVoteOutput message);
              `Stop_daemon));
      send_append_entries_input =
        (fun replica_id message ->
          Eio.Fiber.fork_daemon ~sw (fun () ->
              let replica = Hashtbl.find replica_map replica_id in
              handle_message replica (AppendEntries message);
              `Stop_daemon));
      send_append_entries_output =
        (fun replica_id message ->
          Eio.Fiber.fork_daemon ~sw (fun () ->
              let replica = Hashtbl.find replica_map replica_id in
              handle_message replica (AppendEntriesOutput message);
              `Stop_daemon));
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

  { sw; replicas = test_replicas }

(* Instantiates a test cluster and passes it to [f] *)
let with_test_cluster (f : test_cluster -> unit) =
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
    = {
        term = 1L;
        success = true;
        last_log_index = 1L;
        replica_id = sut.replica.config.id;
      });

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
    = {
        term = 1L;
        success = true;
        last_log_index = 2L;
        replica_id = sut.replica.config.id;
      });

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
        replica_id = 3;
      }
    = { term = 1L; vote_granted = true; replica_id = sut.replica.config.id });

  (* Replica should know it voted in the candidate after restarting *)
  let sut =
    test_replica ~sw:sut.sw ~clock:sut.clock
      (sut.replica.storage.initial_state ())
  in

  assert (sut.replica.persistent_state.voted_for = Some 5)

let%test_unit "request vote: resets election timeout when vote is granted" =
  with_test_replica { current_term = 1L; voted_for = None } @@ fun sut ->
  let timeout = sut.replica.volatile_state.next_election_timeout in

  (* Does not reset the election timeout when vote is not granted. *)
  assert (
    handle_request_vote sut.replica
      {
        term = 0L;
        candidate_id = 5;
        last_log_index = 0L;
        last_log_term = 0L;
        replica_id = 2;
      }
    = { term = 1L; vote_granted = false; replica_id = sut.replica.config.id });

  (* Should not reset. *)
  assert (
    timeout.version = sut.replica.volatile_state.next_election_timeout.version);

  (* Replica should grant vote. *)
  assert (
    handle_request_vote sut.replica
      {
        term = 2L;
        candidate_id = 5;
        last_log_index = 4L;
        last_log_term = 2L;
        replica_id = 3;
      }
    = { term = 2L; vote_granted = true; replica_id = sut.replica.config.id });

  (* Should reset the election timeout. *)
  assert (
    Int64.add 1L timeout.version
    = sut.replica.volatile_state.next_election_timeout.version)

let%test_unit "request vote: grants vote to same candidate if the same message \
               is received twice" =
  with_test_replica { current_term = 0L; voted_for = None } @@ fun sut ->
  (* Replica should grant vote to candidate when the first message is received. *)
  assert (
    handle_request_vote sut.replica
      {
        term = 1L;
        candidate_id = 5;
        last_log_index = 4L;
        last_log_term = 2L;
        replica_id = 3;
      }
    = { term = 1L; vote_granted = true; replica_id = sut.replica.config.id });

  (* Replica should grant vote to the same candidate again
     when the same message is received more than once in the same term. *)
  assert (
    handle_request_vote sut.replica
      {
        term = 1L;
        candidate_id = 5;
        last_log_index = 4L;
        last_log_term = 2L;
        replica_id = 3;
      }
    = { term = 1L; vote_granted = true; replica_id = sut.replica.config.id })

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

let%test_unit "prepare_append_entries: returns the append entry message that \
               should be sent to a specific replica" =
  with_test_replica { current_term = 2L; voted_for = None } @@ fun sut ->
  sut.replica.volatile_state.state <- Leader;
  (* TODO *)
  ()

let%test_unit "handle_heartbeat_timeout_fired: when not in the leader state, \
               returns the empty list because no message should be sent" =
  with_test_replica { current_term = 1L; voted_for = None } @@ fun sut ->
  assert (handle_heartbeat_timeout_fired sut.replica = [])

let%test_unit "handle_heartbeat_timeout_fired: when in the leader state, \
               returns the append entries requests that should be sent to \
               other replicas" =
  with_test_cluster @@ fun cluster ->
  let sut = List.hd cluster.replicas in
  print_endline "       -xxxxxx--";
  sut.replica.volatile_state.state <- Leader;
  (* Pretend the heartbeat timeout fired. *)
  assert (
    handle_heartbeat_timeout_fired sut.replica
    = [
        {
          leader_term = sut.replica.persistent_state.current_term;
          leader_id = sut.replica.config.id;
          replica_id = (List.nth cluster.replicas 1).replica.config.id;
          previous_log_index = 0L;
          previous_log_term = 0L;
          entries = [];
          leader_commit = sut.replica.volatile_state.commit_index;
        };
        {
          leader_term = sut.replica.persistent_state.current_term;
          leader_id = sut.replica.config.id;
          replica_id = (List.nth cluster.replicas 2).replica.config.id;
          previous_log_index = 0L;
          previous_log_term = 0L;
          entries = [];
          leader_commit = sut.replica.volatile_state.commit_index;
        };
      ])

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
  assert (
    actual
    = {
        term = 1L;
        success = false;
        last_log_index = 0L;
        replica_id = sut.replica.config.id;
      })

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
  assert (
    actual
    = {
        term = 1L;
        success = false;
        last_log_index = 0L;
        replica_id = sut.replica.config.id;
      })

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
    = {
        term = 1L;
        success = true;
        last_log_index = 3L;
        replica_id = sut.replica.config.id;
      });

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
    = {
        term = 2L;
        success = true;
        last_log_index = 5L;
        replica_id = sut.replica.config.id;
      })

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
    = {
        term = 1L;
        success = false;
        last_log_index = 0L;
        replica_id = sut.replica.config.id;
      });

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
    = {
        term = 1L;
        success = true;
        last_log_index = 1L;
        replica_id = sut.replica.config.id;
      });

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
    = {
        term = 1L;
        success = false;
        last_log_index = 1L;
        replica_id = sut.replica.config.id;
      })

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
    = {
        term = 1L;
        success = true;
        last_log_index = 2L;
        replica_id = sut.replica.config.id;
      });

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
    = {
        term = 1L;
        success = true;
        last_log_index = 3L;
        replica_id = sut.replica.config.id;
      });

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
    = {
        term = 1L;
        success = true;
        last_log_index = 3L;
        replica_id = sut.replica.config.id;
      });

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
    = {
        term = 1L;
        success = true;
        last_log_index = 1L;
        replica_id = sut.replica.config.id;
      });

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

let%test_unit "append entries: resets election timeout when request succeeds" =
  (* Given a replica in the Candidate state *)
  with_test_replica { current_term = 1L; voted_for = None } @@ fun sut ->
  let timeout = sut.replica.volatile_state.next_election_timeout in

  (* Receives a request from a replica with a smaller term, should reject it. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 0L;
        leader_id = 1;
        replica_id = 0;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" } ];
        leader_commit = 0L;
      }
    = {
        term = 1L;
        success = false;
        last_log_index = 0L;
        replica_id = sut.replica.config.id;
      });

  (* Replica should not have reset the election timeout. *)
  assert (
    timeout.version = sut.replica.volatile_state.next_election_timeout.version);

  (* Replica receives an AppendEntries request from the leader. *)
  assert (
    handle_append_entries sut.replica
      {
        leader_term = 2L;
        leader_id = 2;
        replica_id = 0;
        previous_log_index = 0L;
        previous_log_term = 0L;
        entries = [ { term = 1L; data = "1" } ];
        leader_commit = 0L;
      }
    = {
        term = 2L;
        success = true;
        last_log_index = 1L;
        replica_id = sut.replica.config.id;
      });

  (* Replica should reset the election timeout. *)
  assert (
    Int64.add 1L timeout.version
    = sut.replica.volatile_state.next_election_timeout.version)

let%test_unit "append entries output: message.term <> current term, should \
               ignore message" =
  with_test_replica { current_term = 2L; voted_for = None } @@ fun sut ->
  sut.replica.volatile_state.state <- Leader;

  assert (
    handle_append_entries_output sut.replica
      { term = 1L; success = true; last_log_index = 1L; replica_id = 2 }
    = `Ignore)

let%test_unit "append entries output: not in the leader state, should ignore \
               message" =
  with_test_replica { current_term = 2L; voted_for = None } @@ fun sut ->
  sut.replica.volatile_state.state <- Follower;

  assert (
    handle_append_entries_output sut.replica
      { term = 2L; success = true; last_log_index = 1L; replica_id = 2 }
    = `Ignore)

let%test_unit "append entries output: log inconsistency, should send append \
               entries request again" =
  with_test_cluster @@ fun cluster ->
  let sut = List.hd cluster.replicas in
  sut.replica.volatile_state.state <- Leader;
  sut.replica.persistent_state.current_term <- 2L;

  let replica_id = 2 in

  (* Leader receives an append entries response for a request that did not succeed.
     Should send the append entries request again. *)
  assert (
    handle_append_entries_output sut.replica
      { term = 2L; success = false; last_log_index = 2L; replica_id }
    = `ResendAppendEntries);

  (*
    The response says that the last log index at the replica is 2.
    Should update next index to the log entry just after the last.
       *)
  assert (Hashtbl.find sut.replica.volatile_state.next_index replica_id = 3L);

  (* Same assertions as the ones above but using a different last log index. *)
  assert (
    handle_append_entries_output sut.replica
      { term = 2L; success = false; last_log_index = 5L; replica_id }
    = `ResendAppendEntries);

  assert (Hashtbl.find sut.replica.volatile_state.next_index replica_id = 6L)

let%test_unit "append entries output: leader commits entries that are \
               replicated on a quorum" =
  with_test_cluster @@ fun cluster ->
  let sut = List.hd cluster.replicas in
  sut.replica.volatile_state.state <- Leader;
  sut.replica.persistent_state.current_term <- 2L;

  (* Leader has sent entries up to index 3 on this replica. *)
  assert (
    handle_append_entries_output sut.replica
      { term = 2L; success = true; last_log_index = 3L; replica_id = 1 }
    = `Ignore);

  (* Leader should commit up to index 3 because entries are on the leader and on one other replica. *)
  assert (sut.replica.volatile_state.commit_index = 3L);

  (* Leader has sent entries up to index 5 on this replica. *)
  assert (
    handle_append_entries_output sut.replica
      { term = 2L; success = true; last_log_index = 5L; replica_id = 2 }
    = `Ignore);

  (* Leader should commit up to index 5 because entries are on the leader and on one other replica. *)
  assert (sut.replica.volatile_state.commit_index = 5L)

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

(* Calls a function to assert something until the assertion succeeds or the time expires. *)
let assert_eventually ~clock ~(wait_for_ms : int) (condition : unit -> unit) :
    unit =
  let wait_for =
    (* Convert from ms to ns. *)
    Mtime.Span.of_uint64_ns (Int64.of_int (wait_for_ms * 1000000))
  in

  let started = Eio.Time.Mono.now clock in

  let rec go () =
    try condition ()
    with Assert_failure exn ->
      let elapsed =
        Mtime.Span.abs_diff
          (Mtime.Span.of_uint64_ns
             (Mtime.to_uint64_ns (Eio.Time.Mono.now clock)))
          (Mtime.Span.of_uint64_ns (Mtime.to_uint64_ns started))
      in

      if Mtime.Span.compare elapsed wait_for = 1 then raise (Assert_failure exn)
      else (
        (* Wait for a little while before checking the condition again. *)
        Eio.Time.Mono.sleep clock 0.10;
        go ())
  in
  go ()

let%test_unit "handle_message: election timeout fired" =
  (* Given a cluster with no leader and replicas at term 0. *)
  with_test_cluster @@ fun cluster ->
  let sut = List.hd cluster.replicas in

  let timeout = sut.replica.volatile_state.next_election_timeout in

  (* A replica's election timeout fires and it sends request vote requests. *)
  handle_message sut.replica
    (ElectionTimeoutFired { at = Mtime_clock.now (); version = timeout.version });

  (* Replica starts an election and resets the election timeout. *)
  assert (
    Int64.add 1L timeout.version
    = sut.replica.volatile_state.next_election_timeout.version);

  (* The replica becomes leader. *)
  assert_eventually ~clock:sut.clock ~wait_for_ms:300 (fun () ->
      assert (sut.replica.volatile_state.state = Leader))
