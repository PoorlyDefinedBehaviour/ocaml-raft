let traceln fmt = Eio.Std.traceln ("tcp_transport: " ^^ fmt)

type t = {
  (* Sends a request vote request to the replica. *)
  send_request_vote_input :
    Protocol.replica_id -> Protocol.request_vote_input -> unit;
  (* Sends a request vote response to the replica. *)
  send_request_vote_output :
    Protocol.replica_id -> Protocol.request_vote_output -> unit;
  (* Send a append entries request to the replica. *)
  send_append_entries_input :
    Protocol.replica_id -> Protocol.append_entries_input -> unit;
  (* Sends a append entries response to the replica. *)
  send_append_entries_output :
    Protocol.replica_id -> Protocol.append_entries_output -> unit;
}
[@@deriving show]

(* A message that can be received from another replica. *)
type input_message =
  (* Received a request vote request. *)
  | RequestVote of Protocol.request_vote_input
  (* Received a request vote response. *)
  | RequestVoteOutput of Protocol.request_vote_output
  (* Received an append entries request. *)
  | AppendEntries of Protocol.append_entries_input
  (* Received an append entries response. *)
  | AppendEntriesOutput of Protocol.append_entries_output
[@@deriving show]

let message_type_request_vote_input = '1'
let message_type_request_vote_output = '2'
let message_type_append_entries_input = '3'
let message_type_append_entries_output = '4'

type config = {
  cluster_members : (Protocol.replica_id * Eio.Net.Sockaddr.stream) list;
}

let read_bool (reader : Eio.Buf_read.t) : bool =
  let value = Eio.Buf_read.any_char reader in
  match value with
  | 't' -> true
  | 'f' -> false
  | c ->
      raise
        (Invalid_argument
           (Printf.sprintf
              "read_bool: expected a char representing a boolean but got: %c" c))

let read_char (reader : Eio.Buf_read.t) = Eio.Buf_read.any_char reader

let read_int64_be (reader : Eio.Buf_read.t) =
  let buffer = Eio.Buf_read.take 8 reader in
  String.get_int64_be buffer 0

let read_int32_be (reader : Eio.Buf_read.t) =
  let buffer = Eio.Buf_read.take 4 reader in
  String.get_int32_be buffer 0

let read_string_of_len (reader : Eio.Buf_read.t) (len : int64) : string =
  Eio.Buf_read.take (Int64.to_int len) reader

let encode_bool (b : bool) : char = if b then 't' else 'f'

let encode_request_vote_input (message : Protocol.request_vote_input) =
  let buffer = Buffer.create 0 in
  Buffer.add_char buffer message_type_request_vote_input;
  Buffer.add_int64_be buffer message.term;
  Buffer.add_int32_be buffer message.candidate_id;
  Buffer.add_int32_be buffer message.replica_id;
  Buffer.add_int64_be buffer message.last_log_index;
  Buffer.add_int64_be buffer message.last_log_term;
  Buffer.contents buffer

let decode_request_vote_input (reader : Eio.Buf_read.t) :
    Protocol.request_vote_input =
  let message_type = read_char reader in

  assert (message_type = message_type_request_vote_input);

  let term = read_int64_be reader in
  let candidate_id = read_int32_be reader in
  let replica_id = read_int32_be reader in
  let last_log_index = read_int64_be reader in
  let last_log_term = read_int64_be reader in

  { term; candidate_id; replica_id; last_log_index; last_log_term }

let%test_unit "encode - decode request vote input" =
  let message : Protocol.request_vote_input =
    {
      term = 1L;
      candidate_id = 2l;
      replica_id = 3l;
      last_log_index = 4L;
      last_log_term = 1L;
    }
  in

  assert (
    message |> encode_request_vote_input |> Eio.Flow.string_source
    |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
    |> decode_request_vote_input = message)

let%test_unit "quickcheck: encode - decode request vote input" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_request_vote_input) (fun message ->
        encode_request_vote_input message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> decode_request_vote_input = message)
  in
  QCheck.Test.check_exn test

let encode_request_vote_output (message : Protocol.request_vote_output) =
  let buffer = Buffer.create 0 in
  Buffer.add_char buffer message_type_request_vote_output;
  Buffer.add_int64_be buffer message.term;
  Buffer.add_int32_be buffer message.replica_id;
  Buffer.add_char buffer (encode_bool message.vote_granted);
  Buffer.contents buffer

let decode_request_vote_output (reader : Eio.Buf_read.t) :
    Protocol.request_vote_output =
  let message_type = read_char reader in

  assert (message_type = message_type_request_vote_output);

  let term = read_int64_be reader in
  let replica_id = read_int32_be reader in
  let vote_granted = read_bool reader in
  { term; replica_id; vote_granted }

let%test_unit "quickcheck: encode - decode request vote output" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_request_vote_output) (fun message ->
        encode_request_vote_output message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> decode_request_vote_output = message)
  in
  QCheck.Test.check_exn test

let encode_entry (buffer : Buffer.t) (entry : Protocol.entry) : unit =
  Buffer.add_int64_be buffer entry.term;
  Buffer.add_int64_be buffer (Int64.of_int (String.length entry.data));
  Buffer.add_string buffer entry.data

let read_entry (reader : Eio.Buf_read.t) : Protocol.entry =
  let term = read_int64_be reader in
  let data_len = read_int64_be reader in
  let data = read_string_of_len reader data_len in
  { term; data }

let read_entries (reader : Eio.Buf_read.t) : Protocol.entry list =
  let num_entries = read_int64_be reader in
  List.init (Int64.to_int num_entries) (fun _ -> read_entry reader)

let encode_append_entries_input (message : Protocol.append_entries_input) =
  let buffer = Buffer.create 0 in
  Buffer.add_char buffer message_type_append_entries_input;
  Buffer.add_int64_be buffer message.leader_term;
  Buffer.add_int32_be buffer message.leader_id;
  Buffer.add_int32_be buffer message.replica_id;
  Buffer.add_int64_be buffer message.previous_log_index;
  Buffer.add_int64_be buffer message.previous_log_term;
  Buffer.add_int64_be buffer (Int64.of_int (List.length message.entries));
  List.iter (fun entry -> encode_entry buffer entry) message.entries;
  Buffer.add_int64_be buffer message.leader_commit;
  Buffer.contents buffer

let decode_append_entries_input (reader : Eio.Buf_read.t) :
    Protocol.append_entries_input =
  let message_type = read_char reader in
  assert (message_type = message_type_append_entries_input);

  let leader_term = read_int64_be reader in
  let leader_id = read_int32_be reader in
  let replica_id = read_int32_be reader in
  let previous_log_index = read_int64_be reader in
  let previous_log_term = read_int64_be reader in
  let entries = read_entries reader in
  let leader_commit = read_int64_be reader in
  {
    leader_term;
    leader_id;
    replica_id;
    previous_log_index;
    previous_log_term;
    entries;
    leader_commit;
  }

let%test_unit "quickcheck: encode - decode append entries input" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_append_entries_input) (fun message ->
        encode_append_entries_input message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> decode_append_entries_input = message)
  in
  QCheck.Test.check_exn test

let encode_append_entries_output (message : Protocol.append_entries_output) =
  let buffer = Buffer.create 0 in
  Buffer.add_char buffer message_type_append_entries_output;
  Buffer.add_int64_be buffer message.term;
  Buffer.add_char buffer (encode_bool message.success);
  Buffer.add_int64_be buffer message.last_log_index;
  Buffer.add_int32_be buffer message.replica_id;
  Buffer.contents buffer

let decode_append_entries_output (reader : Eio.Buf_read.t) :
    Protocol.append_entries_output =
  let message_type = read_char reader in
  assert (message_type = message_type_append_entries_output);

  let term = read_int64_be reader in
  let success = read_bool reader in
  let last_log_index = read_int64_be reader in
  let replica_id = read_int32_be reader in
  { term; success; last_log_index; replica_id }

let%test_unit "quickcheck: encode - decode append entries output" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_append_entries_output) (fun message ->
        encode_append_entries_output message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> decode_append_entries_output = message)
  in
  QCheck.Test.check_exn test

(* Creates a connection if a connection does not already exist in the connection map.
   Returns the connection. *)
let find_or_create_connection ~sw ~net ~connections ~cluster_members ~replica_id
    =
  match Hashtbl.find_opt connections replica_id with
  | Some flow -> Ok flow
  | None -> (
      let _replica_id, replica_addr =
        List.find (fun (id, _replica_addr) -> id = replica_id) cluster_members
      in
      try
        let flow = Eio.Net.connect ~sw net replica_addr in
        Hashtbl.replace connections replica_id flow;
        Ok flow
      with exn -> Error exn)

let ignore_connection_exn_regex = Re.compile (Re.str "Connection refused")

let send ~sw ~net ~connections ~cluster_members ~replica_id ~message =
  match
    find_or_create_connection ~sw ~net ~connections ~cluster_members ~replica_id
  with
  | Ok flow ->
      Eio.Buf_write.with_flow flow (fun to_server ->
          Eio.Buf_write.string to_server message)
  | Error exn ->
      let err = Printexc.to_string exn in

      if not (Re.execp ignore_connection_exn_regex err) then
        traceln "error trying to create connection with replica %ld err=%s"
          replica_id err

(* Reads an input_message from [flow] *)
let receive buf_reader : input_message option =
  match Eio.Buf_read.peek_char buf_reader with
  | None ->
      traceln
        "unable to read char from buf reader, no message to receive, returning";
      None
  | Some message_type ->
      traceln "receive: message_type=%c" message_type;
      let message =
        if message_type = message_type_request_vote_input then
          RequestVote (decode_request_vote_input buf_reader)
        else if message_type = message_type_request_vote_output then
          RequestVoteOutput (decode_request_vote_output buf_reader)
        else if message_type = message_type_append_entries_input then
          AppendEntries (decode_append_entries_input buf_reader)
        else if message_type = message_type_append_entries_output then
          AppendEntriesOutput (decode_append_entries_output buf_reader)
        else
          let msg =
            Printf.sprintf
              "unknown message type received, this is a bug. message_type=%c"
              message_type
          in
          traceln "%s" msg;
          raise (Invalid_argument msg)
      in
      Some message

(* TODO: Continue implementing the transport to send messages to replicas. *)
let create ~sw ~net ~(config : config) : t =
  let connections = Hashtbl.create 0 in
  {
    send_request_vote_input =
      (fun replica_id message ->
        traceln "sending request vote to replica %ld" replica_id;
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_request_vote_input message));
    send_request_vote_output =
      (fun replica_id message ->
        traceln "sending request vote output to replica %ld" replica_id;
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_request_vote_output message));
    send_append_entries_input =
      (fun replica_id message ->
        traceln "sending append entries to replica %ld" replica_id;
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_append_entries_input message));
    send_append_entries_output =
      (fun replica_id message ->
        traceln "sending append entries output to replica %ld" replica_id;
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_append_entries_output message));
  }
