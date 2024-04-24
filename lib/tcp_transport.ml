let traceln fmt = Eio.Std.traceln ("tcp_transport: " ^^ fmt)
let message_type_request_vote_input = '1'
let message_type_request_vote_output = '2'
let message_type_append_entries_input = '3'
let message_type_append_entries_output = '4'
let message_type_client_request_response_unknown_leader = '5'
let message_type_client_request_response_redirect_to_leader = '6'
let message_type_client_request_response_success = '7'
let message_type_client_request_response_failure = '8'

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

type config = {
  cluster_members : (Protocol.replica_id * Eio.Net.Sockaddr.stream) list;
}

module Encode = struct
  let bool (b : bool) : char = if b then 't' else 'f'

  let request_vote_input (message : Protocol.request_vote_input) =
    let buffer = Buffer.create 0 in
    Buffer.add_char buffer message_type_request_vote_input;
    Buffer.add_int64_be buffer message.term;
    Buffer.add_int32_be buffer message.candidate_id;
    Buffer.add_int32_be buffer message.replica_id;
    Buffer.add_int64_be buffer message.last_log_index;
    Buffer.add_int64_be buffer message.last_log_term;
    Buffer.contents buffer

  let request_vote_output (message : Protocol.request_vote_output) =
    let buffer = Buffer.create 0 in
    Buffer.add_char buffer message_type_request_vote_output;
    Buffer.add_int64_be buffer message.term;
    Buffer.add_int32_be buffer message.replica_id;
    Buffer.add_char buffer (bool message.vote_granted);
    Buffer.contents buffer

  let encode_entry (buffer : Buffer.t) (entry : Protocol.entry) : unit =
    Buffer.add_int64_be buffer entry.term;
    Buffer.add_int64_be buffer (Int64.of_int (String.length entry.data));
    Buffer.add_string buffer entry.data

  let append_entries_input (message : Protocol.append_entries_input) =
    let buffer = Buffer.create 0 in
    Buffer.add_char buffer message_type_append_entries_input;
    Buffer.add_int64_be buffer message.request_id;
    Buffer.add_int64_be buffer message.leader_term;
    Buffer.add_int32_be buffer message.leader_id;
    Buffer.add_int32_be buffer message.replica_id;
    Buffer.add_int64_be buffer message.previous_log_index;
    Buffer.add_int64_be buffer message.previous_log_term;
    Buffer.add_int64_be buffer (Int64.of_int (List.length message.entries));
    List.iter (fun entry -> encode_entry buffer entry) message.entries;
    Buffer.add_int64_be buffer message.leader_commit;
    Buffer.contents buffer

  let append_entries_output (message : Protocol.append_entries_output) =
    let buffer = Buffer.create 0 in
    Buffer.add_char buffer message_type_append_entries_output;
    Buffer.add_int64_be buffer message.request_id;
    Buffer.add_int64_be buffer message.term;
    Buffer.add_char buffer (bool message.success);
    Buffer.add_int64_be buffer message.last_log_index;
    Buffer.add_int32_be buffer message.replica_id;
    Buffer.contents buffer
end

module Decode = struct
  let request_vote_input (reader : Eio.Buf_read.t) : Protocol.request_vote_input
      =
    let message_type = Buf_reader.read_char reader in

    assert (message_type = message_type_request_vote_input);

    let term = Buf_reader.read_int64_be reader in
    let candidate_id = Buf_reader.read_int32_be reader in
    let replica_id = Buf_reader.read_int32_be reader in
    let last_log_index = Buf_reader.read_int64_be reader in
    let last_log_term = Buf_reader.read_int64_be reader in

    { term; candidate_id; replica_id; last_log_index; last_log_term }

  let request_vote_output (reader : Eio.Buf_read.t) :
      Protocol.request_vote_output =
    let message_type = Buf_reader.read_char reader in

    assert (message_type = message_type_request_vote_output);

    let term = Buf_reader.read_int64_be reader in
    let replica_id = Buf_reader.read_int32_be reader in
    let vote_granted = Buf_reader.read_bool reader in
    { term; replica_id; vote_granted }

  let read_entry (reader : Eio.Buf_read.t) : Protocol.entry =
    let term = Buf_reader.read_int64_be reader in
    let data_len = Buf_reader.read_int64_be reader in
    let data = Buf_reader.read_string_of_len reader data_len in
    { term; data }

  let read_entries (reader : Eio.Buf_read.t) : Protocol.entry list =
    let num_entries = Buf_reader.read_int64_be reader in
    List.init (Int64.to_int num_entries) (fun _ -> read_entry reader)

  let append_entries_input (reader : Eio.Buf_read.t) :
      Protocol.append_entries_input =
    let message_type = Buf_reader.read_char reader in
    assert (message_type = message_type_append_entries_input);

    let request_id = Buf_reader.read_int64_be reader in
    let leader_term = Buf_reader.read_int64_be reader in
    let leader_id = Buf_reader.read_int32_be reader in
    let replica_id = Buf_reader.read_int32_be reader in
    let previous_log_index = Buf_reader.read_int64_be reader in
    let previous_log_term = Buf_reader.read_int64_be reader in
    let entries = read_entries reader in
    let leader_commit = Buf_reader.read_int64_be reader in
    {
      request_id;
      leader_term;
      leader_id;
      replica_id;
      previous_log_index;
      previous_log_term;
      entries;
      leader_commit;
    }

  let append_entries_output (reader : Eio.Buf_read.t) :
      Protocol.append_entries_output =
    let message_type = Buf_reader.read_char reader in
    assert (message_type = message_type_append_entries_output);

    let request_id = Buf_reader.read_int64_be reader in
    let term = Buf_reader.read_int64_be reader in
    let success = Buf_reader.read_bool reader in
    let last_log_index = Buf_reader.read_int64_be reader in
    let replica_id = Buf_reader.read_int32_be reader in
    { request_id; term; success; last_log_index; replica_id }

  let client_request_response (reader : Eio.Buf_read.t) :
      Protocol.client_request_response =
    let message_type = Buf_reader.read_char reader in
    if message_type = message_type_client_request_response_unknown_leader then
      Protocol.UnknownLeader
    else if
      message_type = message_type_client_request_response_redirect_to_leader
    then
      let leader_id = Buf_reader.read_int32_be reader in
      Protocol.RedirectToLeader leader_id
    else if message_type = message_type_client_request_response_success then
      Protocol.ReplicationSuccess
    else if message_type = message_type_client_request_response_failure then
      Protocol.ReplicationFailure
    else
      raise
        (Invalid_argument
           (Printf.sprintf
              "decode_client_request_response: unknown message type: %c"
              message_type))
end

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
    message |> Encode.request_vote_input |> Eio.Flow.string_source
    |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
    |> Decode.request_vote_input = message)

let%test_unit "quickcheck: encode - decode request vote input" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_request_vote_input) (fun message ->
        Encode.request_vote_input message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> Decode.request_vote_input = message)
  in
  QCheck.Test.check_exn test

let%test_unit "quickcheck: encode - decode request vote output" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_request_vote_output) (fun message ->
        Encode.request_vote_output message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> Decode.request_vote_output = message)
  in
  QCheck.Test.check_exn test

let%test_unit "quickcheck: encode - decode append entries output" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_append_entries_output) (fun message ->
        Encode.append_entries_output message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> Decode.append_entries_output = message)
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
      let message =
        if message_type = message_type_request_vote_input then
          RequestVote (Decode.request_vote_input buf_reader)
        else if message_type = message_type_request_vote_output then
          RequestVoteOutput (Decode.request_vote_output buf_reader)
        else if message_type = message_type_append_entries_input then
          AppendEntries (Decode.append_entries_input buf_reader)
        else if message_type = message_type_append_entries_output then
          AppendEntriesOutput (Decode.append_entries_output buf_reader)
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

let receive_client_request buf_reader : string option =
  match Eio.Buf_read.peek_char buf_reader with
  | None ->
      traceln
        "unable to read char from buf reader, no message to receive, returning";
      None
  | Some _ ->
      let len = Buf_reader.read_int64_be buf_reader in
      Some (Buf_reader.read_string_of_len buf_reader len)

let encode_client_request_response (response : Protocol.client_request_response)
    : string =
  let buffer = Buffer.create 0 in
  (match response with
  | UnknownLeader ->
      Buffer.add_char buffer message_type_client_request_response_unknown_leader
  | RedirectToLeader leader_id ->
      Buffer.add_char buffer
        message_type_client_request_response_redirect_to_leader;
      Buffer.add_int32_be buffer leader_id
  | ReplicationSuccess ->
      Buffer.add_char buffer message_type_client_request_response_success
  | ReplicationFailure ->
      Buffer.add_char buffer message_type_client_request_response_failure);
  Buffer.contents buffer

let%test_unit "quickcheck: encode - decode client_request_response" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_client_request_response) (fun message ->
        encode_client_request_response message
        |> Eio.Flow.string_source
        |> Eio.Buf_read.of_flow ~max_size:1_000_000_000
        |> Decode.client_request_response = message)
  in
  QCheck.Test.check_exn test

let create ~sw ~net ~(config : config) : t =
  let connections = Hashtbl.create 0 in
  {
    send_request_vote_input =
      (fun replica_id message ->
        traceln "send_request_vote_input: %s"
          (Protocol.show_request_vote_input message);
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(Encode.request_vote_input message));
    send_request_vote_output =
      (fun replica_id message ->
        traceln "send_request_vote_output: %s"
          (Protocol.show_request_vote_output message);
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(Encode.request_vote_output message));
    send_append_entries_input =
      (fun replica_id message ->
        traceln "send_append_entries_input: %s"
          (Protocol.show_append_entries_input message);
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(Encode.append_entries_input message));
    send_append_entries_output =
      (fun replica_id message ->
        traceln "send_append_entries_output: %s"
          (Protocol.show_append_entries_output message);
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(Encode.append_entries_output message));
  }
