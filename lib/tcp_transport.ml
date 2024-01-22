type t = {
  (* Functions are the same functions as in the transport type. *)
  send_request_vote_input :
    Protocol.replica_id -> Protocol.request_vote_input -> unit;
  send_request_vote_output :
    Protocol.replica_id -> Protocol.request_vote_output -> unit;
  send_append_entries_input :
    Protocol.replica_id -> Protocol.append_entries_input -> unit;
  send_append_entries_output :
    Protocol.replica_id -> Protocol.append_entries_output -> unit;
}

let message_type_request_vote_input = '1'
let message_type_request_vote_output = '2'
let message_type_append_entries_input = '3'
let message_type_append_entries_output = '4'

type config = {
  cluster_members : (Protocol.replica_id, Eio.Net.Sockaddr.stream) Hashtbl.t;
}

type string_reader = { position : int ref; buffer : string }

let create_string_reader (buffer : string) = { position = ref 0; buffer }

let read_bool (reader : string_reader) : bool =
  let value = String.get reader.buffer !(reader.position) in
  reader.position := !(reader.position) + 1;
  match value with
  | 't' -> true
  | 'f' -> false
  | c ->
      raise
        (Invalid_argument
           (Printf.sprintf
              "read_bool: expected a char representing a boolean but got: %c" c))

let read_char (reader : string_reader) =
  let value = String.get reader.buffer !(reader.position) in
  reader.position := !(reader.position) + 1;
  value

let read_int64_be (reader : string_reader) =
  let value = String.get_int64_be reader.buffer !(reader.position) in
  reader.position := !(reader.position) + 8;
  value

let read_int32_be (reader : string_reader) =
  let value = String.get_int32_be reader.buffer !(reader.position) in
  reader.position := !(reader.position) + 4;
  value

let read_string_of_len (reader : string_reader) (len : int64) : string =
  let data_len = Int64.to_int len in
  let buffer = Bytes.make data_len '0' in
  String.blit reader.buffer !(reader.position) buffer 0 data_len;
  reader.position := !(reader.position) + data_len;
  Bytes.to_string buffer

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

let decode_request_vote_input (buffer : string) : Protocol.request_vote_input =
  let reader = create_string_reader buffer in

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
    message |> encode_request_vote_input |> decode_request_vote_input = message)

let%test_unit "quickcheck: encode - decode request vote input" =
  let test =
    QCheck.Test.make ~count:1000 ~name:"basic"
      (QCheck.make Protocol.gen_request_vote_input) (fun message ->
        encode_request_vote_input message |> decode_request_vote_input = message)
  in
  QCheck.Test.check_exn test

let encode_request_vote_output (message : Protocol.request_vote_output) =
  let buffer = Buffer.create 0 in
  Buffer.add_char buffer message_type_request_vote_output;
  Buffer.add_int64_be buffer message.term;
  Buffer.add_int32_be buffer message.replica_id;
  Buffer.add_char buffer (encode_bool message.vote_granted);
  Buffer.contents buffer

let decode_request_vote_output (buffer : string) : Protocol.request_vote_output
    =
  let reader = create_string_reader buffer in

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
        |> decode_request_vote_output = message)
  in
  QCheck.Test.check_exn test

let encode_entry (buffer : Buffer.t) (entry : Protocol.entry) : unit =
  Buffer.add_int64_be buffer entry.term;
  Buffer.add_int64_be buffer (Int64.of_int (String.length entry.data));
  Buffer.add_string buffer entry.data

let read_entry (reader : string_reader) : Protocol.entry =
  let term = read_int64_be reader in
  let data_len = read_int64_be reader in
  let data = read_string_of_len reader data_len in
  { term; data }

let read_entries (reader : string_reader) : Protocol.entry list =
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

let decode_append_entries_input (buffer : string) :
    Protocol.append_entries_input =
  let reader = create_string_reader buffer in

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

let decode_append_entries_output (buffer : string) :
    Protocol.append_entries_output =
  let reader = create_string_reader buffer in

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
        |> decode_append_entries_output = message)
  in
  QCheck.Test.check_exn test

(* Creates a connection if a connection does not already exist in the connection map.
   Returns the connection. *)
let find_or_create_connection ~sw ~net ~connections ~cluster_members ~replica_id
    =
  match Hashtbl.find_opt connections replica_id with
  | None ->
      let replica_addr = Hashtbl.find cluster_members replica_id in
      let flow = Eio.Net.connect ~sw net replica_addr in
      Hashtbl.replace connections replica_id flow;
      flow
  | Some flow -> flow

let send ~sw ~net ~connections ~cluster_members ~replica_id ~message =
  let flow =
    find_or_create_connection ~sw ~net ~connections ~cluster_members ~replica_id
  in
  Eio.Buf_write.with_flow flow (fun to_server ->
      Eio.Buf_write.string to_server message)

(* TODO: Continue implementing the transport to send messages to replicas. *)
let create ~sw ~net ~(config : config) : t =
  let connections = Hashtbl.create 0 in
  {
    send_request_vote_input =
      (fun replica_id message ->
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_request_vote_input message));
    send_request_vote_output =
      (fun replica_id message ->
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_request_vote_output message));
    send_append_entries_input =
      (fun replica_id message ->
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_append_entries_input message));
    send_append_entries_output =
      (fun replica_id message ->
        send ~sw ~net ~connections ~cluster_members:config.cluster_members
          ~replica_id
          ~message:(encode_append_entries_output message));
  }
