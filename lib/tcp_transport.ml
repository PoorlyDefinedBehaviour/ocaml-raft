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

type config = {
  cluster_members : (Protocol.replica_id, Eio.Net.Sockaddr.stream) Hashtbl.t;
}

type string_reader = { position : int ref; buffer : string }

let create_string_reader (buffer : string) = { position = ref 0; buffer }

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
    QCheck.Test.make ~count:1000 ~name:"list_rev_is_involutive"
      (QCheck.make Protocol.gen_request_vote_input) (fun message ->
        encode_request_vote_input message |> decode_request_vote_input = message)
  in
  QCheck.Test.check_exn test

(* TODO: continue implementing the tcp transport*)
let encode_request_vote_output message = assert false
let encode_append_entries_input message = assert false
let encode_append_entries_output mesage = assert false

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
