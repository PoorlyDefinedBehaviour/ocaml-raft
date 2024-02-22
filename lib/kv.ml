type t = { entries : (string, string) Hashtbl.t }
type set_command = { key : string; value : string } [@@deriving show]

(* TODO: test encode and decode *)
let encode_set_command (key : string) (value : string) : string =
  let buffer = Buffer.create 0 in
  Buffer.add_int32_be buffer (String.length key |> Int32.of_int);
  Buffer.add_string buffer key;
  Buffer.add_int32_be buffer (String.length value |> Int32.of_int);
  Buffer.add_string buffer value;
  Buffer.contents buffer

let decode_set_command (buffer : string) : set_command =
  let key_len = Int32.to_int (String.get_int32_be buffer 0) in
  let key = String.sub buffer 4 key_len in
  let value_len = Int32.to_int (String.get_int32_be buffer (4 + key_len)) in
  let value = String.sub buffer (4 + key_len + 4) value_len in
  { key; value }

let create () : t = { entries = Hashtbl.create 0 }

let apply (kv : t) (entry : Protocol.entry) : unit =
  let command = decode_set_command entry.data in
  Hashtbl.replace kv.entries command.key command.value

let%test_unit "apply" =
  let kv = create () in
  let buffer = Buffer.create 0 in
  Buffer.add_int32_be buffer 3l;
  Buffer.add_string buffer "key";
  Buffer.add_int32_be buffer 5l;
  Buffer.add_string buffer "value";
  apply kv { term = 1L; data = Buffer.contents buffer };
  assert (Hashtbl.find kv.entries "key" = "value");
  apply kv { term = 1L; data = Buffer.contents buffer };
  assert (Hashtbl.find kv.entries "key" = "value")
