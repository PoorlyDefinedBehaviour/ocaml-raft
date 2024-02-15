type t = { entries : (string, string) Hashtbl.t }

let create () : t = { entries = Hashtbl.create 0 }

let apply (kv : t) (entry : Protocol.entry) : unit =
  let key_len = Int32.to_int (String.get_int32_be entry.data 0) in
  let key = String.sub entry.data 4 key_len in
  let value_len = Int32.to_int (String.get_int32_be entry.data (4 + key_len)) in
  let value = String.sub entry.data (4 + key_len + 4) value_len in

  Hashtbl.replace kv.entries key value

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
