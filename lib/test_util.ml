let uuid_v4 () : string =
  Stdlib.Random.self_init ();
  Uuidm.v4_gen (Stdlib.Random.get_state ()) () |> Uuidm.to_string

let temp_dir () : string = Printf.sprintf "/tmp/ocaml-raft/%s" (uuid_v4 ())
