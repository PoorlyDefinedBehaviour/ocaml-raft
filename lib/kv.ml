let traceln fmt = Eio.Std.traceln ("kv: " ^^ fmt)

type t = { entries : (string, string) Hashtbl.t }

let create () : t = { entries = Hashtbl.create 0 }

let apply (kv : t) (entry : Protocol.entry) : unit =
  traceln "applying entry. entry=%s" (Protocol.show_entry entry);
  assert false
