type t
type config = { 
(** Directory used to store Raft files *)  
dir : string }

val create : config -> t
val initial_state : t -> Protocol.initial_state
val last_log_term : t -> Protocol.term
val last_log_index : t -> int64
val first_log_index_with_latest_term:  t -> int64 
val persist : t -> Protocol.persistent_state -> unit
(* Returns the entry at [index]. *)
val entry_at_index:t -> int64 -> Protocol.entry option
val truncate : t -> int64 -> unit
val append_entries: t -> int64 -> Protocol.entry list -> unit

val get_entry_batch : t -> from_index:int64 -> max_size_bytes:int -> Protocol.entry list
(* Returns the max number of entries where the sum of their size in bytes is less than or equal to [max_size_bytes] *)