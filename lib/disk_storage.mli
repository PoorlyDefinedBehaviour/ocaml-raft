type t
type config = { 
(** Directory used to store Raft files *)  
dir : string }

val create : config -> t
val initial_state : t -> Protocol.initial_state
val last_log_term : t -> Protocol.term
val last_log_index : t -> int64
val persist : t -> Protocol.persistent_state -> unit
