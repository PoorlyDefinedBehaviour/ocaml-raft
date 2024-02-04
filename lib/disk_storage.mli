type t = {
  (* Returns the persistent state stored on disk.
     Returns if the default initial state if there's no state stored on disk. *)
  initial_state : unit -> Protocol.initial_state;
  (* Returns the term of the last log entry. *)
  last_log_term : unit -> Protocol.term;
  (* Returns the index of the last log entry. *)
  last_log_index : unit -> int64;
  (* Returns the index of the first log entry with the latest term seen. *)
  first_log_index_with_latest_term : unit -> int64;
  (* Returns the entry at the index. *)
  entry_at_index : int64 -> Protocol.entry option;
  (* Saves the state on persistent storage. *)
  persist : Protocol.persistent_state -> unit;
  (* Truncates the log starting from the index. *)
  truncate : int64 -> unit;
  (* Appends entries to the log.  *)
  append_entries : int64 -> Protocol.entry list -> unit;
  (* Returns the max number of entries where their size in bytes is less than or equal to the max size. *)
  get_entry_batch :
    from_index:int64 -> max_size_bytes:int -> Protocol.entry list;
}
[@@deriving show]

type config = { 
(** Directory used to store Raft files *)  
dir : string }

val create : config -> t
