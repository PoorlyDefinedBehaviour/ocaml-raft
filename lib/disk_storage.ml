type t = {
  (* Used to read from the file that has the persistent state. *)
  state_file_in : in_channel;
  (* Used to write to the file that has the persistent state. *)
  state_file_out : out_channel;
  (* Used to read from the file that has the log entries. *)
  log_file_in : Unix.file_descr;
  (* Used to write to the file that has the log entries. *)
  log_file_out : out_channel;
  (* The term of the last log entry. *)
  mutable last_log_term : Protocol.term;
  (* The index of the last log entry. *)
  mutable last_log_index : int64;
  (* The index of the first log entry that had the highest term seen.
     Example:
     Given a log with the entries:
        {index=1; term=1}
        {index=2; term=2}

        The index would be 2.

        If we append one more entry, the index would be 3
        because the entry term is greater than the greatest term
        {index=3; term=3}

        If we append another entry, the index is still 3 because the term is not greater.
       {index=4; term=3}
  *)
  mutable first_log_index_with_latest_term : int64;
}

type config = { (* Directory used to store Raft files *)
                dir : string }

(* Contains the term and the length of the data of a entry. *)
type header = {
  (* The term when the entry was created. *)
  term : int64;
  (* The number of bytes of the entry data. *)
  data_len : int64;
  (* A checksum of the entry data. *)
  data_checksum : int32;
}

(* How many bytes each log entry occupies on disk *)
let page_size_in_bytes = 64L

(* The name given to the file that is used to store the last term the replica has seen and who it voted for *)
let state_file_name = "state.raft"

(* The extension given to files that are used to store log entries *)
let log_file_extension = ".data"

let create (config : config) : t =
  (*
     Directories are files on unix.
     Create the directory where data files will be stored.
  *)
  if not (Sys.file_exists config.dir) then
    Core_unix.mkdir_p ~perm:0o777 config.dir;

  let state_file_pathj = Printf.sprintf "%s/%s" config.dir state_file_name in

  let log_file_path = Printf.sprintf "%s/0%s" config.dir log_file_extension in

  {
    state_file_in =
      In_channel.open_gen
        [ In_channel.Open_creat; In_channel.Open_rdonly ]
        0o777 state_file_pathj;
    state_file_out =
      Out_channel.open_gen
        [ Out_channel.Open_creat; Out_channel.Open_append ]
        0o777 state_file_pathj;
    log_file_in =
      Unix.openfile log_file_path [ Unix.O_CREAT; Unix.O_RDONLY ] 0o777;
    log_file_out =
      Out_channel.open_gen
        [ Out_channel.Open_creat; Out_channel.Open_append ]
        0o777 log_file_path;
    (* TODO: get last_log_entry *)
    last_log_term = 0L;
    last_log_index = 0L;
    first_log_index_with_latest_term = 0L;
  }

let initial_state (storage : t) : Protocol.initial_state =
  seek_in storage.state_file_in 0;

  let contents = In_channel.input_all storage.state_file_in in

  if String.length contents > 0 then
    let pieces = String.split_on_char '/' contents in

    {
      current_term = Int64.of_string (List.nth pieces 0);
      voted_for =
        (match List.nth pieces 1 with
        | "-1" -> None
        | replica_id -> Some (Int32.of_string replica_id));
    }
  else { current_term = 0L; voted_for = None }

(* Returns a checksum of the fields in [header] *)
let checksum_of_header (header : header) : Optint.t =
  let header_buffer = Buffer.create 0 in
  Buffer.add_int64_be header_buffer header.term;
  Buffer.add_int64_be header_buffer header.data_len;

  Checkseum.Crc32.digest_string
    (Buffer.contents header_buffer)
    0
    (Buffer.length header_buffer)
    Checkseum.Crc32.default

(* Returns a checksum of [s]. *)
let checksum_of_string (s : string) : Optint.t =
  Checkseum.Crc32.digest_string s 0 (String.length s) Checkseum.Crc32.default

let header_of_bytes (buffer : bytes) : header =
  let header_checksum = Bytes.get_int32_be buffer 0 in
  (* Header values *)
  let term = Bytes.get_int64_be buffer 4 in
  let data_len = Bytes.get_int64_be buffer 12 in
  let data_checksum = Bytes.get_int32_be buffer 20 in
  let header = { term; data_len; data_checksum } in
  assert (header_checksum = Optint.to_int32 (checksum_of_header header));
  header

let last_log_term (storage : t) : Protocol.term = storage.last_log_term
let last_log_index (storage : t) : int64 = storage.last_log_index

let first_log_index_with_latest_term (storage : t) : int64 =
  storage.first_log_index_with_latest_term

(* Reads an entry from [in_chan] *)
let read_entry (in_chan : in_channel) : Protocol.entry option =
  let bytes = Bytes.make (Int64.to_int page_size_in_bytes) '0' in
  match
    In_channel.really_input in_chan bytes 0 (Int64.to_int page_size_in_bytes)
  with
  (* No entry found *)
  | None -> None
  (* An entry exists *)
  | Some () ->
      let header = header_of_bytes bytes in

      (* Start at index 24 to skip header. *)
      let data = Bytes.sub_string bytes 24 (Int64.to_int header.data_len) in

      assert (header.data_checksum = Optint.to_int32 (checksum_of_string data));

      Some { term = header.term; data }

(* Returns the entry at [index]. *)
let entry_at_index (storage : t) (index : int64) : Protocol.entry option =
  assert (index > 0L);

  if index > storage.last_log_index then None
  else
    (* -1 because the first index is 1 but the first entry is at offset 0 in the file. *)
    let entry_starts_at = Int64.mul (Int64.sub index 1L) page_size_in_bytes in

    (* Seek to the first byte of the entry. *)
    let _ =
      Unix.lseek storage.log_file_in
        (Int64.to_int entry_starts_at)
        Unix.SEEK_SET
    in

    (* Read the entry at the offset. *)
    read_entry (Unix.in_channel_of_descr storage.log_file_in)

(* TODO: test *)
let get_entry_batch (storage : t) ~(from_index : int64) ~(max_size_bytes : int)
    : Protocol.entry list =
  let rec go i size entries =
    match entry_at_index storage i with
    | None -> entries
    | Some entry ->
        let entry_size_bytes = 8 (*term size*) + String.length entry.data in
        let new_size = size + entry_size_bytes in
        if new_size >= max_size_bytes then entries
        else go (Int64.add 1L i) new_size (entry :: entries)
  in

  go from_index 0 []

(* Writes [entry] to [out_chan]. Does not flush [out_chan]. *)
let write_entry (out_chan : out_channel) (entry : Protocol.entry) : unit =
  let header =
    {
      term = entry.term;
      data_len = Int64.of_int (String.length entry.data);
      data_checksum = Optint.to_int32 (checksum_of_string entry.data);
    }
  in

  let header_checksum = checksum_of_header header in

  let buffer = Buffer.create (Int64.to_int page_size_in_bytes) in

  Buffer.add_int32_be buffer (Optint.to_int32 header_checksum);
  Buffer.add_int64_be buffer header.term;
  Buffer.add_int64_be buffer header.data_len;
  Buffer.add_int32_be buffer header.data_checksum;
  Buffer.add_string buffer entry.data;

  (* Fill the buffer with '0' to make sure it fills the page. *)
  Buffer.add_bytes buffer
    (Bytes.init
       (Int64.to_int page_size_in_bytes - Buffer.length buffer)
       (fun _ -> '0'));

  Buffer.output_buffer out_chan buffer

(* Does the actual iteration over the entries in [file]. *)
let rec do_iter ~(f : Protocol.entry -> unit) (file : in_channel) : unit =
  match read_entry file with
  (* End of file reached, stop recursion *)
  | None -> ()
  | Some entry ->
      f entry;
      do_iter ~f file

(* Iterates over the [Protocol.entry]'s in the [file]. *)
let iter ~(f : Protocol.entry -> unit) (file : in_channel) : unit =
  (* Start from the beginning of the file *)
  seek_in file 0;

  do_iter ~f file

let last_log_entry (file : in_channel) : Protocol.entry option =
  let last_entry = ref None in
  iter file ~f:(fun entry -> last_entry := Some entry);
  !last_entry

let truncate (storage : t) (index : int64) : unit =
  let entry_ends_at = Int64.mul index page_size_in_bytes in

  let file_descr = Unix.descr_of_out_channel storage.log_file_out in

  Unix.ftruncate file_descr (Int64.to_int entry_ends_at);
  Unix.fsync file_descr;

  storage.last_log_index <- index;
  storage.last_log_term <-
    (if index = 0L then 0L
     else
       match entry_at_index storage index with
       | None -> 0L
       | Some entry -> entry.term)

(* TODO: store entries with len > page size in other files and point to them *)
(* Stores [entries] on disk *)
let append_entries (storage : t) (previous_log_index : int64)
    (entries : Protocol.entry list) : unit =
  let new_entries_start_at_offset =
    Int64.mul storage.last_log_index page_size_in_bytes
  in

  (* Move the pointer just after the latest entry that was successfully stored.  *)
  seek_out storage.log_file_out (Int64.to_int new_entries_start_at_offset);

  List.iteri
    (fun (i : int) (entry : Protocol.entry) ->
      (* Add 1 because the log is 1-indexed. *)
      let entry_index = Int64.add previous_log_index (Int64.of_int (i + 1)) in

      match entry_at_index storage entry_index with
      (* New entry, append to the log. *)
      | None ->
          write_entry storage.log_file_out entry;
          if entry.term > storage.last_log_term then
            storage.first_log_index_with_latest_term <- entry_index
      | Some existing_entry ->
          (* New entry conflicts with existing entry, truncate the log and append the new entry. *)
          if existing_entry.term <> entry.term then (
            truncate storage (Int64.sub entry_index 1L);
            write_entry storage.log_file_out entry);

          (* If the entry already exists and there's no conflict, do nothing. *)
          ())
    entries;

  Out_channel.flush storage.log_file_out;

  storage.last_log_index <-
    Int64.add storage.last_log_index (Int64.of_int (List.length entries));

  (* List.nth and List.length are fine assuming that only a small number of entries are stored at a time. *)
  (if List.length entries > 0 then
     let last_entry = List.nth entries (List.length entries - 1) in
     storage.last_log_term <- last_entry.term);
  ()

let persist (storage : t) (state : Protocol.persistent_state) : unit =
  let contents =
    Printf.sprintf "%s/%s"
      (Int64.to_string state.current_term)
      (match state.voted_for with
      | None -> "-1"
      | Some replica_id -> Int32.to_string replica_id)
  in
  seek_out storage.state_file_out 0;
  Out_channel.output_string storage.state_file_out contents;
  Out_channel.flush storage.state_file_out

let%test_unit "append entries: updates first log index with latest term" =
  let storage = create { dir = Test_util.temp_dir () } in

  append_entries storage (last_log_index storage)
    [ { term = 1L; data = "1" }; { term = 2L; data = "2" } ];
  assert (storage.first_log_index_with_latest_term = 2L);

  append_entries storage (last_log_index storage) [ { term = 3L; data = "3" } ];

  assert (storage.first_log_index_with_latest_term = 3L);

  append_entries storage (last_log_index storage) [ { term = 3L; data = "4" } ];

  assert (storage.first_log_index_with_latest_term = 3L)

let%test_unit "append entries: updates last log term and last log index" =
  let storage = create { dir = Test_util.temp_dir () } in

  append_entries storage 0L [ { term = 2L; data = "1" } ];

  assert (2L = last_log_term storage);
  (* The first entry starts at index 1. *)
  assert (1L = last_log_index storage);

  append_entries storage (last_log_index storage) [ { term = 3L; data = "1" } ];

  assert (3L = last_log_term storage);
  assert (2L = last_log_index storage)

let%test_unit "append entries: truncates the log on entry conflict" =
  let storage = create { dir = Test_util.temp_dir () } in

  (* Leader adds some entries to the replica's log. *)
  append_entries storage (last_log_index storage)
    [
      { term = 1L; data = "1" };
      { term = 2L; data = "2" };
      { term = 3L; data = "3" };
    ];

  (* Another leader overrides the replica's log. *)
  append_entries storage 2L
    [ { term = 4L; data = "3" }; { term = 4L; data = "4" } ];

  assert (entry_at_index storage 1L = Some { term = 1L; data = "1" });
  assert (entry_at_index storage 2L = Some { term = 2L; data = "2" });
  (* Entry at index 3 has been overwritten. *)
  assert (entry_at_index storage 3L = Some { term = 4L; data = "3" });
  (* Entry at index 4 is new. *)
  assert (entry_at_index storage 4L = Some { term = 4L; data = "4" })

let%test_unit "persist: stores persistent state on disk" =
  let temp_dir = Test_util.temp_dir () in
  let storage = create { dir = temp_dir } in

  let state : Protocol.persistent_state =
    { current_term = 1L; voted_for = Some 10l }
  in

  (* Save the state to disk *)
  persist storage state;

  (* Read the state from disk *)
  let state = initial_state storage in

  (* Should have read the state just stored *)
  let expected : Protocol.initial_state =
    { current_term = state.current_term; voted_for = state.voted_for }
  in

  assert (expected = state);

  (* Reopen the file and read the same state again *)
  let storage = create { dir = temp_dir } in
  let state = initial_state storage in

  assert (expected = state)

let%test_unit "last_log_term: returns the term of the last log entry" =
  (* Given an empty log *)
  let storage = create { dir = Test_util.temp_dir () } in

  (* The last log term is 0 *)
  assert (0L = last_log_term storage);

  (* Given a non-empty log *)
  append_entries storage (last_log_index storage)
    [
      { term = 0L; data = "0" };
      { term = 1L; data = "1" };
      { term = 2L; data = "2" };
    ];

  (* The last log term is the term of the last log entry *)
  assert (2L = last_log_term storage)

let%test_unit "last_log_index: returns the index of the last log entry" =
  (* Given an empty log *)
  let storage = create { dir = Test_util.temp_dir () } in

  (* The last log index is 0 *)
  assert (0L = last_log_index storage);

  (* Given a non-empty log *)
  append_entries storage (last_log_index storage)
    [
      { term = 0L; data = "0" };
      { term = 1L; data = "1" };
      { term = 2L; data = "2" };
      { term = 2L; data = "3" };
    ];

  (* The last log index is the index of the last log entry *)
  assert (4L = last_log_index storage)

let%test_unit "initial_state: returns the default initial state on first boot \
               (there's no file on disk)" =
  let storage = create { dir = Test_util.temp_dir () } in

  let initial_state = initial_state storage in

  let expected : Protocol.initial_state =
    { current_term = 0L; voted_for = None }
  in

  assert (expected = initial_state)

let%test_unit "initial_state: returns the stored state (there's a file on disk)"
    =
  let dir = Test_util.temp_dir () in

  let storage = create { dir } in

  (* Should return the initial state. *)
  assert (initial_state storage = { current_term = 0L; voted_for = None });

  (* Update the state file. *)
  persist storage { current_term = 1L; voted_for = Some 1l };

  (* Read the file again, initial state should be the one stored. *)
  assert (initial_state storage = { current_term = 1L; voted_for = Some 1l });

  (* Reopen the file just to be sure it doesn't get truncated. *)
  let storage = create { dir } in
  assert (initial_state storage = { current_term = 1L; voted_for = Some 1l })

let%test_unit "entry_at_index: returns the entry at the index" =
  let dir = Test_util.temp_dir () in

  let storage = create { dir } in

  (* Log is empty *)
  assert (entry_at_index storage 1L = None);

  (* Append an entry to the log. *)
  append_entries storage (last_log_index storage) [ { term = 1L; data = "1" } ];

  (* Should be able to get the entry we just appended. *)
  assert (entry_at_index storage 1L = Some { term = 1L; data = "1" });

  (* Append another entry to the log. *)
  append_entries storage (last_log_index storage) [ { term = 1L; data = "2" } ];

  (* Should be able to get the entry we just appended and the previous entry. *)
  assert (entry_at_index storage 1L = Some { term = 1L; data = "1" });
  assert (entry_at_index storage 2L = Some { term = 1L; data = "2" });
  assert (entry_at_index storage 3L = None)

let%test_unit "truncate: truncates the log to index" =
  let storage = create { dir = Test_util.temp_dir () } in

  (* Log is empty *)
  assert (entry_at_index storage 1L = None);

  (* Append an entry to the log. *)
  append_entries storage (last_log_index storage)
    [
      { term = 1L; data = "1" };
      { term = 2L; data = "2" };
      { term = 3L; data = "3" };
    ];

  (* Get the entries that were just appended. *)
  assert (entry_at_index storage 1L = Some { term = 1L; data = "1" });
  assert (entry_at_index storage 2L = Some { term = 2L; data = "2" });
  assert (entry_at_index storage 3L = Some { term = 3L; data = "3" });
  assert (last_log_index storage = 3L);
  assert (last_log_term storage = 3L);

  (* Truncate the log. Remove entries after index 1. *)
  truncate storage 1L;

  (* The entries after index 1 should not be in the log anymore. *)
  assert (entry_at_index storage 1L = Some { term = 1L; data = "1" });
  assert (entry_at_index storage 2L = None);
  assert (entry_at_index storage 3L = None);
  assert (last_log_index storage = 1L);
  assert (last_log_term storage = 1L);

  (* Append the entries again. They should go to the end of the log. *)
  append_entries storage (last_log_index storage)
    [ { term = 2L; data = "2" }; { term = 3L; data = "3" } ];

  (* Ensure the entries have been appended and we can read them. *)
  assert (entry_at_index storage 1L = Some { term = 1L; data = "1" });
  assert (entry_at_index storage 2L = Some { term = 2L; data = "2" });
  assert (entry_at_index storage 3L = Some { term = 3L; data = "3" });
  assert (last_log_index storage = 3L);
  assert (last_log_term storage = 3L);

  (* Empty the log. *)
  truncate storage 0L;

  assert (entry_at_index storage 1L = None);
  assert (entry_at_index storage 2L = None);
  assert (entry_at_index storage 3L = None);
  assert (last_log_index storage = 0L);
  assert (last_log_term storage = 0L)
