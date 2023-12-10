type t = {
  persistent_state_file_in : in_channel;
  persistent_state_file_out : out_channel;
  mutable last_log_term : Protocol.term;
  mutable last_log_index : int64;
}

type config = { 
  (** Directory used to store Raft files *)   
  dir : string 
}

(** A log entry ready in the format that will is stored on disk *)
type encoded_entry = {
  (** Data that is stored before each log entry *)
  header_bytes: string;
  header_checksum: int64;

  (** Encoded log entry data *)
  data_bytes: string;
  data_checksum: int64
}


(** How many bytes each log entry occupies on disk *)
let page_size_in_bytes = 256L
(** The name given to the file that is used to store the last term the replica has seen and who it voted for *)
let state_file_name = "state.raft"
(** The extension given to files that are used to store log entries *)
let log_file_extension = ".data"

let create (config : config) : t =
  (*
     Directories are files on unix.
     Create the directory where data files will be stored.
  *)
  if not (Sys.file_exists config.dir) then
    Core_unix.mkdir_p ~perm:0o777 config.dir;

  (* TODO: fetch latest filename sequence number *)
 
  let persistent_state_file_path =
    Printf.sprintf "%s/%s" config.dir state_file_name
  in

  {
    persistent_state_file_in =
      In_channel.open_gen
        [ In_channel.Open_creat; In_channel.Open_rdonly ]
        0o777 persistent_state_file_path;
    persistent_state_file_out =
      Out_channel.open_gen
        [ Out_channel.Open_creat; Out_channel.Open_append ]
        0o777 persistent_state_file_path;
    last_log_term = 0L;
    last_log_index = 0L;
  }

let initial_state (storage : t) : Protocol.initial_state =
  let contents = In_channel.input_all storage.persistent_state_file_in in

  if String.length contents > 0 then
    let pieces = String.split_on_char '/' contents in

    {
      current_term = Int64.of_string (List.nth pieces 0);
      voted_for =
        (match List.nth pieces 1 with
        | "-1" -> None
        | replica_id -> Some (int_of_string replica_id));
    }
  else { current_term = 0L; voted_for = None }

let sequence_number_from_filename(filename: string): int =
  String.split_on_char '_' filename
  |> List.rev 
  |> List.hd 
  |> int_of_string 

let latest_filename_sequence_number (data_dir:string): int option = 
    Array.fold_left (fun latest_sequence_number filename -> 
      match  Filename.chop_suffix_opt ~suffix:log_file_extension filename with 
      | None -> latest_sequence_number;
      | Some filename -> 
        let sequence_number = sequence_number_from_filename filename in 
          match latest_sequence_number with 
          | None -> Some sequence_number;
          | Some(n) -> Some (max n sequence_number)
        
      ) None (Sys.readdir data_dir)
  
let rec iter (f: Protocol.entry -> unit) (file: in_channel): unit  =
  (* Start from the beginning of the file *)
  seek_in file 0;

  let buffer = Bytes.make (Int64.to_int page_size_in_bytes) '0' in 
  match In_channel.really_input file buffer 0 (Int64.to_int page_size_in_bytes) with
  (* End of file reached, stop recursion *)
  | None -> ()
  (* There are more entries, keep iterating *)
  | Some () -> 
    (* TODO: decode entry *)
    iter f file

(* let last_log_entry_(data_dir: string) : Protocol.entry option = 
match latest_filename_sequence_number data_dir with 
| None -> None 
| Some sequence_number -> 
    let file_path = Printf.sprintf "%s/%d" data_dir sequence_number in 
    let file_in = In_channel.open_gen
    [ In_channel.Open_creat; In_channel.Open_rdonly ]
      0o777 file_path in 
     assert false *)

let last_log_term (storage : t) : Protocol.term = storage.last_log_term
let last_log_index (storage : t) : int64 = storage.last_log_index

(** Writes [entry] and a header to buffers. Returns the buffers and the checksums *)
let encode_entry (entry: Protocol.entry): encoded_entry = 
  let header_buffer = Buffer.create 0 in

  Buffer.add_int64_le header_buffer entry.term;
  Buffer.add_int64_be header_buffer
    (Int64.of_int (String.length entry.data));

  let header_checksum =
    Checkseum.Crc32.digest_string
      (Buffer.contents header_buffer)
      0
      (Buffer.length header_buffer)
      Checkseum.Crc32.default
  in

  let data_checksum =
    Checkseum.Crc32.digest_string entry.data 0 (String.length entry.data)
      Checkseum.Crc32.default
  in

  {
    header_bytes = Buffer.contents header_buffer;
    header_checksum = Checkseum.Crc32.to_int64 header_checksum;
    data_bytes = entry.data;
    data_checksum = Buffer.contents data_checksum;
  }

(** Reads an entry from [in_chan] *)
let read_entry (in_chan: in_channel): Protocol.entry option = 
  let bytes = Bytes.make (Int64.to_int page_size_in_bytes) '0' in 
  match In_channel.really_input in_chan bytes 0 (Int64.to_int page_size_in_bytes) with 
  (* No entry found *)
  | None -> None 
  (* An entry exists *)
  | Some () -> 
      let header_checksum = Bytes.get_int64_be bytes 0 in 
      (* Header values *)
      let term = Bytes.get_int64_be bytes 8 in 
      let data_len = Int64.to_int(Bytes.get_int64_be bytes 16) in
      let data_checksum = Bytes.get_int64_be bytes 24 in 
      let data = Bytes.sub_string bytes 32 data_len in 
      Some {term = term; data = data}
  

(** Writes [entry] to [out_chan] *)
let write_entry (out_chan: out_channel) (entry: Protocol.entry): unit =  
  let header_buffer = Buffer.create 0 in

      Buffer.add_int64_le header_buffer entry.term;
      Buffer.add_int64_be header_buffer
        (Int64.of_int (String.length entry.data));

      let header_checksum =
        Checkseum.Crc32.digest_string
          (Buffer.contents header_buffer)
          0
          (Buffer.length header_buffer)
          Checkseum.Crc32.default
      in

      let data_checksum =
        Checkseum.Crc32.digest_string entry.data 0 (String.length entry.data)
          Checkseum.Crc32.default
      in

      Out_channel.output_string out_chan
        (Optint.to_string header_checksum);

      Out_channel.output_string out_chan
        (Buffer.contents header_buffer);

      Out_channel.output_string out_chan
        (Optint.to_string data_checksum);

      Out_channel.output_string out_chan entry.data;

(* TODO: store entries with len > page size in other files and point to them *)
(** Stores [entries] on disk *)
let append_entries (storage : t) (entries : Protocol.entry list) : unit =
  let new_entries_start_at_offset =
    Int64.mul storage.last_log_index page_size_in_bytes
  in

  (* Move the pointer just after the latest entry that was successfully stored. *)
  seek_out storage.persistent_state_file_out
    (Int64.to_int new_entries_start_at_offset);

  List.iter (fun entry -> write_entry storage.persistent_state_file_out entry) entries;

  Out_channel.flush storage.persistent_state_file_out;

  storage.last_log_index <-
    Int64.add storage.last_log_index (Int64.of_int (List.length entries));

  (* List.nth and List.length are fine assuming that only a small number of entries are stored at a time. *)
  let last_entry = List.nth entries (List.length entries - 1) in
  storage.last_log_term <- last_entry.term

let persist (storage : t) (state : Protocol.persistent_state) : unit =
  let contents =
    Printf.sprintf "%s/%s"
      (Int64.to_string state.current_term)
      (match state.voted_for with
      | None -> "-1"
      | Some replica_id -> string_of_int replica_id)
  in
  seek_out storage.persistent_state_file_out 0;
  Out_channel.output_string storage.persistent_state_file_out contents;
  Out_channel.flush storage.persistent_state_file_out

let%test_unit "create: fetches the last log term and last log index from disk" =
  assert false

let%test_unit "initial_state: returns the default initial state on first boot \
               (there's no file on disk)" =
  let storage = create { dir = Test_util.temp_dir () } in

  let initial_state = initial_state storage in

  let expected : Protocol.initial_state =
    { current_term = 0L; voted_for = None }
  in

  assert (expected = initial_state)

let%test_unit "last_log_term: returns the term of the last log entry" =
  (* Given an empty log *)
  let storage = create { dir = Test_util.temp_dir () } in

  (* The last log term is 0 *)
  assert (0L = last_log_term storage);

  (* Given a non-empty log *)
  append_entries storage
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
  append_entries storage
    [
      { term = 0L; data = "0" };
      { term = 1L; data = "1" };
      { term = 2L; data = "2" };
      { term = 2L; data = "3" };
    ];

  (* The last log index is the index of the last log entry *)
  assert (4L = last_log_index storage)

let%test_unit "persist: stores persistent state on disk" =
  let temp_dir = Test_util.temp_dir () in
  let storage = create { dir = temp_dir } in

  let state : Protocol.persistent_state =
    { current_term = 1L; voted_for = Some 10 }
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

let%test_unit "append entries: updates last log term and last log index" =
  let storage = create { dir = Test_util.temp_dir () } in

  append_entries storage [ { term = 2L; data = "1" } ];

  assert (2L = last_log_term storage);
  (* The first entry starts at index 1. *)
  assert (1L = last_log_index storage)
