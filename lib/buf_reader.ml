(* Extends Eio.Buf_read. *)

include Eio.Buf_read

let read_bool (reader : Eio.Buf_read.t) : bool =
  let value = Eio.Buf_read.any_char reader in
  match value with
  | 't' -> true
  | 'f' -> false
  | c ->
      raise
        (Invalid_argument
           (Printf.sprintf
              "read_bool: expected a char representing a boolean but got: %c" c))

let read_char (reader : Eio.Buf_read.t) = Eio.Buf_read.any_char reader

let read_int64_be (reader : Eio.Buf_read.t) =
  let buffer = Eio.Buf_read.take 8 reader in
  String.get_int64_be buffer 0

let read_int32_be (reader : Eio.Buf_read.t) =
  let buffer = Eio.Buf_read.take 4 reader in
  String.get_int32_be buffer 0

let read_string_of_len (reader : Eio.Buf_read.t) (len : int64) : string =
  Eio.Buf_read.take (Int64.to_int len) reader
