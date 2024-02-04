type t = {
  (* Returns a number in the range [min, max).
     Requires min <= max. *)
  gen_range_int64 : int64 -> int64 -> int64;
}
[@@deriving show]

let create ?(seed : int array option) () : t =
  let state =
    match seed with
    | None -> Stdlib.Random.State.make_self_init ()
    | Some seed -> Stdlib.Random.State.make seed
  in

  {
    gen_range_int64 =
      (fun min max ->
        assert (min <= max);
        if min = max then min
        else Int64.add min (Stdlib.Random.State.int64 state (Int64.sub max min)));
  }
