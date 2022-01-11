(*
 * Copyright (c) 2018-2021 Tarides <contact@tarides.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

module Seq : sig
  include module type of Stdlib.Seq

  module Custom : sig
    val take : int -> 'a Seq.t -> 'a list
    val mapi64 : 'a Seq.t -> (int64 * 'a) Seq.t
    val take_up_to : is_last:('a -> bool) -> 'a Seq.t -> 'a Seq.t * 'a list
    val take_until : is_last:('a -> bool) -> 'a Seq.t -> 'a Seq.t
  end
end

(* val operations_of_block_level : int -> Tezos_metrics.block_info Lwt.t *)
(** Retrieve some basic informations on a given {i tezos mainnet} block level by
    querying the tezstats.com JSON API.

    Uses both in-memory and on-disk cache that are never emptied *)

(** See [Trace_stats_summary] for an explanation and an example.

    Heavily inspired by the "repr" library.

    Type parameters:

    - ['res] is the output of [finalise].
    - ['f] is the full contructor that creates a ['res].
    - ['v] is the output of [folder.finalise], one parameter of ['f].
    - ['rest] is ['f] or ['res] or somewhere in between.
    - ['acc] is the accumulator of one folder.
    - ['row] is what needs to be fed to all [folder.accumulate].

    Typical use case:

    {[
      let pf =
        open_ (fun res_a res_b -> my_constructor res_a res_b)
        |+ folder my_acc_a my_accumulate_a my_finalise_a
        |+ folder my_acc_b my_accumulate_b my_finalise_b
        |> seal
      in
      let res = my_row_sequence |> Seq.fold_left accumulate pf |> finalise in
    ]} *)
module Parallel_folders : sig
  (** Section 1/3 - Individual folders *)

  type ('row, 'acc, 'v) folder

  val folder :
    'acc -> ('acc -> 'row -> 'acc) -> ('acc -> 'v) -> ('row, 'acc, 'v) folder
  (** Create one folder to be passed to an open parallel folder using [|+]. *)

  (** Section 2/3 - Open parallel folder *)

  type ('res, 'row, 'v) folders
  type ('res, 'row, 'f, 'rest) open_t

  val open_ : 'f -> ('res, 'row, 'f, 'f) open_t
  (** Start building a parallel folder. *)

  val app :
    ('res, 'row, 'f, 'v -> 'rest) open_t ->
    ('row, 'acc, 'v) folder ->
    ('res, 'row, 'f, 'rest) open_t
  (** Add a folder to an open parallel folder. *)

  val ( |+ ) :
    ('res, 'row, 'f, 'v -> 'rest) open_t ->
    ('row, 'acc, 'v) folder ->
    ('res, 'row, 'f, 'rest) open_t
  (** Alias for [app]. *)

  (** Section 3/3 - Closed parallel folder *)

  type ('res, 'row) t

  val seal : ('res, 'row, 'f, 'res) open_t -> ('res, 'row) t
  (** Stop building a parallel folder.

      Gotcha: It may seal a partially applied [f]. *)

  val accumulate : ('res, 'row) t -> 'row -> ('res, 'row) t
  (** Forward a row to all registered functional folders. *)

  val finalise : ('res, 'row) t -> 'res
  (** Finalise all folders and pass their result to the user-defined function
      provided to [open_]. *)
end
