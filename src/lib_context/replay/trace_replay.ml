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

open Irmin.Export_for_backends
open Bench_common
include Trace_replay_intf
module Def = Trace_definitions.Replayable_actions_trace
module Def_stats = Trace_definitions.Stats_trace
module Time = Tezos_base.Time
module TzPervasives = Tezos_base.TzPervasives
module Block_hash = TzPervasives.Block_hash
module Block_metadata_hash = TzPervasives.Block_metadata_hash
module Chain_id = TzPervasives.Chain_id
module Context_hash = TzPervasives.Context_hash
module Operation_metadata_list_list_hash =
  TzPervasives.Operation_metadata_list_list_hash
module Protocol_hash = TzPervasives.Protocol_hash
module Test_chain_status = TzPervasives.Test_chain_status
module TzString = TzPervasives.TzString

let ( // ) = Filename.concat

let rec recursively_iter_files_in_directory f directory =
  Sys.readdir directory |> Array.to_list
  |> List.map (fun fname -> directory // fname)
  |> List.iter (fun p ->
         if Sys.is_directory p then recursively_iter_files_in_directory f p
         else f p)

let chmod_ro p = Unix.chmod p 0o444

let chmod_rw p = Unix.chmod p 0o644

let exec_cmd cmd args =
  let cmd = Filename.quote_command cmd args in
  Logs.info (fun l -> l "Executing %s" cmd) ;
  let err = Sys.command cmd in
  if err <> 0 then Fmt.failwith "Got error code %d for %s" err cmd

let should_check_hashes config =
  config.path_conversion = `None
  && config.inode_config = (32, 256)
  && config.empty_blobs = false

(* TODO: Decide what to do about synthetic flattening *)

(* let is_hex_char = function
 *   | '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' -> true
 *   | _ -> false
 *
 * let is_2char_hex s =
 *   if String.length s <> 2 then false
 *   else s |> String.to_seq |> List.of_seq |> List.for_all is_hex_char
 *
 * let all_6_2char_hex a b c d e f =
 *   is_2char_hex a
 *   && is_2char_hex b
 *   && is_2char_hex c
 *   && is_2char_hex d
 *   && is_2char_hex e
 *   && is_2char_hex f
 *
 * let is_30char_hex s =
 *   if String.length s <> 30 then false
 *   else s |> String.to_seq |> List.of_seq |> List.for_all is_hex_char
 *
 * (\** This function flattens all the 6 step-long chunks forming 40 byte-long
 *     hashes to a single step.
 *
 *     Those flattenings are performed during the trace replay, i.e. they count in
 *     the total time.
 *
 *     If a path contains 2 or more of those patterns, only the leftmost one is
 *     converted.
 *
 *     A chopped hash has this form
 *
 *     {v ([0-9a-f]{2}/){5}[0-9a-f]{30} v}
 *
 *     and is flattened to that form
 *
 *     {v [0-9a-f]{40} v} *\)
 * let flatten_v0 key =
 *   let rec aux rev_prefix suffix =
 *     match suffix with
 *     | a :: b :: c :: d :: e :: f :: tl
 *       when is_2char_hex a
 *            && is_2char_hex b
 *            && is_2char_hex c
 *            && is_2char_hex d
 *            && is_2char_hex e
 *            && is_30char_hex f ->
 *         let mid = a ^ b ^ c ^ d ^ e ^ f in
 *         aux (mid :: rev_prefix) tl
 *     | hd :: tl -> aux (hd :: rev_prefix) tl
 *     | [] -> List.rev rev_prefix
 *   in
 *   aux [] key
 *
 * (\** This function removes from the paths all the 6 step-long hashes of this form
 *
 *     {v ([0-9a-f]{2}/){6} v}
 *
 *     Those flattenings are performed during the trace replay, i.e. they count in
 *     the total time.
 *
 *     The paths in tezos:
 *     https://www.dailambda.jp/blog/2020-05-11-plebeia/#tezos-path
 *
 *     Tezos' PR introducing this flattening:
 *     https://gitlab.com/tezos/tezos/-/merge_requests/2771 *\)
 * let flatten_v1 = function
 *   | "data" :: "contracts" :: "index" :: a :: b :: c :: d :: e :: f :: tl
 *     when all_6_2char_hex a b c d e f -> (
 *       match tl with
 *       | hd :: "delegated" :: a :: b :: c :: d :: e :: f :: tl
 *         when all_6_2char_hex a b c d e f ->
 *           "data" :: "contracts" :: "index" :: hd :: "delegated" :: tl
 *       | _ -> "data" :: "contracts" :: "index" :: tl)
 *   | "data" :: "big_maps" :: "index" :: a :: b :: c :: d :: e :: f :: tl
 *     when all_6_2char_hex a b c d e f ->
 *       "data" :: "big_maps" :: "index" :: tl
 *   | "data" :: "rolls" :: "index" :: _ :: _ :: tl ->
 *       "data" :: "rolls" :: "index" :: tl
 *   | "data" :: "rolls" :: "owner" :: "current" :: _ :: _ :: tl ->
 *       "data" :: "rolls" :: "owner" :: "current" :: tl
 *   | "data" :: "rolls" :: "owner" :: "snapshot" :: a :: b :: _ :: _ :: tl ->
 *       "data" :: "rolls" :: "owner" :: "snapshot" :: a :: b :: tl
 *   | l -> l
 *
 * let flatten_op ~flatten_path = function
 *   (\* | _ -> failwith "super" *\)
 *   | Def.Checkout _ as op -> op
 *   | Add op -> Add { op with key = flatten_path op.key }
 *   | Remove (keys, in_ctx_id, out_ctx_id) ->
 *       Remove (flatten_path keys, in_ctx_id, out_ctx_id)
 *   | Copy op ->
 *       Copy
 *         {
 *           op with
 *           key_src = flatten_path op.key_src;
 *           key_dst = flatten_path op.key_dst;
 *         }
 *   | Find (keys, b, ctx) -> Find (flatten_path keys, b, ctx)
 *   | Mem (keys, b, ctx) -> Mem (flatten_path keys, b, ctx)
 *   | Mem_tree (keys, b, ctx) -> Mem_tree (flatten_path keys, b, ctx)
 *   | Commit _ as op -> op *)

let open_reader max_block_count path_conversion path =
  let (version, header, ops_seq) = Def.open_reader path in
  if max_block_count > header.block_count then
    Logs.info (fun l ->
        l
          "Will only replay %d blocks instead of %d"
          header.block_count
          max_block_count) ;
  let block_count = min max_block_count header.block_count in
  if path_conversion <> `None && header.initial_block <> None then
    invalid_arg
      "Can't use patch_conversion on a replayable trace that doesn't start \
       from genesis." ;
  if path_conversion <> `None then failwith "Re-enable convert_path" ;
  (* let _convert_path =
   *   match path_conversion with
   *   | `None -> Fun.id
   *   | `V1 -> flatten_v1
   *   | `V0 -> flatten_v0
   *   | `V0_and_v1 -> fun p -> flatten_v1 p |> flatten_v0
   * in *)
  let aux (ops_seq, block_sent_count) =
    if block_sent_count >= block_count then None
    else
      match ops_seq () with
      | Seq.Nil ->
          Fmt.failwith
            "Reached the end of replayable trace while loading blocks idx %d. \
             The file was expected to contain %d blocks."
            block_sent_count
            header.block_count
      | Cons (row, ops_sec) -> Some (row, (ops_sec, block_sent_count + 1))
  in
  (version, block_count, header, Seq.unfold aux (ops_seq, 0))

module Make (Store : Store) = struct
  module Stats_collector = Trace_stats_collection.Make (Store)
  module Context = Tezos_context.Context.Make (Store)

  type ('a, 'b) assoc = ('a * 'b) list

  type warm_replay_state = {
    stats : Stats_collector.t;
    index : Context.index;
    mutable contexts : (int64, Context.t) assoc;
    mutable trees : (int64, Context.tree) assoc;
    mutable hash_corresps : (Def.hash, Context_hash.t) assoc;
    check_hashes : bool;
    empty_blobs : bool;
    block_count : int;
    mutable current_block_idx : int;
    mutable current_row : Def.row;
    mutable current_event_idx : int;
    mutable recursion_depth : int;
  }

  type cold = [`Cold of config * Stats_collector.t]

  type warm = [`Warm of warm_replay_state]

  (** [t] is the type of the replay state.

      Before the very first operation is replayed (i.e. [init]) it is of type
      [cold]. After that operation, and until the end of the replay, it is of
      type [warm].

      The reason for this separation is that the [index] field of
      [warm_replay_state] is only available after the [init] operation.

      [warm_replay_state] is implemented using mutability, it could not be
      implemented with a fully functional scheme because we could not return an
      updated version when replaying [patch_context].

      The 3 dictionaries in [warm_replay_state] are implemented using [assoc]
      instead of [hashtbl] or [map] for performance reason -- these dictionaries
      rarely contain more that 1 element. *)
  type t = [cold | warm]

  let check_hash_trace hash_trace hash_replayed =
    let hash_replayed = Context_hash.to_string hash_replayed in
    if hash_trace <> hash_replayed then
      Fmt.failwith "hash replay %s, hash trace %s" hash_replayed hash_trace

  let get_ok = function
    | Error e -> Fmt.(str "%a" (list TzPervasives.pp) e) |> failwith
    | Ok h -> h

  let bad_result rs res_t expected result =
    let pp_res = Repr.pp res_t in
    let ev = rs.current_row.ops.(rs.current_event_idx) in
    Fmt.failwith
      "Cannot reproduce event idx %#d of block idx %#d (%a) expected %a for %a"
      rs.current_block_idx
      rs.current_block_idx
      (Repr.pp Def.event_t)
      ev
      pp_res
      expected
      pp_res
      result

  (** To be called each time lib_context procudes a tree *)
  let on_rhs_tree rs (scope_end, tracker) tree =
    match scope_end with
    | Def.Last_occurence -> ()
    | Will_reoccur -> rs.trees <- (tracker, tree) :: rs.trees

  (** To be called each time lib_context procudes a context *)
  let on_rhs_context rs (scope_end, tracker) context =
    match scope_end with
    | Def.Last_occurence -> ()
    | Will_reoccur -> rs.contexts <- (tracker, context) :: rs.contexts

  (** To be called each time lib_context procudes a commit hash *)
  let on_rhs_hash rs (scope_start, scope_end, hash_trace) hash_replayed =
    if rs.check_hashes then check_hash_trace hash_trace hash_replayed ;
    match (scope_start, scope_end) with
    | (Def.First_instanciation, Def.Last_occurence) -> ()
    | (First_instanciation, Will_reoccur) ->
        rs.hash_corresps <- (hash_trace, hash_replayed) :: rs.hash_corresps
    | (Reinstanciation, Last_occurence) ->
        rs.hash_corresps <- List.remove_assoc hash_trace rs.hash_corresps
    | (Reinstanciation, Will_reoccur) ->
        (* This may occur if 2 commits of the replay have the same hash *)
        ()

  (** To be called each time a tree is passed to lib_context *)
  let on_lhs_tree rs (scope_end, tracker) =
    let v =
      List.assoc tracker rs.trees
      (* Shoudn't fail because it should follow a [save_context]. *)
    in
    if scope_end = Def.Last_occurence then
      rs.trees <- List.remove_assoc tracker rs.trees ;
    v

  (** To be called each time a context is passed to lib_context *)
  let on_lhs_context rs (scope_end, tracker) =
    let v =
      List.assoc tracker rs.contexts
      (* Shoudn't fail because it should follow a [save_context]. *)
    in
    if scope_end = Def.Last_occurence then
      rs.contexts <- List.remove_assoc tracker rs.contexts ;
    v

  (** To be called each time a commit hash is passed to lib_context *)
  let on_lhs_hash rs (scope_start, scope_end, hash_trace) =
    match (scope_start, scope_end) with
    | (Def.Instanciated, Def.Last_occurence) ->
        let v = List.assoc hash_trace rs.hash_corresps in
        rs.hash_corresps <- List.remove_assoc hash_trace rs.hash_corresps ;
        v
    | (Instanciated, Def.Will_reoccur) -> List.assoc hash_trace rs.hash_corresps
    | (Not_instanciated, (Def.Last_occurence | Def.Will_reoccur)) ->
        (* This hash has not been seen yet out of a [commit] or [commit_genesis],
           this implies that [hash_trace] exist in the store prior to replay.

           The typical occurence of that situation is the first checkout of a
           replay starting from an existing store. *)
        Context_hash.of_string_exn hash_trace

  module Tree = struct
    let exec_empty rs ((), tr) =
      let tr' = Context.Tree.empty 42 in
      on_rhs_tree rs tr tr' ;
      Lwt.return_unit

    let exec_of_raw rs (raw, tr) =
      let rec conv = function
        | `Value _ as v -> v
        | `Tree bindings ->
            `Tree
              (bindings |> List.to_seq
              |> Seq.map (fun (k, v) -> (k, conv v))
              |> TzString.Map.of_seq)
      in
      let raw = conv raw in
      let tr' = Context.Tree.of_raw raw in
      on_rhs_tree rs tr tr' ;
      Lwt.return_unit

    let exec_of_value rs (v, tr) =
      let* tr' = Context.Tree.of_value 42 v in
      on_rhs_tree rs tr tr' ;
      Lwt.return_unit

    let exec_mem rs ((tr, k), res) =
      let tr' = on_lhs_tree rs tr in
      let* res' = Context.Tree.mem tr' k in
      if res <> res' then bad_result rs Repr.bool res res' ;
      Lwt.return_unit

    let exec_mem_tree rs ((tr, k), res) =
      let tr' = on_lhs_tree rs tr in
      let* res' = Context.Tree.mem_tree tr' k in
      if res <> res' then bad_result rs Repr.bool res res' ;
      Lwt.return_unit

    let exec_find rs ((tr, k), res) =
      let tr' = on_lhs_tree rs tr in
      let* res' = Context.Tree.find tr' k in
      let res' = Option.is_some res' in
      if res <> res' then bad_result rs Repr.bool res res' ;
      Lwt.return_unit

    let exec_is_empty rs (tr, res) =
      let tr' = on_lhs_tree rs tr in
      let res' = Context.Tree.is_empty tr' in
      if res <> res' then bad_result rs Repr.bool res res' ;
      Lwt.return_unit

    let exec_kind rs (tr, res) =
      let tr' = on_lhs_tree rs tr in
      let res' = Context.Tree.kind tr' in
      if res <> res' then bad_result rs [%typ: [`Tree | `Value]] res res' ;
      Lwt.return_unit

    let exec_hash rs (tr, ()) =
      let tr' = on_lhs_tree rs tr in
      let (_ : Context_hash.t) = Context.Tree.hash tr' in
      Lwt.return_unit

    let exec_equal rs ((tr0, tr1), res) =
      let tr0' = on_lhs_tree rs tr0 in
      let tr1' = on_lhs_tree rs tr1 in
      let res' = Context.Tree.equal tr0' tr1' in
      if res <> res' then bad_result rs Repr.bool res res' ;
      Lwt.return_unit

    let exec_to_value rs (tr, res) =
      let tr' = on_lhs_tree rs tr in
      let* res' = Context.Tree.to_value tr' in
      let res' = Option.is_some res' in
      if res <> res' then bad_result rs Repr.bool res res' ;
      Lwt.return_unit

    let exec_clear rs ((depth, tr), ()) =
      let tr' = on_lhs_tree rs tr in
      Context.Tree.clear ?depth tr' ;
      Lwt.return_unit

    let exec_find_tree rs ((tr0, k), tr1_opt) =
      let tr0' = on_lhs_tree rs tr0 in
      let* tr1'_opt = Context.Tree.find_tree tr0' k in
      match (tr1_opt, tr1'_opt) with
      | (Some tr1, Some tr1') ->
          on_rhs_tree rs tr1 tr1' ;
          Lwt.return_unit
      | (None, None) -> Lwt.return_unit
      | _ ->
          bad_result
            rs
            Repr.bool
            (Option.is_some tr1_opt)
            (Option.is_some tr1'_opt)

    let exec_add rs ((tr0, k, v), tr1) =
      let tr0' = on_lhs_tree rs tr0 in
      let* tr1' = Context.Tree.add tr0' k v in
      on_rhs_tree rs tr1 tr1' ;
      Lwt.return_unit

    let exec_add_tree rs ((tr0, k, tr1), tr2) =
      let tr0' = on_lhs_tree rs tr0 in
      let tr1' = on_lhs_tree rs tr1 in
      let* tr2' = Context.Tree.add_tree tr0' k tr1' in
      on_rhs_tree rs tr2 tr2' ;
      Lwt.return_unit

    let exec_remove rs ((tr0, k), tr1) =
      let tr0' = on_lhs_tree rs tr0 in
      let* tr1' = Context.Tree.remove tr0' k in
      on_rhs_tree rs tr1 tr1' ;
      Lwt.return_unit
  end

  let exec_find_tree rs ((c, k), tr_opt) =
    let c' = on_lhs_context rs c in
    let* tr'_opt = Context.find_tree c' k in
    match (tr_opt, tr'_opt) with
    | (Some tr, Some tr') ->
        on_rhs_tree rs tr tr' ;
        Lwt.return_unit
    | (None, None) -> Lwt.return_unit
    | _ ->
        bad_result rs Repr.bool (Option.is_some tr_opt) (Option.is_some tr'_opt)

  let exec_add_tree rs ((c0, k, tr), c1) =
    let c0' = on_lhs_context rs c0 in
    let tr' = on_lhs_tree rs tr in
    let* c1' = Context.add_tree c0' k tr' in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_mem rs ((c, k), res) =
    let c' = on_lhs_context rs c in
    let* res' = Context.mem c' k in
    if res <> res' then bad_result rs Repr.bool res res' ;
    Lwt.return_unit

  let exec_mem_tree rs ((c, k), res) =
    let c' = on_lhs_context rs c in
    let* res' = Context.mem_tree c' k in
    if res <> res' then bad_result rs Repr.bool res res' ;
    Lwt.return_unit

  let exec_find rs ((c, k), res) =
    let c' = on_lhs_context rs c in
    let* res' = Context.find c' k in
    let res' = Option.is_some res' in
    if res <> res' then bad_result rs Repr.bool res res' ;
    Lwt.return_unit

  let exec_get_protocol rs (c, ()) =
    let c = on_lhs_context rs c in
    let* (_ : Protocol_hash.t) = Context.get_protocol c in
    Lwt.return_unit

  let exec_hash rs ((time, message, c), ()) =
    let time = Time.Protocol.of_seconds time in
    let c = on_lhs_context rs c in
    let (_ : Context_hash.t) = Context.hash ~time ?message c in
    Lwt.return_unit

  let exec_find_predecessor_block_metadata_hash rs (c, ()) =
    let c = on_lhs_context rs c in
    let* (_ : Block_metadata_hash.t option) =
      Context.find_predecessor_block_metadata_hash c
    in
    Lwt.return_unit

  let exec_find_predecessor_ops_metadata_hash rs (c, ()) =
    let c = on_lhs_context rs c in
    let* (_ : Operation_metadata_list_list_hash.t option) =
      Context.find_predecessor_ops_metadata_hash c
    in
    Lwt.return_unit

  let exec_get_test_chain rs (c, ()) =
    let c = on_lhs_context rs c in
    let* (_ : Test_chain_status.t) = Context.get_test_chain c in
    Lwt.return_unit

  let exec_exists rs (hash, res) =
    let hash = on_lhs_hash rs hash in
    let* res' = Context.exists rs.index hash in
    if res <> res' then bad_result rs Repr.bool res res' ;
    Lwt.return_unit

  let exec_retrieve_commit_info rs (hash, res) =
    let hash = on_lhs_hash rs hash in
    let* res' = Context.retrieve_commit_info rs.index hash in
    let res' = Result.is_ok res' in
    if res <> res' then bad_result rs Repr.bool res res' ;
    Lwt.return_unit

  let exec_add rs ((c0, k, v), c1) =
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.add c0' k v in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_remove rs ((c0, k), c1) =
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.remove c0' k in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_add_protocol rs ((c0, h), c1) =
    let h = Protocol_hash.of_string_exn h in
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.add_protocol c0' h in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_add_predecessor_block_metadata_hash rs ((c0, h), c1) =
    let h = Block_metadata_hash.of_string_exn h in
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.add_predecessor_block_metadata_hash c0' h in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_add_predecessor_ops_metadata_hash rs ((c0, h), c1) =
    let h = Operation_metadata_list_list_hash.of_string_exn h in
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.add_predecessor_ops_metadata_hash c0' h in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_add_test_chain rs ((c0, s), c1) =
    let s =
      match s with
      | Def.Not_running -> Test_chain_status.Not_running
      | Forking {protocol; expiration} ->
          let protocol = Protocol_hash.of_string_exn protocol in
          let expiration = Time.Protocol.of_seconds expiration in
          Test_chain_status.Forking {protocol; expiration}
      | Running {chain_id; genesis; protocol; expiration} ->
          let chain_id = Chain_id.of_string_exn chain_id in
          let genesis = Block_hash.of_string_exn genesis in
          let protocol = Protocol_hash.of_string_exn protocol in
          let expiration = Time.Protocol.of_seconds expiration in
          Test_chain_status.Running {chain_id; genesis; protocol; expiration}
    in
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.add_test_chain c0' s in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_remove_test_chain rs (c0, c1) =
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.remove_test_chain c0' in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_fork_test_chain rs ((c0, protocol, expiration), c1) =
    let protocol = Protocol_hash.of_string_exn protocol in
    let expiration = Time.Protocol.of_seconds expiration in
    let c0' = on_lhs_context rs c0 in
    let* c1' = Context.fork_test_chain c0' ~protocol ~expiration in
    on_rhs_context rs c1 c1' ;
    Lwt.return_unit

  let exec_checkout rs (hash, c) =
    let hash = on_lhs_hash rs hash in
    let* c' = Context.checkout rs.index hash in
    let c' = match c' with None -> failwith "Checkout failed" | Some x -> x in
    on_rhs_context rs c c' ;
    Lwt.return_unit

  let exec_clear_test_chain rs (chain_id, ()) =
    let chain_id = Chain_id.of_string_exn chain_id in
    Context.clear_test_chain rs.index chain_id

  let exec_simple_event rs ev =
    let (op_tag, future) =
      match ev with
      | Def.Tree ev -> (
          match ev with
          | Empty data -> (`Tree `Empty, Tree.exec_empty rs data)
          | Of_raw data -> (`Tree `Of_raw, Tree.exec_of_raw rs data)
          | Of_value data -> (`Tree `Of_value, Tree.exec_of_value rs data)
          | Mem data -> (`Tree `Mem, Tree.exec_mem rs data)
          | Mem_tree data -> (`Tree `Mem_tree, Tree.exec_mem_tree rs data)
          | Find data -> (`Tree `Find, Tree.exec_find rs data)
          | Is_empty data -> (`Tree `Is_empty, Tree.exec_is_empty rs data)
          | Kind data -> (`Tree `Kind, Tree.exec_kind rs data)
          | Hash data -> (`Tree `Hash, Tree.exec_hash rs data)
          | Equal data -> (`Tree `Equal, Tree.exec_equal rs data)
          | To_value data -> (`Tree `To_value, Tree.exec_to_value rs data)
          | Clear data -> (`Tree `Clear, Tree.exec_clear rs data)
          | Find_tree data -> (`Tree `Find_tree, Tree.exec_find_tree rs data)
          | Add data -> (`Tree `Add, Tree.exec_add rs data)
          | Add_tree data -> (`Tree `Add_tree, Tree.exec_add_tree rs data)
          | Remove data -> (`Tree `Remove, Tree.exec_remove rs data))
      | Find_tree data -> (`Find_tree, exec_find_tree rs data)
      | Add_tree data -> (`Add_tree, exec_add_tree rs data)
      | Mem data -> (`Mem, exec_mem rs data)
      | Mem_tree data -> (`Mem_tree, exec_mem_tree rs data)
      | Find data -> (`Find, exec_find rs data)
      | Get_protocol data -> (`Get_protocol, exec_get_protocol rs data)
      | Hash data -> (`Hash, exec_hash rs data)
      | Find_predecessor_block_metadata_hash data ->
          ( `Find_predecessor_block_metadata_hash,
            exec_find_predecessor_block_metadata_hash rs data )
      | Find_predecessor_ops_metadata_hash data ->
          ( `Find_predecessor_ops_metadata_hash,
            exec_find_predecessor_ops_metadata_hash rs data )
      | Get_test_chain data -> (`Get_test_chain, exec_get_test_chain rs data)
      | Exists data -> (`Exists, exec_exists rs data)
      | Retrieve_commit_info data ->
          (`Retrieve_commit_info, exec_retrieve_commit_info rs data)
      | Add data -> (`Add, exec_add rs data)
      | Remove data -> (`Remove, exec_remove rs data)
      | Add_protocol data -> (`Add_protocol, exec_add_protocol rs data)
      | Add_predecessor_block_metadata_hash data ->
          ( `Add_predecessor_block_metadata_hash,
            exec_add_predecessor_block_metadata_hash rs data )
      | Add_predecessor_ops_metadata_hash data ->
          ( `Add_predecessor_ops_metadata_hash,
            exec_add_predecessor_ops_metadata_hash rs data )
      | Add_test_chain data -> (`Add_test_chain, exec_add_test_chain rs data)
      | Remove_test_chain data ->
          (`Remove_test_chain, exec_remove_test_chain rs data)
      | Fork_test_chain data -> (`Fork_test_chain, exec_fork_test_chain rs data)
      | Checkout data -> (`Checkout, exec_checkout rs data)
      | Clear_test_chain data ->
          (`Clear_test_chain, exec_clear_test_chain rs data)
      | ( Fold_start _ | Fold_end | Fold_step_enter _ | Fold_step_exit _
        | Init _ | Commit_genesis _ | Commit _ | Patch_context_exit _
        | Patch_context_enter _ ) as ev ->
          Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__
    in
    Stats_collector.direct_op_begin rs.stats ;
    let* () = future in
    Stats_collector.direct_op_end rs.stats op_tag ;
    Lwt.return_unit

  let exec_commit_genesis rs ((chain_id, time, protocol), hash) =
    let chain_id = Chain_id.of_string_exn chain_id in
    let time = Time.Protocol.of_seconds time in
    let protocol = Protocol_hash.of_string_exn protocol in
    let specs =
      Def_stats.Commit_op.
        {
          level = rs.current_row.level;
          tzop_count = rs.current_row.tzop_count;
          tzop_count_tx = rs.current_row.tzop_count_tx;
          tzop_count_contract = rs.current_row.tzop_count_contract;
          ev_count = Array.length rs.current_row.ops;
          uses_patch_context = rs.current_row.uses_patch_context;
        }
    in
    let* () = Stats_collector.commit_begin rs.stats None in
    (* Might execute [exec_init @ patch_context]. *)
    let* hash' = Context.commit_genesis ~time ~protocol ~chain_id rs.index in
    let* () = Stats_collector.commit_end rs.stats ~specs None in
    let hash' = get_ok hash' in
    on_rhs_hash rs hash hash' ;
    Lwt.return_unit

  let exec_commit rs ((time, message, c), hash) =
    let time = Time.Protocol.of_seconds time in
    let c = on_lhs_context rs c in
    let tree_opt = Some (Context.Private.store_tree c) in
    let specs =
      Def_stats.Commit_op.
        {
          level = rs.current_row.level;
          tzop_count = rs.current_row.tzop_count;
          tzop_count_tx = rs.current_row.tzop_count_tx;
          tzop_count_contract = rs.current_row.tzop_count_contract;
          ev_count = Array.length rs.current_row.ops;
          uses_patch_context = rs.current_row.uses_patch_context;
        }
    in
    let* () = Stats_collector.commit_begin rs.stats tree_opt in
    let* hash' = Context.commit ~time ?message c in
    let* () = Stats_collector.commit_end rs.stats ~specs tree_opt in
    on_rhs_hash rs hash hash' ;
    Lwt.return_unit

  let rec exec_init (config : config) stats (row : Def.row) (readonly, ()) =
    let rsref = ref None in
    let patch_context c' =
      (* Will be called from [exec_commit_genesis] if
         [row.uses_patch_context = true]. *)
      let rs = Option.get !rsref in
      exec_patch_context rs c'
    in
    let patch_context =
      if row.uses_patch_context then Some patch_context else None
    in
    Stats_collector.direct_op_begin stats ;
    let* index = Context.init ?readonly ?patch_context config.store_dir in
    Stats_collector.direct_op_end stats `Init ;
    let rs =
      {
        stats;
        index;
        contexts = [];
        trees = [];
        hash_corresps = [];
        check_hashes = should_check_hashes config;
        empty_blobs = config.empty_blobs;
        block_count = config.ncommits_trace;
        current_block_idx = 0;
        current_row = row;
        current_event_idx = 0;
        recursion_depth = 0;
      }
    in
    rsref := Some rs ;
    Lwt.return rs

  and exec_patch_context rs c' =
    Stats_collector.patch_context_begin rs.stats ;
    (match rs.current_row.ops.(rs.current_event_idx) with
    | Commit_genesis _ -> ()
    | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__) ;

    assert (rs.recursion_depth = 0) ;
    rs.recursion_depth <- 1 ;

    let* () =
      rs.current_event_idx <- rs.current_event_idx + 1 ;
      match rs.current_row.ops.(rs.current_event_idx) with
      | Def.Patch_context_enter c ->
          on_rhs_context rs c c' ;
          exec_next_events rs
      | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__
    in

    assert (rs.recursion_depth = 1) ;
    rs.recursion_depth <- 0 ;

    match rs.current_row.ops.(rs.current_event_idx) with
    | Patch_context_exit (c, d) ->
        let _c' : Context.t = on_lhs_context rs c in
        let d' = on_lhs_context rs d in
        Stats_collector.patch_context_end rs.stats ;
        Lwt.return (Ok d')
    | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__

  and exec_fold rs depth c k =
    let c = on_lhs_context rs c in
    let f _k tr' () = exec_fold_step rs tr' in
    Stats_collector.recursive_op_begin rs.stats ;
    let* () = Context.fold ?depth c k ~init:() ~f in
    Stats_collector.recursive_op_end rs.stats `Fold ;

    rs.current_event_idx <- rs.current_event_idx + 1 ;
    match rs.current_row.ops.(rs.current_event_idx) with
    | Fold_end -> Lwt.return_unit
    | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__

  and exec_fold_step rs tr' =
    Stats_collector.recursive_op_exit rs.stats ;
    let recursion_depth = rs.recursion_depth in
    rs.recursion_depth <- recursion_depth + 1 ;

    let* () =
      rs.current_event_idx <- rs.current_event_idx + 1 ;
      match rs.current_row.ops.(rs.current_event_idx) with
      | Def.Fold_step_enter tr ->
          on_rhs_tree rs tr tr' ;
          exec_next_events rs
      | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__
    in

    assert (rs.recursion_depth = recursion_depth + 1) ;
    rs.recursion_depth <- recursion_depth ;

    match rs.current_row.ops.(rs.current_event_idx) with
    | Fold_step_exit tr ->
        let _tr' : Context.tree = on_lhs_tree rs tr in
        Stats_collector.recursive_op_enter rs.stats ;
        Lwt.return_unit
    | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__

  and exec_next_events rs =
    rs.current_event_idx <- rs.current_event_idx + 1 ;
    let events = rs.current_row.Def.ops in
    let commit_idx = Array.length events - 1 in
    let i = rs.current_event_idx in
    let ev = events.(i) in
    if i = commit_idx then (
      assert (rs.recursion_depth = 0) ;
      match ev with
      | Def.Commit data -> exec_commit rs data
      | Commit_genesis data -> exec_commit_genesis rs data
      | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__)
    else
      match ev with
      | Fold_start (x, y, z) ->
          let* () = exec_fold rs x y z in
          (exec_next_events [@tailcall]) rs
      | Patch_context_exit (_, _) ->
          (* Will destack to [exec_patch_context] *)
          Lwt.return_unit
      | Fold_step_exit _ ->
          (* Will destack to [exec_fold_step] *)
          Lwt.return_unit
      | _ ->
          let* () = exec_simple_event rs ev in
          (exec_next_events [@tailcall]) rs

  let exec_block : [< t] -> _ -> _ -> warm Lwt.t =
   fun t row block_idx ->
    let exec_very_first_event config stats =
      assert (block_idx = 0) ;
      let events = row.Def.ops in
      let ev = events.(0) in
      match ev with
      | Def.Init data -> exec_init config stats row data
      | ev -> Fmt.failwith "Got %a at %s" (Repr.pp Def.event_t) ev __LOC__
    in
    match t with
    | `Cold (config, stats) ->
        Logs.info (fun l ->
            l
              "exec block idx:%#6d, level:%#d, events:%#7d, tzops:%3d (tx:%3d \
               + misc:%2d) tzcontracts:%3d."
              block_idx
              row.Def.level
              (Array.length row.Def.ops)
              row.Def.tzop_count
              row.Def.tzop_count_tx
              (row.Def.tzop_count - row.Def.tzop_count_tx)
              row.Def.tzop_count_contract) ;
        let* t = exec_very_first_event config stats in
        let* () = exec_next_events t in
        Lwt.return (`Warm t)
    | `Warm rs ->
        if
          block_idx mod 250 = 0
          || block_idx + 1 = rs.block_count
          || Array.length row.Def.ops > 35_000
          || List.length rs.trees > 0
          || List.length rs.contexts > 0
          || List.length rs.hash_corresps <> 1
        then
          Logs.info (fun l ->
              l
                "exec block idx:%#6d, level:%#d, events:%#7d, tzops:%3d \
                 (tx:%3d + misc:%2d) tzcontracts:%3d. tree/context/hash \
                 caches:%d/%d/%d."
                block_idx
                row.Def.level
                (Array.length row.Def.ops)
                row.Def.tzop_count
                row.Def.tzop_count_tx
                (row.Def.tzop_count - row.Def.tzop_count_tx)
                row.Def.tzop_count_contract
                (List.length rs.trees)
                (List.length rs.contexts)
                (List.length rs.hash_corresps)) ;
        rs.current_block_idx <- block_idx ;
        rs.current_row <- row ;
        rs.current_event_idx <- -1 ;
        let* () = exec_next_events rs in
        Lwt.return (`Warm rs)

  let exec_blocks config stats row_seq : warm Lwt.t =
    with_progress_bar
      ~message:"Replaying trace"
      ~n:config.ncommits_trace
      ~unit:"commits"
    @@ fun prog ->
    let rec aux t commit_idx row_seq =
      match row_seq () with
      | Seq.Nil -> (
          match t with
          | `Cold _ -> assert false
          | `Warm _ as t ->
              Lwt.return t (* on_end () >|= fun () -> commit_idx *))
      | Cons (row, row_seq) ->
          let* t = exec_block t row commit_idx in
          let t = (t :> t) in
          (* let* () = on_commit i (Option.get t.latest_commit) in *)
          prog 1 ;
          aux t (commit_idx + 1) row_seq
    in
    aux (`Cold (config, stats)) 0 row_seq

  let run ext_config config =
    (* TODO: Decide what to do about [ext_config] and suck. I.e. reimplement
       layered store replay.*)
    (* let* repo, on_commit, on_end, repo_pp = Store.create_repo' ext_config in *)
    let check_hashes = should_check_hashes config in
    let store_dir = config.store_dir in
    let stats_path = Filename.concat config.artefacts_dir "stats_trace.repr" in
    ignore ext_config ;
    Logs.info (fun l ->
        l
          "Will %scheck commit hashes against reference."
          (if check_hashes then "" else "NOT ")) ;
    Logs.info (fun l ->
        l
          "Will %skeep irmin store at the end."
          (if config.keep_store then "" else "NOT ")) ;
    Logs.info (fun l ->
        l
          "Will %skeep stat trace at the end."
          (if config.keep_stats_trace then "" else "NOT ")) ;
    Logs.info (fun l ->
        l
          "Will %ssave a custom message in stats trace."
          (if config.stats_trace_message <> None then "" else "NOT ")) ;
    prepare_artefacts_dir config.artefacts_dir ;
    if Sys.file_exists store_dir then
      invalid_arg "Can't open irmin-pack store. Destination already exists" ;

    (* 1. First open the replayable trace, *)
    let (replayable_trace_version, block_count, header, row_seq) =
      open_reader
        config.ncommits_trace
        config.path_conversion
        config.commit_data_file
    in
    let config = {config with ncommits_trace = block_count} in

    let run () =
      (match config.startup_store_type with
      | `Fresh -> ()
      | `Copy_from origin ->
          (* 2 - then make a copy of the reference RO store, *)
          exec_cmd "cp" ["-r"; origin; store_dir] ;
          recursively_iter_files_in_directory chmod_rw store_dir) ;
      let stats =
        (* 3 - and instanciate the stats collector, *)
        let c =
          let (entries, stable_hash) = config.inode_config in
          Trace_definitions.Stats_trace.
            {
              setup =
                `Replay
                  {
                    path_conversion = config.path_conversion;
                    artefacts_dir = config.artefacts_dir;
                    initial_block_level = Option.map fst header.initial_block;
                    replayable_trace_version;
                    message = config.stats_trace_message;
                  };
              inode_config = (entries, entries, stable_hash);
              store_type = config.store_type;
            }
        in
        Stats_collector.create_file stats_path c store_dir
      in

      (* 4 - now launch the full replay, *)
      let* (`Warm replay_state) = exec_blocks config stats row_seq in

      (* 5 - and close the various things open, *)
      Logs.info (fun l -> l "Closing repo...") ;
      Stats_collector.close_begin stats ;
      let+ () = Context.close replay_state.index in
      Stats_collector.close_end stats ;
      Stats_collector.close stats ;

      if not config.no_summary then (
        Logs.info (fun l -> l "Computing summary...") ;
        ignore block_count ;
        (* 6 - compute the summary, *)
        Some
          (Trace_stats_summary.summarise ~info:(block_count, true) stats_path))
      else None
    in
    let finalize () =
      (* 7 - remove or preserve the various temporary files, *)
      if config.keep_stats_trace then (
        Logs.info (fun l -> l "Stats trace kept at %s" stats_path) ;
        chmod_ro stats_path)
      else Sys.remove stats_path ;
      if config.keep_store then (
        Logs.info (fun l -> l "Store kept at %s" config.store_dir) ;
        recursively_iter_files_in_directory chmod_ro store_dir)
      else exec_cmd "rm" ["-rf"; store_dir] ;
      Lwt.return_unit
    in

    let+ summary_opt = Lwt.finalize run finalize in
    match summary_opt with
    | Some summary ->
        (* 8 - and finally save and print the summary. *)
        let p = Filename.concat config.artefacts_dir "stats_summary.json" in
        Trace_stats_summary.save_to_json summary p ;
        fun ppf ->
          (* Format.fprintf ppf "\n%t\n%a" repo_pp *)
          Format.fprintf
            ppf
            "\n%a"
            (Trace_stats_summary_pp.pp 5)
            ([""], [summary])
    | None -> fun ppf -> Format.fprintf ppf "super2"

  (* fun ppf -> Format.fprintf ppf "\n%t\n" repo_pp *)
end
