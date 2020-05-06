(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2020 Nomadic Labs, <contact@nomadic-labs.com>               *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

open Test_utils

let check_import_invariants ~test_descr ~rolling
    (previously_baked_blocks, exported_block) (imported_chain_store, head) =
  protect
    ~on_error:(fun err ->
      Format.eprintf "Error while checking invariants at: %s" test_descr ;
      Lwt.return (Error err))
    (fun () ->
      (* Check that the head exists with metadata and corresponds to
           the exported block *)
      assert_presence_in_store ~with_metadata:true imported_chain_store [head]
      >>=? fun () ->
      assert_presence_in_store
        ~with_metadata:true
        imported_chain_store
        [exported_block]
      >>=? fun () ->
      Assert.equal_block
        ~msg:("imported head consistency: " ^ test_descr)
        (Store.Block.header exported_block)
        (Store.Block.header head) ;
      (* Check that we possess all the blocks wrt our descriptors *)
      Store.Chain.savepoint imported_chain_store
      >>= fun savepoint ->
      Store.Chain.checkpoint imported_chain_store
      >>= fun checkpoint ->
      Store.Chain.caboose imported_chain_store
      >>= fun caboose ->
      let (expected_present, expected_absent) =
        List.partition
          (fun b ->
            Compare.Int32.(Store.Block.level b <= snd checkpoint)
            && Compare.Int32.(Store.Block.level b >= snd caboose))
          previously_baked_blocks
      in
      assert_presence_in_store
        ~with_metadata:false
        imported_chain_store
        expected_present
      >>=? fun () ->
      assert_absence_in_store imported_chain_store expected_absent
      >>=? fun () ->
      (* Check that the descriptors are consistent *)
      ( if rolling then
        (* In rolling: we expected to have at least the max_op_ttl
              blocks from the head *)
        Store.Block.get_block_metadata imported_chain_store head
        >>=? fun metadata ->
        let max_op_ttl = Store.Block.max_operations_ttl metadata in
        return Int32.(sub (Store.Block.level head) (of_int max_op_ttl))
      else return 0l )
      >>=? fun expected_caboose_level ->
      Assert.equal
        ~msg:("savepoint consistency: " ^ test_descr)
        (Store.Block.level exported_block)
        (snd savepoint) ;
      Assert.equal
        ~msg:("checkpoint consistency: " ^ test_descr)
        (snd savepoint)
        (snd checkpoint) ;
      Assert.equal
        ~msg:("caboose consistency: " ^ __LOC__)
        ~eq:Int32.equal
        ~prn:Int32.to_string
        expected_caboose_level
        (snd caboose) ;
      return_unit)

let export_import ~test_descr (store_dir, context_dir) chain_store
    ~previously_baked_blocks ?exported_block_hash ~rolling =
  check_invariants chain_store
  >>= fun () ->
  let snapshot_dir = store_dir // "snapshot.full" in
  (* No lockfile on the same process, we must enforce waiting the
     merging thread to finish *)
  let block_store = Store.Unsafe.get_block_store chain_store in
  Block_store.await_merging block_store
  >>=? fun () ->
  let exported_block =
    Option.unopt_map
      ~default:(`Alias (`Checkpoint, 0))
      ~f:(fun hash -> `Hash (hash, 0))
      exported_block_hash
  in
  Snapshots.export
    ~rolling
    ~block:exported_block
    ~store_dir
    ~context_dir
    ~chain_name:(Distributed_db_version.Name.of_string "test")
    ~snapshot_dir
    genesis
  >>=? fun () ->
  let dir = store_dir // "imported_store" in
  let dst_store_dir = dir // "store" in
  let dst_context_dir = dir // "context" in
  let block = Option.map ~f:Block_hash.to_b58check exported_block_hash in
  Snapshots.import
    ?block
    ~snapshot_dir
    ~dst_store_dir
    ~dst_context_dir
    ~user_activated_upgrades:[]
    ~user_activated_protocol_overrides:[]
    genesis
  >>=? fun () ->
  Store.init
    ~store_dir:dst_store_dir
    ~context_dir:dst_context_dir
    ~allow_testchains:true
    genesis
  >>=? fun store' ->
  protect
    ~on_error:(fun err ->
      Store.close_store store' >>= fun () -> Lwt.return (Error err))
    (fun () ->
      let chain_store' = Store.main_chain_store store' in
      Store.Chain.current_head chain_store'
      >>= fun head' ->
      ( match exported_block_hash with
      | Some hash ->
          Assert.equal
            ~msg:("export with given hash: " ^ test_descr)
            ~eq:Block_hash.equal
            (Store.Block.hash head')
            hash ;
          Lwt.return head'
      | None ->
          Store.Chain.checkpoint chain_store
          >>= fun checkpoint ->
          Assert.equal
            ~msg:("export checkpoint: " ^ test_descr)
            ~eq:Block_hash.equal
            (Store.Block.hash head')
            (fst checkpoint) ;
          Lwt.return head' )
      >>= fun exported_block ->
      let history_mode = Store.Chain.history_mode chain_store' in
      assert (
        match history_mode with
        | Rolling _ when rolling ->
            true
        | Full _ when not rolling ->
            true
        | _ ->
            false ) ;
      check_import_invariants
        ~test_descr
        ~rolling
        (previously_baked_blocks, exported_block)
        (chain_store', head')
      >>=? fun () -> return (store', chain_store', head'))

let check_baking_continuity ~test_descr ~exported_chain_store
    ~imported_chain_store =
  let open Tezos_protocol_alpha.Protocol.Alpha_context in
  Store.Chain.current_head imported_chain_store
  >>= fun imported_head ->
  Alpha_utils.get_constants imported_chain_store imported_head
  >>=? fun {Constants.parametric = {blocks_per_cycle; preserved_cycles; _}; _} ->
  let imported_history_mode = Store.Chain.history_mode imported_chain_store in
  let imported_offset =
    match imported_history_mode with
    | History_mode.Rolling {offset} | Full {offset} ->
        offset
    | Archive ->
        assert false
  in
  Store.Chain.current_head exported_chain_store
  >>= fun export_store_head ->
  let level_to_reach =
    let min_nb_blocks_to_bake =
      Int32.(
        of_int
          (to_int blocks_per_cycle * (preserved_cycles + imported_offset + 2)))
    in
    Compare.Int32.(
      max
        (Store.Block.level export_store_head)
        (Int32.add (Store.Block.level imported_head) min_nb_blocks_to_bake))
  in
  (* Bake until we have enough cycles to reach our offset (and a bit more) *)
  (* Check invariants after every baking *)
  let rec loop head = function
    | 0 ->
        return head
    | n ->
        Alpha_utils.bake imported_chain_store head
        >>=? fun new_head ->
        check_invariants imported_chain_store
        >>= fun () -> loop new_head (n - 1)
  in
  let nb_blocks_to_bake_in_import =
    Int32.(to_int (sub level_to_reach (Store.Block.level imported_head)))
  in
  loop imported_head nb_blocks_to_bake_in_import
  >>=? fun last' ->
  (* Also bake with the exported store so we make sure we bake the same blocks *)
  Store.Chain.current_head exported_chain_store
  >>= fun exported_head ->
  ( if Compare.Int32.(Store.Block.level export_store_head < level_to_reach) then
    let nb_blocks_to_bake_in_export =
      Int32.(to_int (sub level_to_reach (Store.Block.level export_store_head)))
    in
    Alpha_utils.bake_n
      exported_chain_store
      nb_blocks_to_bake_in_export
      exported_head
    >>=? fun (_blocks, last) -> return last
  else Store.Block.read_block_by_level exported_chain_store level_to_reach )
  >>=? fun last ->
  Assert.equal_block
    ~msg:("check both head after baking: " ^ test_descr)
    (Store.Block.header last)
    (Store.Block.header last') ;
  (* Check that the checkpoint are the same *)
  Store.Chain.checkpoint exported_chain_store
  >>= fun checkpoint ->
  Store.Chain.checkpoint imported_chain_store
  >>= fun checkpoint' ->
  Assert.equal
    ~msg:("checkpoint equality: " ^ test_descr)
    ~prn:(fun (hash, level) ->
      Format.asprintf "%a (%ld)" Block_hash.pp hash level)
    checkpoint
    checkpoint' ;
  return_unit

let test store_path store ~test_descr ?exported_block_level
    ~nb_blocks_to_bake_before_export ~rolling =
  let chain_store = Store.main_chain_store store in
  Store.Chain.genesis_block chain_store
  >>= fun genesis_block ->
  Alpha_utils.bake_n chain_store nb_blocks_to_bake_before_export genesis_block
  >>=? fun (previously_baked_blocks, _current_head) ->
  let consistency_check () =
    Store.Chain.checkpoint chain_store
    >>= fun checkpoint ->
    Store.Chain.savepoint chain_store
    >>= fun savepoint ->
    Store.Chain.caboose chain_store
    >>= fun caboose ->
    let import_export exported_block_hash =
      export_import
        ~test_descr
        store_path
        chain_store
        ~rolling
        ?exported_block_hash
        ~previously_baked_blocks
    in
    let expected_level =
      Option.unopt ~default:(snd checkpoint) exported_block_level
    in
    let open Snapshots in
    Store.Block.read_block_by_level_opt chain_store expected_level
    >>= fun block_opt ->
    ( match block_opt with
    | None ->
        return_some `Unknown
    | Some block ->
        if Compare.Int32.(expected_level = Store.Block.level genesis_block)
        then return_some `Genesis
        else if Compare.Int32.(expected_level = snd caboose) then
          return_some `Caboose
        else if Compare.Int32.(expected_level = snd savepoint) then
          return_some `Pruned_pred
        else if Compare.Int32.(expected_level < snd savepoint) then
          if Compare.Int32.(expected_level < snd caboose) then
            return_some `Unknown
          else return_some `Pruned
        else
          Store.Block.get_block_metadata chain_store block
          >>=? fun metadata ->
          let min_level =
            Compare.Int32.(
              max
                (Store.Block.level genesis_block)
                Int32.(
                  sub
                    (Store.Block.level block)
                    (of_int (Store.Block.max_operations_ttl metadata))))
          in
          if Compare.Int32.(min_level < snd caboose) then
            return_some `Not_enough_pred
          else (* Should not fail *)
            return_none )
    >>=? function
    | None ->
        (* Normal behavior *)
        let block = Option.unopt_assert ~loc:__POS__ block_opt in
        let hash =
          Option.map exported_block_level ~f:(fun _ -> Store.Block.hash block)
        in
        import_export hash >>=? return_some
    | Some reason -> (
        let block = Option.unopt_assert ~loc:__POS__ block_opt in
        let reason_to_string = function
          | `Pruned ->
              "Pruned"
          | `Pruned_pred ->
              "Pruned_pred"
          | `Unknown ->
              "Unknown"
          | `Caboose ->
              "Caboose"
          | `Genesis ->
              "Genesis"
          | `Not_enough_pred ->
              "Not_enough_pred"
          | `Missing_context ->
              "Missing_context"
        in
        import_export (Some (Store.Block.hash block))
        >>= function
        | Error [Invalid_export_block {block = _; reason = reason'}]
          when reason = reason' ->
            (* Expected error *)
            return_none
        | Ok _ ->
            Assert.fail_msg
              "Unexpected success in export: expected %s error"
              (reason_to_string reason)
        | Error err ->
            Assert.fail_msg
              "Unexpected error in export. Expected error with %s - Got : %a"
              (reason_to_string reason)
              Error_monad.pp_print_error
              err )
  in
  consistency_check ()
  >>=? function
  | None ->
      (* Encountered an expected error, nothing to do *)
      return_unit
  | Some (store', chain_store', _head) ->
      Lwt.finalize
        (fun () ->
          check_baking_continuity
            ~test_descr
            ~exported_chain_store:chain_store
            ~imported_chain_store:chain_store')
        (fun () ->
          (* only close store' - store will be closed by the test
                wrapper *)
          Store.close_store store' >>= fun _ -> Lwt.return_unit)

let make_tests speed genesis_parameters =
  let open Tezos_protocol_alpha.Protocol in
  let { Parameters_repr.constants =
          {Constants_repr.blocks_per_cycle; preserved_cycles; _};
        _ } =
    genesis_parameters
  in
  let blocks_per_cycle = Int32.to_int blocks_per_cycle in
  (* "Au paradis des louches" *)
  let nb_initial_blocks_list =
    match speed with
    | `Slow ->
        [ preserved_cycles * blocks_per_cycle;
          ((2 * preserved_cycles) + 1) * blocks_per_cycle;
          65;
          77;
          89 ]
    | `Quick ->
        [((2 * preserved_cycles) + 1) * blocks_per_cycle; 77]
  in
  let exporter_history_modes =
    let open History_mode in
    match speed with
    | `Slow ->
        [ Archive;
          Full {offset = 0};
          Full {offset = default_offset};
          Rolling {offset = 0};
          Rolling {offset = default_offset} ]
    | `Quick ->
        [ Full {offset = default_offset};
          Rolling {offset = 0};
          Rolling {offset = default_offset} ]
  in
  let export_blocks_levels nb_initial_blocks =
    match speed with
    | `Slow ->
        [ None;
          Some Int32.(of_int (nb_initial_blocks - blocks_per_cycle));
          Some (Int32.of_int nb_initial_blocks) ]
    | `Quick ->
        [None; Some (Int32.of_int nb_initial_blocks)]
  in
  let permutations =
    List.(
      product
        (product
           nb_initial_blocks_list
           ( map export_blocks_levels nb_initial_blocks_list
           |> flatten |> List.sort_uniq compare ))
        (product exporter_history_modes [false; true]))
    |> List.map (fun ((a, b), (c, d)) -> (a, b, c, d))
  in
  List.filter_map
    (fun (nb_initial_blocks, exported_block_level, history_mode, rolling) ->
      let test_descr =
        Format.asprintf
          "export => import with %d initial blocks from %a to %s (exported \
           block at %s)"
          nb_initial_blocks
          History_mode.pp
          history_mode
          (if rolling then "rolling" else "full")
          ( match exported_block_level with
          | None ->
              "checkpoint"
          | Some i ->
              Format.sprintf "level %ld" i )
      in
      match history_mode with
      | Rolling _ when rolling = false ->
          None
      | _ -> (
        match exported_block_level with
        | Some level
          when Compare.Int32.(Int32.of_int nb_initial_blocks <= level) ->
            None
        | None | Some _ ->
            Some
              (wrap_test
                 ~keep_dir:false
                 ~history_mode
                 ~patch_context:(fun ctxt ->
                   Alpha_utils.patch_context ~genesis_parameters ctxt)
                 ( test_descr,
                   fun store_path store ->
                     test
                       ?exported_block_level
                       ~nb_blocks_to_bake_before_export:nb_initial_blocks
                       ~rolling
                       ~test_descr
                       store_path
                       store )) ))
    permutations

let test_rolling genesis_parameters =
  let patch_context ctxt =
    Alpha_utils.patch_context ~genesis_parameters ctxt
  in
  let test (store_dir, context_dir) store =
    let chain_store = Store.main_chain_store store in
    Store.Chain.genesis_block chain_store
    >>= fun genesis_block ->
    let nb_cycles_to_bake = 6 in
    Alpha_utils.bake_until_n_cycle_end
      chain_store
      nb_cycles_to_bake
      genesis_block
    >>=? fun (_blocks, head) ->
    let snapshot_dir = store_dir // "snapshot.full" in
    let dst_dir = store_dir // "imported_store" in
    let dst_store_dir = dst_dir // "store" in
    let dst_context_dir = dst_dir // "context" in
    Snapshots.export
      ~rolling:true
      ~block:(`Head 0)
      ~store_dir
      ~context_dir
      ~chain_name:(Distributed_db_version.Name.of_string "test")
      ~snapshot_dir
      genesis
    >>=? fun () ->
    Snapshots.import
      ~snapshot_dir
      ~dst_store_dir
      ~dst_context_dir
      ~user_activated_upgrades:[]
      ~user_activated_protocol_overrides:[]
      genesis
    >>=? fun () ->
    Store.init
      ~patch_context
      ~history_mode:History_mode.default_rolling
      ~readonly:false
      ~store_dir:dst_store_dir
      ~context_dir:dst_context_dir
      ~allow_testchains:true
      genesis
    >>=? fun store' ->
    let chain_store' = Store.main_chain_store store' in
    let rec loop blk = function
      | 0 ->
          return blk
      | n ->
          Alpha_utils.bake_until_cycle_end chain_store' blk
          >>=? fun (_, blk) -> loop blk (n - 1)
    in
    loop head 4
    >>=? fun _head ->
    Store.Chain.checkpoint chain_store'
    >>= fun checkpoint ->
    let prn i = Format.sprintf "%ld" i in
    Store.Block.read_block chain_store' (fst checkpoint)
    >>=? fun checkpoint_block ->
    Store.Block.get_block_metadata chain_store' checkpoint_block
    >>=? fun metadata ->
    let max_op_ttl_cp =
      Int32.(
        sub (snd checkpoint) (of_int (Store.Block.max_operations_ttl metadata)))
    in
    Store.Chain.caboose chain_store'
    >>= fun caboose ->
    Assert.equal
      ~prn
      ~msg:__LOC__
      ~eq:Compare.Int32.equal
      max_op_ttl_cp
      (snd caboose) ;
    Store.close_store store' >>= fun () -> return_unit
  in
  wrap_test
    ~keep_dir:false
    ~history_mode:History_mode.default
    ~patch_context
    ( Format.asprintf
        "genesis consistency after rolling import (blocks per cycle = %ld)"
        Tezos_protocol_alpha.Protocol.(
          genesis_parameters.Parameters_repr.constants
            .Constants_repr.blocks_per_cycle),
      test )

(* TODO:
   export => import => export => import from full & rolling
   export equivalence
*)

let tests speed =
  let test_cases =
    let generated_tests =
      make_tests
        speed
        Tezos_protocol_alpha_parameters.Default_parameters.(
          parameters_of_constants constants_sandbox)
    in
    let rolling_tests =
      [ test_rolling
          Tezos_protocol_alpha_parameters.Default_parameters.(
            parameters_of_constants constants_sandbox);
        test_rolling
          Tezos_protocol_alpha_parameters.Default_parameters.(
            parameters_of_constants constants_test) ]
    in
    generated_tests @ rolling_tests
  in
  ("snapshots", test_cases)
