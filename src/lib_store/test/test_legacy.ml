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
open Legacy_utils

(* From "legacy chain_validator"*)
let may_update_checkpoint chain_state new_head =
  Legacy_state.Chain.checkpoint chain_state
  >>= fun checkpoint ->
  Legacy_state.Block.last_allowed_fork_level new_head
  >>=? fun new_level ->
  if new_level <= checkpoint.shell.level then return_unit
  else
    let state = Legacy_state.Chain.global_state chain_state in
    Legacy_state.history_mode state
    >>= fun history_mode ->
    let head_level = Legacy_state.Block.level new_head in
    Legacy_state.Block.predecessor_n
      new_head
      (Int32.to_int (Int32.sub head_level new_level))
    >>= function
    | None ->
        assert false (* should not happen *)
    | Some new_checkpoint -> (
        Legacy_state.Block.read_opt chain_state new_checkpoint
        >>= function
        | None ->
            assert false (* should not happen *)
        | Some new_checkpoint -> (
            let new_checkpoint = Legacy_state.Block.header new_checkpoint in
            match history_mode with
            | History_mode.Legacy.Archive ->
                Legacy_state.Chain.set_checkpoint chain_state new_checkpoint
                >>= fun () -> return_unit
            | Full ->
                Legacy_state.Chain.set_checkpoint_then_purge_full
                  chain_state
                  new_checkpoint
            | Rolling ->
                Legacy_state.Chain.set_checkpoint_then_purge_rolling
                  chain_state
                  new_checkpoint ) )

let assert_presence new_chain_store previously_baked_blocks ?savepoint ?caboose
    = function
  | History_mode.Archive ->
      assert_presence_in_store
        ~with_metadata:true
        new_chain_store
        previously_baked_blocks
  | Full _ ->
      let expected_savepoint = Option.unopt_assert ~loc:__POS__ savepoint in
      let (pruned, complete) =
        List.split_n (Int32.to_int expected_savepoint) previously_baked_blocks
      in
      assert_presence_in_store ~with_metadata:false new_chain_store pruned
      >>=? fun () ->
      assert_presence_in_store ~with_metadata:true new_chain_store complete
  | Rolling _ ->
      let expected_caboose = Option.unopt_assert ~loc:__POS__ caboose in
      let expected_savepoint = Option.unopt_assert ~loc:__POS__ savepoint in
      let (pruned, complete) =
        let rolling_window =
          List.filter
            (fun b -> Store.Block.level b >= expected_caboose)
            previously_baked_blocks
        in
        List.split_n (Int32.to_int expected_savepoint) rolling_window
      in
      assert_presence_in_store ~with_metadata:false new_chain_store pruned
      >>=? fun () ->
      assert_presence_in_store ~with_metadata:true new_chain_store complete

let check_flags new_chain_store previously_baked_blocks history_mode =
  let last = List.last_exn previously_baked_blocks in
  Assert.equal_history_mode
    ~msg:"history mode consistency: "
    history_mode
    (Store.Chain.history_mode new_chain_store) ;
  Store.Chain.checkpoint new_chain_store
  >>= fun checkpoint ->
  Store.Block.get_block_metadata new_chain_store last
  >>=? fun metadata ->
  let expected_checkpoint = Store.Block.last_allowed_fork_level metadata in
  Assert.equal
    ~prn:(Format.sprintf "%ld")
    ~msg:"checkpoint consistency: "
    expected_checkpoint
    (snd checkpoint) ;
  Store.Chain.savepoint new_chain_store
  >>= fun savepoint ->
  Store.Chain.caboose new_chain_store
  >>= fun caboose ->
  match history_mode with
  | History_mode.Archive ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:"savepoint consistency: "
        0l
        (snd savepoint) ;
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:"caboose consistency: "
        (snd savepoint)
        (snd caboose) ;
      assert_presence new_chain_store previously_baked_blocks history_mode
  | Full _ ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:"savepoint consistency: "
        expected_checkpoint
        (snd savepoint) ;
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:"caboose consistency: "
        0l
        (snd caboose) ;
      assert_presence
        new_chain_store
        previously_baked_blocks
        ~savepoint:(snd savepoint)
        history_mode
  | Rolling _ ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:"savepoint consistency: "
        expected_checkpoint
        (snd savepoint) ;
      Store.Block.get_block_metadata new_chain_store last
      >>=? fun metadata ->
      let max_op_ttl = Store.Block.max_operations_ttl metadata in
      let expected_caboose =
        max 0l Int32.(add (sub expected_checkpoint (of_int max_op_ttl)) 0l)
      in
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:"caboose consistency: "
        expected_caboose
        (snd caboose) ;
      assert_presence
        new_chain_store
        previously_baked_blocks
        ~caboose:expected_caboose
        ~savepoint:expected_checkpoint
        history_mode

let test_upgrade store (legacy_dir, (legacy_state : Legacy_state.t)) blocks =
  let patch_context ctxt = Alpha_utils.default_patch_context ctxt in
  let chain_store = Store.main_chain_store store in
  let genesis = Store.Chain.genesis chain_store in
  Lwt_utils_unix.create_dir legacy_dir
  >>= fun () ->
  let chain_name = Distributed_db_version.Name.of_string "TEZOS" in
  Legacy_state.Chain.get_exn legacy_state (Store.Chain.chain_id chain_store)
  >>= fun legacy_chain ->
  Lwt_list.map_p
    (fun block ->
      let hash = Store.Block.hash block in
      Legacy_state.Block.known legacy_chain hash
      >>= fun known -> Lwt.return (hash, known))
    blocks
  >>= fun present_blocks_in_legacy ->
  Legacy.upgrade_0_0_4 ~data_dir:legacy_dir ~patch_context ~chain_name genesis
  >>=? fun _upgrade_message ->
  let history_mode = Store.Chain.history_mode chain_store in
  Store.init
    ~patch_context
    ~history_mode
    ~readonly:false
    ~store_dir:(legacy_dir // "store")
    ~context_dir:(legacy_dir // "context")
    ~allow_testchains:true
    genesis
  >>=? fun upgraded_store ->
  Lwt.finalize
    (fun () ->
      let upgraded_chain_store = Store.main_chain_store upgraded_store in
      Lwt_list.iter_s
        (fun (hash, is_known) ->
          Store.Block.is_known upgraded_chain_store hash
          >>= fun is_known' ->
          Assert.equal
            ~msg:
              (Format.asprintf
                 "check %a existence after upgrade"
                 Block_hash.pp
                 hash)
            is_known
            is_known' ;
          Lwt.return_unit)
        present_blocks_in_legacy
      >>= fun () ->
      check_flags upgraded_chain_store blocks history_mode
      >>=? fun () ->
      Test_utils.check_invariants upgraded_chain_store
      >>= fun () ->
      (* Try baking a bit after upgrading... *)
      Store.Chain.current_head upgraded_chain_store
      >>= fun head ->
      Alpha_utils.bake_until_n_cycle_end upgraded_chain_store 10 head
      >>=? fun _ -> return_unit)
    (fun () -> Store.close_store upgraded_store)

let test_snapshot legacy_snapshot_history_mode store
    (legacy_dir, (legacy_state : Legacy_state.t)) blocks =
  let patch_context ctxt = Alpha_utils.default_patch_context ctxt in
  let chain_store = Store.main_chain_store store in
  let genesis = Store.Chain.genesis chain_store in
  Lwt_utils_unix.create_dir legacy_dir
  >>= fun () ->
  let chain_name = Distributed_db_version.Name.of_string "TEZOS" in
  Legacy_state.Chain.get_exn legacy_state (Store.Chain.chain_id chain_store)
  >>= fun legacy_chain ->
  Lwt_list.map_p
    (fun block ->
      let descr = Store.Block.descriptor block in
      Legacy_state.Block.known legacy_chain (fst descr)
      >>= fun known -> Lwt.return (descr, known))
    blocks
  >>= fun present_blocks_in_legacy ->
  Legacy_chain.head legacy_chain
  >>= fun legacy_head ->
  let snapshot_file = legacy_dir // "legacy_snapshot" in
  let head_hash = Legacy_state.Block.hash legacy_head in
  Legacy_snapshots.export
    ~export_rolling:(legacy_snapshot_history_mode = History_mode.Legacy.Rolling)
    ~store_root:(legacy_dir // "store")
    ~context_root:(legacy_dir // "context")
    ~genesis
    snapshot_file
    head_hash
  >>=? fun () ->
  let root_dir = Store.directory store // ".." // "imported_store" in
  let dst_store_dir = root_dir // "store" in
  let dst_context_dir = legacy_dir // "context" in
  Format.printf "import from legacy snapshot : %s@." snapshot_file ;
  Snapshots.import_legacy
    ~patch_context
    ~block:head_hash
    ~dst_store_dir
    ~dst_context_dir
    ~chain_name
    ~user_activated_upgrades:[]
    ~user_activated_protocol_overrides:[]
    ~snapshot_file
    genesis
  >>=? fun () ->
  Format.printf "init store after import@." ;
  let history_mode = History_mode.convert legacy_snapshot_history_mode in
  Store.init
    ~patch_context
    ~history_mode
    ~readonly:false
    ~store_dir:dst_store_dir
    ~context_dir:dst_context_dir
    ~allow_testchains:true
    genesis
  >>=? fun imported_store ->
  let imported_chain_store = Store.main_chain_store imported_store in
  Lwt.finalize
    (fun () ->
      Format.printf "sanity checks@." ;
      Lwt_list.iter_s
        (fun ((hash, level), is_known) ->
          Store.Block.is_known imported_chain_store hash
          >>= fun is_known' ->
          if is_known && not is_known' then (
            Store.Chain.caboose imported_chain_store
            >>= fun (_, caboose_level) ->
            Assert.is_true
              ~msg:"check block absence consistency with history mode"
              ( match history_mode with
              | Rolling _ ->
                  caboose_level > level
              | _ ->
                  false ) ;
            Lwt.return_unit )
          else (
            Assert.equal
              ~msg:
                (Format.asprintf
                   "check %a existence after upgrade"
                   Block_hash.pp
                   hash)
              is_known
              is_known' ;
            Lwt.return_unit ))
        present_blocks_in_legacy
      >>= fun () ->
      Test_utils.check_invariants imported_chain_store
      >>= fun () ->
      (* Try baking a bit after importing... *)
      Store.Chain.current_head imported_chain_store
      >>= fun head ->
      Alpha_utils.bake_until_n_cycle_end imported_chain_store 10 head
      >>=? fun _ -> return_unit)
    (fun () -> Store.close_store imported_store)

let make_upgrade_test_cases ?(keep_dir = false) speed :
    string Alcotest_lwt.test_case list =
  let history_modes =
    History_mode.[Legacy.Archive; Legacy.Full; Legacy.Rolling]
  in
  let nb_blocks_to_bake =
    match speed with `Slow -> 0 -- 100 | `Quick -> [8; 57; 89; 101]
  in
  let permutations = List.(product nb_blocks_to_bake history_modes) in
  List.map
    (fun (nb_blocks_to_bake, legacy_history_mode) ->
      let name =
        Format.asprintf
          "Upgrade legacy %a with %d blocks"
          History_mode.Legacy.pp
          legacy_history_mode
          nb_blocks_to_bake
      in
      let test =
        {
          name;
          speed;
          legacy_history_mode;
          nb_blocks = `Blocks nb_blocks_to_bake;
          test = test_upgrade;
        }
      in
      wrap_test_legacy ~keep_dir test)
    permutations

let make_snapshot_test_cases ?(keep_dir = false) ~history_modes
    ~snapshot_history_modes ~nb_blocks_to_bake =
  let permutations =
    List.(
      product nb_blocks_to_bake (product history_modes snapshot_history_modes))
    |> List.filter
         (fun (_, (legacy_history_mode, legacy_snapshot_history_mode)) ->
           if legacy_history_mode = History_mode.Legacy.Rolling then
             legacy_snapshot_history_mode = History_mode.Legacy.Rolling
           else true)
    |> List.sort_uniq compare
  in
  List.map
    (fun ( nb_blocks_to_bake,
           (legacy_history_mode, legacy_snapshot_history_mode) ) ->
      let name =
        Format.asprintf
          "Import legacy snapshot in %a from %a with %d blocks"
          History_mode.Legacy.pp
          legacy_snapshot_history_mode
          History_mode.Legacy.pp
          legacy_history_mode
          nb_blocks_to_bake
      in
      let test =
        {
          name;
          speed = `Quick;
          legacy_history_mode;
          nb_blocks = `Blocks nb_blocks_to_bake;
          test = test_snapshot legacy_snapshot_history_mode;
        }
      in
      wrap_test_legacy ~keep_dir test)
    permutations

let upgrade_tests : string Alcotest_lwt.test list =
  let speed =
    try
      let s = Sys.getenv "SLOW_TEST" in
      match String.(trim (uncapitalize_ascii s)) with
      | "true" | "1" | "yes" ->
          `Slow
      | _ ->
          `Quick
    with Not_found -> `Quick
  in
  let upgrade_cases = make_upgrade_test_cases ~keep_dir:false speed in
  let history_modes =
    History_mode.[Legacy.Archive; Legacy.Full; Legacy.Rolling]
  in
  let snapshot_history_modes = History_mode.[Legacy.Full; Legacy.Rolling] in
  let nb_blocks_to_bake = [40; 57; 89; 101] in
  let snapshots_cases =
    make_snapshot_test_cases
      ~keep_dir:false
      ~history_modes
      ~snapshot_history_modes
      ~nb_blocks_to_bake
  in
  [ ("legacy store upgrade", upgrade_cases);
    ("legacy snapshot import", snapshots_cases) ]

let () =
  let open Cmdliner in
  let arg =
    Arg.(
      required
      & opt (some string) None
      & info ~docv:"[LEGACY_STORE_BUILDER_PATH]" ["builder-path"])
  in
  Lwt_main.run
    (Alcotest_lwt.run_with_args "tezos-store-legacy" arg upgrade_tests)
