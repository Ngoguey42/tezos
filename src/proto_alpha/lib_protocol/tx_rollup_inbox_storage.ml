(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2022 Marigold <contact@marigold.dev>                        *)
(* Copyright (c) 2022 Nomadic Labs <contact@nomadic-labs.com>                *)
(* Copyright (c) 2022 Oxhead Alpha <info@oxhead-alpha.com>                   *)
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

open Tx_rollup_errors_repr

(** [prepare_metadata ctxt rollup state level] prepares the metadata
    for an inbox at [level], which may imply creating it if it does
    not already exist. *)
let prepare_metadata :
    Raw_context.t ->
    Tx_rollup_repr.t ->
    Tx_rollup_state_repr.t ->
    Raw_level_repr.t ->
    (Raw_context.t
    * Tx_rollup_state_repr.t
    * Tx_rollup_level_repr.t
    * Tx_rollup_inbox_repr.metadata)
    tzresult
    Lwt.t =
 fun ctxt rollup state level ->
  (* First, check if there are too many inboxes *)
  fail_when
    Compare.Int.(
      Constants_storage.tx_rollup_max_unfinalized_levels ctxt
      <= Tx_rollup_state_repr.inboxes_count state)
    Too_many_inboxes
  >>=? fun () ->
  match Tx_rollup_state_repr.head_level state with
  | Some (_, tezos_lvl) when Raw_level_repr.(level < tezos_lvl) ->
      fail (Internal_error "Trying to write into an inbox from the past")
  | Some (tx_lvl, tezos_lvl) when Raw_level_repr.(tezos_lvl = level) ->
      (* An inbox should already exists *)
      Storage.Tx_rollup.Inbox_metadata.get (ctxt, tx_lvl) rollup
      >>=? fun (ctxt, metadata) -> return (ctxt, state, tx_lvl, metadata)
  | _ ->
      (* We need a new inbox *)
      Tx_rollup_state_repr.record_inbox_creation state level
      >>?= fun (state, tx_level) ->
      let metadata = Tx_rollup_inbox_repr.empty_metadata in
      Storage.Tx_rollup.Inbox_metadata.init (ctxt, tx_level) rollup metadata
      >>=? fun (ctxt, _) ->
      let ctxt = Raw_context.record_tx_rollup ctxt rollup in
      return (ctxt, state, tx_level, metadata)

(** [update_metadata metadata msg_size] updates [metadata] to account
    for a new message of [msg_size] bytes. *)
let update_metadata :
    Tx_rollup_inbox_repr.metadata ->
    Tx_rollup_message_repr.hash ->
    int ->
    Tx_rollup_inbox_repr.metadata tzresult =
 fun metadata msg_hash msg_size ->
  let hash = Tx_rollup_inbox_repr.extend_hash metadata.hash msg_hash in
  ok
    Tx_rollup_inbox_repr.
      {
        inbox_length = Int32.succ metadata.inbox_length;
        cumulated_size = msg_size + metadata.cumulated_size;
        hash;
      }

let append_message :
    Raw_context.t ->
    Tx_rollup_repr.t ->
    Tx_rollup_state_repr.t ->
    Tx_rollup_message_repr.t ->
    (Raw_context.t * Tx_rollup_state_repr.t) tzresult Lwt.t =
 fun ctxt rollup state message ->
  let level = (Raw_context.current_level ctxt).level in
  let message_size = Tx_rollup_message_repr.size message in
  prepare_metadata ctxt rollup state level
  >>=? fun (ctxt, new_state, tx_level, metadata) ->
  fail_when
    Compare.Int.(
      Int32.to_int metadata.inbox_length
      >= Constants_storage.tx_rollup_max_messages_per_inbox ctxt)
    (Inbox_count_would_exceed_limit rollup)
  >>=? fun () ->
  Tx_rollup_message_builder.hash ctxt message >>?= fun (ctxt, message_hash) ->
  update_metadata metadata message_hash message_size >>?= fun new_metadata ->
  let new_size = new_metadata.cumulated_size in
  let inbox_limit =
    Constants_storage.tx_rollup_hard_size_limit_per_inbox ctxt
  in
  fail_unless
    Compare.Int.(new_size <= inbox_limit)
    (Inbox_size_would_exceed_limit rollup)
  >>=? fun () ->
  (* Checks have passed, so we can actually record in the storage. *)
  Storage.Tx_rollup.Inbox_metadata.add (ctxt, tx_level) rollup new_metadata
  >>=? fun (ctxt, _, _) ->
  Storage.Tx_rollup.Inbox_contents.add
    ((ctxt, tx_level), rollup)
    metadata.inbox_length
    message_hash
  >>=? fun (ctxt, _, _) -> return (ctxt, new_state)

let messages_opt :
    Raw_context.t ->
    Tx_rollup_level_repr.t ->
    Tx_rollup_repr.t ->
    (Raw_context.t * Tx_rollup_message_repr.hash list option) tzresult Lwt.t =
 fun ctxt level tx_rollup ->
  Storage.Tx_rollup.Inbox_contents.list_values ((ctxt, level), tx_rollup)
  >>=? function
  | (ctxt, []) ->
      (*
        Prior to returning [None], we check whether or not the
        transaction rollup address is valid, to raise the appropriate
        if need be.
       *)
      Tx_rollup_state_storage.assert_exist ctxt tx_rollup >>=? fun ctxt ->
      return (ctxt, None)
  | (ctxt, contents) -> return (ctxt, Some contents)

let messages :
    Raw_context.t ->
    Tx_rollup_level_repr.t ->
    Tx_rollup_repr.t ->
    (Raw_context.t * Tx_rollup_message_repr.hash list) tzresult Lwt.t =
 fun ctxt level tx_rollup ->
  messages_opt ctxt level tx_rollup >>=? function
  | (ctxt, Some messages) -> return (ctxt, messages)
  | (_, None) ->
      fail (Tx_rollup_errors_repr.Inbox_does_not_exist (tx_rollup, level))

let size :
    Raw_context.t ->
    Tx_rollup_level_repr.t ->
    Tx_rollup_repr.t ->
    (Raw_context.t * int) tzresult Lwt.t =
 fun ctxt level tx_rollup ->
  Storage.Tx_rollup.Inbox_metadata.find (ctxt, level) tx_rollup >>=? function
  | (ctxt, Some {cumulated_size; _}) -> return (ctxt, cumulated_size)
  | (ctxt, None) ->
      (*
        Prior to raising an error related to the missing inbox, we
        check whether or not the transaction rollup address is valid,
        to raise the appropriate if need be.
       *)
      Tx_rollup_state_storage.assert_exist ctxt tx_rollup >>=? fun _ctxt ->
      fail (Inbox_does_not_exist (tx_rollup, level))

let find :
    Raw_context.t ->
    Tx_rollup_level_repr.t ->
    Tx_rollup_repr.t ->
    (Raw_context.t * Tx_rollup_inbox_repr.t option) tzresult Lwt.t =
 fun ctxt level tx_rollup ->
  let open Tx_rollup_inbox_repr in
  (*
    [messages_opt] checks whether or not [tx_rollup] is valid, so
    we do not have to do it here.
   *)
  messages_opt ctxt level tx_rollup >>=? function
  | (ctxt, Some contents) ->
      size ctxt level tx_rollup >>=? fun (ctxt, cumulated_size) ->
      let hash = Tx_rollup_inbox_repr.hash_hashed_inbox contents in
      return (ctxt, Some {cumulated_size; contents; hash})
  | (ctxt, None) -> return (ctxt, None)

let get :
    Raw_context.t ->
    Tx_rollup_level_repr.t ->
    Tx_rollup_repr.t ->
    (Raw_context.t * Tx_rollup_inbox_repr.t) tzresult Lwt.t =
 fun ctxt level tx_rollup ->
  (*
    [inbox_opt] checks whether or not [tx_rollup] is valid, so we
    don’t have to do it here.
   *)
  find ctxt level tx_rollup >>=? function
  | (ctxt, Some res) -> return (ctxt, res)
  | (_, None) -> fail (Inbox_does_not_exist (tx_rollup, level))

let get_metadata :
    Raw_context.t ->
    Tx_rollup_level_repr.t ->
    Tx_rollup_repr.t ->
    (Raw_context.t * Tx_rollup_inbox_repr.metadata) tzresult Lwt.t =
 fun ctxt level tx_rollup ->
  Storage.Tx_rollup.Inbox_metadata.find (ctxt, level) tx_rollup >>=? function
  | (_, None) -> fail (Inbox_does_not_exist (tx_rollup, level))
  | (ctxt, Some metadata) -> return (ctxt, metadata)

let remove :
    Raw_context.t ->
    Tx_rollup_level_repr.t ->
    Tx_rollup_repr.t ->
    Raw_context.t tzresult Lwt.t =
 fun ctxt level rollup ->
  let rec remove_messages ctxt i len =
    if Compare.Int32.(i < len) then
      Storage.Tx_rollup.Inbox_contents.remove ((ctxt, level), rollup) i
      >>=? fun (ctxt, _, _) -> remove_messages ctxt (Int32.succ i) len
    else return ctxt
  in
  get_metadata ctxt level rollup >>=? fun (ctxt, metadata) ->
  Storage.Tx_rollup.Inbox_metadata.remove (ctxt, level) rollup
  >>=? fun (ctxt, _, _) -> remove_messages ctxt 0l metadata.inbox_length
