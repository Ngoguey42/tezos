(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
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

open Tezos_protocol_environment
open Context
open Lwt.Infix

module C = struct
  include Tezos_context.Context_v0

  let set_protocol = add_protocol
end

include Environment_context.Register (C)

let impl_name = "shell"

let checkout index context_hash =
  Tezos_context.Context_v0.checkout index context_hash
  >|= Option.map @@ fun ctxt ->
      Context.make ~ops ~ctxt ~kind:Context ~equality_witness ~impl_name

let checkout_exn index context_hash =
  Tezos_context.Context_v0.checkout_exn index context_hash >|= fun ctxt ->
  Context.make ~ops ~ctxt ~kind:Context ~equality_witness ~impl_name

let wrap_disk_context ctxt =
  Context.make ~ops ~ctxt ~kind:Context ~equality_witness ~impl_name

let unwrap_disk_context : t -> Tezos_context.Context_v0.t = function
  | Context.Context {ctxt; kind = Context; _} -> ctxt
  | Context.Context t ->
      Environment_context.err_implementation_mismatch
        ~expected:impl_name
        ~got:t.impl_name
