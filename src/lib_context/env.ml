(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018-2022 Tarides <contact@tarides.com>                     *)
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

type t = {
  verbosity : [`Default | `Info | `Debug];
  index_log_size : int option;
  auto_flush : int option;
  record_raw_actions_trace : [`No | `Yes of string];
  record_stats_trace : [`No | `Yes of string];
  stats_trace_message : string option;
}

let default =
  {
    verbosity = `Default;
    index_log_size = None;
    auto_flush = None;
    record_raw_actions_trace = `No;
    record_stats_trace = `No;
    stats_trace_message = None;
  }

let max_verbosity a b =
  match (a, b) with
  | (`Debug, _) | (_, `Debug) -> `Debug
  | (`Info, _) | (_, `Info) -> `Info
  | _ -> `Default

let v =
  match Unix.getenv "TEZOS_CONTEXT" with
  | exception Not_found -> default
  | v ->
      List.fold_left
        (fun acc s ->
          match String.trim s with
          | "v" | "verbose" ->
              {acc with verbosity = max_verbosity acc.verbosity `Info}
          | "vv" -> {acc with verbosity = `Debug}
          | v -> (
              match String.split '=' v |> List.map String.trim with
              | ["index-log-size"; n] -> (
                  match int_of_string_opt n with
                  | None ->
                      Fmt.epr
                        "[WARNING] Trying to convert %s into an integer for \
                         index-log-size, but the conversion failed. Using \
                         default settings."
                        n ;
                      acc
                  | some -> {acc with index_log_size = some})
              | ["auto-flush"; n] -> (
                  match int_of_string_opt n with
                  | None ->
                      Fmt.epr
                        "[WARNING] Trying to convert %s into an integer for \
                         auto-flush, but the conversion failed. Using default \
                         settings."
                        n ;
                      acc
                  | some -> {acc with auto_flush = some})
              | ["actions-trace-record-directory"; path] ->
                  {acc with record_raw_actions_trace = `Yes path}
              | ["stats-trace-record-directory"; path] ->
                  {acc with record_stats_trace = `Yes path}
              | unknown :: _ ->
                  Fmt.epr
                    "[WARNING] Unknow option %s detected in the environment \
                     variable TEZOS_CONTEXT."
                    unknown ;
                  acc
              | [] ->
                  Fmt.epr
                    "[WARNING] Empty string detected in the environment \
                     variable TEZOS_CONTEXT." ;
                  acc))
        default
        (String.split ',' v)

let v =
  match Unix.getenv "STATS_TRACE_MESSAGE" with
  | exception Not_found -> v
  | msg -> {v with stats_trace_message = Some msg}
