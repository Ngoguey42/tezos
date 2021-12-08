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

(** [actions.exe --help] *)

open Bench_common
open Irmin.Export_for_backends
open Irmin_traces
module Rawdef = Trace_definitions.Raw_actions_trace
module Summary = Trace_raw_actions_summary

let summarise path =
  Summary.(summarise path |> Fmt.pr "%a\n" (Irmin.Type.pp_json t))

let to_replayable () path block_level_of_first_opt block_level_of_last_opt
    block_idx_of_first_opt block_count_opt =
  let first =
    match (block_idx_of_first_opt, block_level_of_first_opt) with
    | (None, None) -> `Idx 0
    | (Some _, Some _) -> invalid_arg "TODO"
    | (Some i, None) -> `Idx i
    | (None, Some i) -> `Level i
  in
  let last =
    match (block_count_opt, block_level_of_last_opt) with
    | (None, None) -> `End
    | (Some _, Some _) -> invalid_arg "TODO"
    | (Some i, None) -> `Count i
    | (None, Some i) -> `Level i
  in
  Lwt_main.run (Trace_raw_actions_to_replayable.run ~first ~last path stdout) ;
  flush stdout

let list path =
  Rawdef.trace_files_of_trace_directory path
  |> List.iter (fun (path, _) ->
         Fmt.pr "Reading %s\n" path ;
         let (_, (), row_seq) = Rawdef.open_reader path in
         Seq.iter (Fmt.pr "%a\n" (Repr.pp Rawdef.row_t)) row_seq) ;
  Fmt.pr "%!"

open Cmdliner

let setup_log =
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let term_summarise =
  let stats_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const summarise $ stats_trace_file)

let term_to_rep =
  let stats_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in

  let arg_block_level_of_first =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block_level_of_first"] in
    let a = opt (some int) None doc in
    value a
  in
  let arg_block_level_of_last =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block_level_of_last"] in
    let a = opt (some int) None doc in
    value a
  in
  let arg_block_idx_of_first =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block_idx_of_first"] in
    let a = opt (some int) None doc in
    value a
  in
  let arg_block_count =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block_count"] in
    let a = opt (some int) None doc in
    value a
  in

  Term.(
    const to_replayable $ setup_log $ stats_trace_file
    $ arg_block_level_of_first $ arg_block_level_of_last
    $ arg_block_idx_of_first $ arg_block_count)

let term_list =
  let stats_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const list $ stats_trace_file)

let () =
  let man = [] in
  let i = Term.info ~man ~doc:"Processing of actions traces." "actions" in

  let man =
    [
      `P "From raw actions trace (directory) to summary (json).";
      `S "EXAMPLE";
      `P
        "manage_actions.exe summarise ./raw_actions/ \
         >./raw_actions_summary.json";
    ]
  in
  let j = Term.info ~man ~doc:"Raw Actions Summary" "summarise" in

  let man =
    [
      `P "From raw actions trace (directory) to replayable actions trace (file).";
      `S "EXAMPLE";
      `P
        "manage_actions.exe to-replayable ./raw_actions/ \
         >./replayable_actions.trace ";
    ]
  in
  let k = Term.info ~man ~doc:"Replayable Actions Trace" "to-replayable" in

  let man = [`P "List the operations from a raw actions trace (directory)."] in
  let l = Term.info ~man ~doc:"List Raw Actions" "list" in

  Term.exit
  @@ Term.eval_choice
       (term_summarise, i)
       [(term_summarise, j); (term_to_rep, k); (term_list, l)]
