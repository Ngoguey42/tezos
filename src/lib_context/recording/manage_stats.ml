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

(** [manage_stats.exe --help] *)

open Tezos_context_recording
module Def = Stats_trace_definition
module Summary = Trace_stats_summary

let summarise path =
  Summary.(summarise path |> Fmt.pr "%a\n" (Irmin.Type.pp_json t))

let summary_of_path p =
  if Filename.extension p <> ".json" then failwith "Input file should be JSON";
  let chan = open_in_bin p in
  let raw = really_input_string chan (in_channel_length chan) in
  close_in chan;
  match Irmin.Type.of_json_string Summary.t raw with
  | Error (`Msg msg) ->
      Fmt.invalid_arg
        "File \"%s\" should be a json summary.\nError: %s" p msg
  | Ok s -> s

let pp name_per_path paths cols_opt =
  let summaries = List.map summary_of_path paths in
  let col_count =
    match cols_opt with
    | Some v -> v
    | None -> if List.length summaries > 1 then 4 else 5
  in
  (* ignore (col_count, name_per_path); *)
  (* assert false *)
  Fmt.pr "%a\n" (Trace_stats_summary_pp.pp col_count) (name_per_path, summaries)

let pp paths named_paths cols_opt =
  let name_per_path, paths =
    List.mapi (fun i v -> (string_of_int i, v)) paths @ named_paths
    |> List.split
  in
  if List.length paths = 0 then
    invalid_arg "manage_stats.exe pp: At least one path should be provided";
  pp name_per_path paths cols_opt

let summary_to_cb _path =
  assert false

open Cmdliner

let term_summarise =
  let stats_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A stats trace file" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const summarise $ stats_trace_file)

let term_pp =
  let arg_indexed_files =
    let open Arg in
    let a = pos_all non_dir_file [] (info [] ~docv:"FILE") in
    value a
  in
  let arg_named_files =
    let open Arg in
    let a =
      opt_all (pair string non_dir_file) []
        (info [ "f"; "named-file" ]
           ~doc:
             "A comma-separated pair of short name / path to trace or summary. \
              The short name is used to tag the rows inside the pretty printed \
              table.")
    in
    value a
  in
  let arg_columns =
    let open Arg in
    let doc =
      Arg.info ~doc:"Number of sample columns to show." [ "c"; "columns" ]
    in
    let a = opt (some int) None doc in
    value a
  in
  Term.(const pp $ arg_indexed_files $ arg_named_files $ arg_columns)

let term_cb =
  let summary_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A stats trace summary file" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const summary_to_cb $ summary_file)

let () =
  let man = [] in
  let i =
    Term.info ~man ~doc:"Processing of stats traces and stats trace summaries."
      "stats"
  in

  let man =
    [
      `P "From stats trace (repr) to summary (json).";
      `S "EXAMPLE";
      `P "manage_stats.exe summarise run0.repr > run0.json";
    ]
  in
  let j = Term.info ~man ~doc:"Stats Trace to Summary" "summarise" in

  let man =
    [
      `P "Pretty print summaries (json).";
      `P
        "When a single file is provided, a subset of the summary of that file \
         is computed and shown.";
      `P
        "When multiple files are provided, a subset of the summary of each \
         file is computed and shown in a way that makes comparisons between \
         files easy.";
      `S "EXAMPLES";
      `P "manage_stats.exe pp run0.json";
      `Noblank;
      `P "manage_stats.exe pp run0.json run2.json run3.json";
      `Noblank;
      `P "manage_stats.exe pp -f r0,run0.json -f r1,run1.json";
    ]
  in
  let k = Term.info ~man ~doc:"Comparative Pretty Printing" "pp" in
  let l =
    Term.info ~man ~doc:"Summary JSON to Continous Benchmarks JSON" "cb"
  in
  Term.exit
  @@ Term.eval_choice (term_summarise, i)
       [ (term_summarise, j); (term_pp, k); (term_cb, l) ]
