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

(** Pretty printing of one or more stats trace summaries.

    This file contains A LOT of uninteresting boilerplate in order to build the
    pretty-printable table. Doing this using pandas-like multi-level dataframes
    would make the task much more simpler. *)

open Trace_stats_summary
module Utils = Trace_stats_summary_utils
module Summary = Trace_stats_summary

(* let ( /. ) = Stdlib.( /.) *)

module Pb = struct
  include PrintBox

  let () = PrintBox_unicode.setup ()

  (* Some utilities to work with lists instead of array *)

  let transpose_matrix l =
    l
    |> List.map Array.of_list
    |> Array.of_list
    |> PrintBox.transpose
    |> Array.to_list
    |> List.map Array.to_list

  let matrix_to_text m = List.map (List.map PrintBox.text) m
  let align_matrix where = List.map (List.map (PrintBox.align ~h:where ~v:`Top))

  (** Dirty trick to only have vertical bars, and not the horizontal ones *)
  let matrix_with_column_spacers =
    let rec interleave sep = function
      | ([ _ ] | []) as l -> l
      | hd :: tl -> hd :: sep :: interleave sep tl
    in
    List.map (interleave (PrintBox.text " | "))
end

let fprintf_result ppf =
  Format.fprintf ppf
    {|-- setups --
%s

%s

 - (1) Longest Context.commit.
 - The "per sec" stats are calculated over CPU time.
 - "max memory usage" is the max size of OCaml's major heap.
 - "mean CPU usage" is inexact.

-- global --
%s

%s

-- evolution through blocks --
%s

Types of curves:
 *C: Cumulative. No smoothing.
 *LA: Local Average. Smoothed using a weighted sum of the value in the
      block and the exponentially decayed values of the previous blocks.
      Every %.2f blocks, half of the past is forgotten.
 *S: Size. E.g. directory entries, file bytes. No smoothing.
 *N: Very noisy.|}

type summary = Summary.t

type scalar_format_fixed = [ `SM | `S3 | `Sm | `Su | `RG | `RM | `Ri | `R3 | `P ]
(** Seconds minutes, Seconds 3 digits, Seconds milli, Seconds micro, Real giga,
    Real mega, Real as integer, Real 3 digits, Percent *)

let pp_scalar_fixed ppf (format, v) =
  if Float.is_nan v then Format.fprintf ppf "n/a"
  else if Float.is_infinite v then Format.fprintf ppf "%f" v
  else if v = 0. then Format.fprintf ppf "0"
  else
    match format with
    | `SM ->
        let m = Float.floor (v /. 60.) in
        let s = v -. (m *. 60.) in
        Format.fprintf ppf "%.0fm%02.0fs" m s
    | `S3 -> Format.fprintf ppf "%.3f s" v
    | `Sm -> Format.fprintf ppf "%.3f ms" (v *. 1e3)
    | `Su -> Format.fprintf ppf "%.3f \xc2\xb5s" (v *. 1e6)
    | `RG -> Format.fprintf ppf "%.3f G" (v /. 1e9)
    | `RM -> Format.fprintf ppf "%.3f M" (v /. 1e6)
    | `Ri -> Format.fprintf ppf "%#d" (Float.round v |> int_of_float)
    | `R3 -> Format.fprintf ppf "%.3f" v
    | `P -> Format.fprintf ppf "%.0f%%" (v *. 100.)

(** Summary *)
module Table0 = struct
  let summary_config_entries =
    [
      `Hostname;
      `Word_size;
      `Timeofday;
      (* `Inode_config; *)
      `Store_type;
      (* `Replay_path_conversion; *)
    ]

  let name_of_summary_config_entry = function
    | `Hostname -> "Hostname"
    | `Word_size -> "Word Size"
    | `Timeofday -> "Start Time"
    (* | `Inode_config -> "Inode Config" *)
    | `Store_type -> "Store Type"
    | `Replay_path_conversion -> "Path Conversion"

  let cell_of_summary_config (s : summary) = function
    | `Hostname -> s.hostname
    | `Word_size -> Printf.sprintf "%d bits" s.word_size
    | `Timeofday ->
        let open Unix in
        let t = gmtime s.timeofday in
        Printf.sprintf "%04d/%02d/%02d %02d:%02d:%02d (GMT)" (1900 + t.tm_year)
          (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min t.tm_sec
    (* | `Inode_config -> *)
        (* let a, b, c = s.config.inode_config in *)
        (* Printf.sprintf "mls:%d bf:%d sh:%d" a b c *)
    | `Store_type -> (
        match s.config.store_type with
        | `Pack -> "pack"
        | `Pack_layered -> "pack-layered"
        | `Pack_mem -> "pack-mem")
    (* | `Replay_path_conversion -> (
     *     match s.config.setup with
     *     | `Play _ -> "n/a"
     *     | `Replay s -> (
     *         match s.path_conversion with
     *         | `None -> "none"
     *         | `V1 -> "v1"
     *         | `V0_and_v1 -> "v0+v1"
     *         | `V0 -> "v0")) *)

  let box_of_summaries_config summary_names (summaries : summary list) =
    let row0 =
      if List.length summary_names = 1 then [] else [ "" :: summary_names ]
    in
    let rows =
      List.map
        (fun e ->
          let n = name_of_summary_config_entry e in
          let l = List.map (fun s -> cell_of_summary_config s e) summaries in
          n :: l)
        summary_config_entries
    in
    row0 @ rows |> Pb.matrix_to_text
end

(** Highlights *)
module Table1 = struct
  let rows_of_summaries summaries =
    let cpu_time_elapsed = List.map (fun s -> s.elapsed_cpu) summaries in
    let add_per_sec =
      List.map
        (fun s ->
          fst
            Summary.(Span.Map.find (`Frequent_op `Add) s.span).cumu_count
              .max_value
          /. s.elapsed_cpu)
        summaries
    in
    let tail_latency =
      List.map
        (fun s -> fst Summary.(Span.Map.find `Commit s.span).duration.max_value)
        summaries
    in
    let tx_per_sec =
      List.map
        (fun s ->
          Summary.(s.block_specs.tzop_count_tx.value.diff) /. s.elapsed_cpu)
        summaries
    in
    let tz_ops_per_sec =
      List.map
        (fun s ->
          Summary.(s.block_specs.tzop_count.value.diff) /. s.elapsed_cpu)
        summaries
    in
    let bytes =
      List.map (fun s -> s.index.bytes_both.value_after_commit.diff) summaries
    in
    let read_bytes =
      List.map (fun s -> s.index.bytes_read.value_after_commit.diff) summaries
    in
    let written_bytes =
      List.map
        (fun s -> s.index.bytes_written.value_after_commit.diff)
        summaries
    in
    let throughput =
      List.map
        (fun s -> s.index.bytes_both.value_after_commit.diff /. s.elapsed_cpu)
        summaries
    in
    let read_throughput =
      List.map
        (fun s -> s.index.bytes_read.value_after_commit.diff /. s.elapsed_cpu)
        summaries
    in
    let write_throughput =
      List.map
        (fun s ->
          s.index.bytes_written.value_after_commit.diff /. s.elapsed_cpu)
        summaries
    in
    let iops =
      List.map
        (fun s -> s.index.nb_both.value_after_commit.diff /. s.elapsed_cpu)
        summaries
    in
    let read_iops =
      List.map
        (fun s -> s.index.nb_reads.value_after_commit.diff /. s.elapsed_cpu)
        summaries
    in
    let write_iops =
      List.map
        (fun s -> s.index.nb_writes.value_after_commit.diff /. s.elapsed_cpu)
        summaries
    in
    let max_ram =
      (* TODO: Use max rss (too?) *)
      List.map
        (fun s -> s.gc.major_heap_top_bytes.value_after_commit.max_value |> fst)
        summaries
    in
    let mean_cpu_usage =
      List.map (fun s -> s.elapsed_cpu /. s.elapsed_wall) summaries
    in
    [
      `Section "-- main metrics --";
      `Data (`SM, "CPU time elapsed", cpu_time_elapsed);
      `Data (`R3, "TZ-transactions per sec", tx_per_sec);
      `Data (`R3, "TZ-operations per sec", tz_ops_per_sec);
      `Data (`R3, "Context.add per sec", add_per_sec);
      `Data (`S3, "tail latency (1)", tail_latency);
      `Section "";
      `Section "-- resource usage --";
      `Section "disk IO (total)";
      `Data (`Ri, "  IOPS (op/sec)", iops);
      `Data (`RM, "  throughput (bytes/sec)", throughput);
      `Data (`RG, "  total (bytes)", bytes);
      `Section "disk IO (read)";
      `Data (`Ri, "  IOPS (op/sec)", read_iops);
      `Data (`RM, "  throughput (bytes/sec)", read_throughput);
      `Data (`RG, "  total (bytes)", read_bytes);
      `Section "disk IO (write)";
      `Data (`Ri, "  IOPS (op/sec)", write_iops);
      `Data (`RM, "  throughput (bytes/sec)", write_throughput);
      `Data (`RG, "  total (bytes)", written_bytes);
      `Section "";
      `Data (`RG, "max memory usage", max_ram);
      `Data (`P, "mean CPU usage", mean_cpu_usage);
    ]

  type data_row = [ `Data of scalar_format_fixed * string * float list ]
  type section_row = [ `Section of string ]

  let cells_of_data_row (`Data (scalar_format, row_name, scalars) : data_row) =
    let v0 = Stdlib.List.hd scalars in
    let pp_cell i v =
      let percent ppf =
        if i = 0 then ()
        else if scalar_format = `P then Format.fprintf ppf "     "
        else Format.fprintf ppf " %a" Utils.pp_percent (v /. v0)
      in
      Fmt.str "%a%t" pp_scalar_fixed (scalar_format, v) percent
    in

    Pb.text row_name
    :: (List.mapi pp_cell scalars
       |> List.map Pb.text
       |> List.map (Pb.align ~h:`Right ~v:`Top))

  let cells_of_section_row col_count (`Section name : section_row) =
    Pb.text name
    :: (Stdlib.List.init (col_count - 1) (Fun.const "") |> List.map Pb.text)

  let cells_of_row col_count = function
    | `Data _ as row -> cells_of_data_row row
    | `Section _ as row -> (cells_of_section_row col_count) row

  let matrix_of_rows col_count rows = List.map (cells_of_row col_count) rows
end

module Table2 = struct
  type variable = float * float * float * float * float
  (** total, min, max, avg, avg per sec *)

  type summary_floor =
    [ `Spacer
    | `Data of
      (scalar_format_fixed * scalar_format_fixed * scalar_format_fixed)
      * string
      * (string * variable) list ]

  let create_header_rows summaries =
    let only_one_summary = List.length summaries = 1 in
    [
      ("" :: (if only_one_summary then [] else [ "" ]))
      @ [
          "total";
          "min per block";
          "max per block";
          "avg per block";
          "avg per sec";
        ];
    ]
    |> Pb.matrix_to_text
    |> Pb.align_matrix `Center

  let floors_of_summaries : string list -> summary list -> summary_floor list =
   fun summary_names summaries ->
    let zip : (summary -> variable) -> (string * variable) list =
     fun variable_of_summary ->
      Stdlib.List.map2
        (fun sname s -> (sname, variable_of_summary s))
        summary_names summaries
    in
    let pb :
        ?scale:float ->
        ?f:_ ->
        string ->
        (summary -> Summary.bag_stat) ->
        summary_floor =
     fun ?(scale = 1.) ?(f = (`RM, `Ri, `R3)) stat_name lbs_of_summary ->
      let variables =
        zip (fun s ->
            let vs = (lbs_of_summary s).diff_per_block in
            let vs' = (lbs_of_summary s).value_after_commit in
            ( vs'.diff *. scale,
              fst vs.min_value *. scale,
              fst vs.max_value *. scale,
              vs.mean *. scale,
              vs.mean *. float_of_int s.block_count /. s.elapsed_wall *. scale
            ))
      in
      `Data (f, stat_name, variables)
    in
    let span_occu_per_block : [< Span.Key.t ] -> summary_floor =
     fun op ->
      let name = Fmt.str "%s count" (Span.Key.to_string op) in
      let op = (op :> Span.Key.t) in
      let variables =
        let open Summary in
        zip (fun s ->
            let vs = Span.(Map.find op s.span).count in
            let vs' = Span.(Map.find op s.span).cumu_count in
            ( vs'.diff,
              fst vs.min_value,
              fst vs.max_value,
              vs.mean,
              vs.mean *. float_of_int s.block_count /. s.elapsed_wall ))
      in
      `Data ((`Ri, `Ri, `R3), name, variables)
    in
    [ `Spacer ]
    @ List.map span_occu_per_block Span.Key.all_atoms_seen
    @ [
        `Spacer;
        pb ~f:(`RG, `RM, `RM) "Disk bytes read" (fun s -> s.index.bytes_read);
        pb
          ~f:(`RG, `RM, `RM)
          "Disk bytes written"
          (fun s -> s.index.bytes_written);
        pb ~f:(`RG, `RM, `RM) "Disk bytes both" (fun s -> s.index.bytes_both);
        `Spacer;
        pb "Disk reads" (fun s -> s.index.nb_reads);
        pb "Disk writes" (fun s -> s.index.nb_writes);
        pb "Disk both" (fun s -> s.index.nb_both);
        `Spacer;
        pb "pack.finds" (fun s -> s.pack.finds);
        pb "pack.cache_misses" (fun s -> s.pack.cache_misses);
        pb "pack.appended_hashes" (fun s -> s.pack.appended_hashes);
        pb "pack.appended_offsets" (fun s -> s.pack.appended_offsets);
        `Spacer;
        pb "tree.contents_hash" (fun s -> s.tree.contents_hash);
        pb "tree.contents_find" (fun s -> s.tree.contents_find);
        pb "tree.contents_add" (fun s -> s.tree.contents_add);
        pb "tree.node_hash" (fun s -> s.tree.node_hash);
        pb "tree.node_mem" (fun s -> s.tree.node_mem);
        pb "tree.node_add" (fun s -> s.tree.node_add);
        pb "tree.node_find" (fun s -> s.tree.node_find);
        pb "tree.node_val_v" (fun s -> s.tree.node_val_v);
        pb "tree.node_val_find" (fun s -> s.tree.node_val_find);
        pb "tree.node_val_list" (fun s -> s.tree.node_val_list);
        `Spacer;
        pb
          ~f:(`RG, `RM, `Ri)
          "index.cumu_data_bytes"
          (fun s -> s.index.cumu_data_bytes);
        `Spacer;
        pb
          ~f:(`RG, `RM, `RM)
          "gc.minor_words allocated"
          (fun s -> s.gc.minor_words);
        pb
          ~f:(`RG, `RM, `RM)
          "gc.major_words allocated"
          (fun s -> s.gc.major_words);
        pb "gc.minor_collections" (fun s -> s.gc.minor_collections);
        pb "gc.major_collections" (fun s -> s.gc.major_collections);
        `Spacer;
        pb "rusage.utime" ~f:(`Ri, `R3, `R3) (fun s -> s.rusage.utime);
        pb "rusage.stime" ~f:(`Ri, `R3, `R3) (fun s -> s.rusage.stime);
        (* pb "rusage.maxrss" ~scale:1000. (fun s -> s.rusage.maxrss); *)
        pb "rusage.minflt" (fun s -> s.rusage.minflt);
        pb ~f:(`Ri, `Ri, `R3) "rusage.majflt" (fun s -> s.rusage.majflt);
        pb "rusage.inblock" (fun s -> s.rusage.inblock);
        pb "rusage.oublock" (fun s -> s.rusage.oublock);
        pb "rusage.nvcsw" (fun s -> s.rusage.nvcsw);
        pb "rusage.nivcsw" (fun s -> s.rusage.nivcsw);
      ]

  let matrix_of_data_floor
      (`Data
        ( (scalar_format_c, scalar_format_a, scalar_format_b),
          floor_name,
          names_and_variables )) =
    let only_one_summary = List.length names_and_variables = 1 in
    let _, variables = List.split names_and_variables in
    let total0, min0, max0, avg0, avg_ps0 = Stdlib.List.hd variables in

    let box_of_scalar scalar_format row_idx v0 v =
      let ratio = v /. v0 in
      let show_percent =
        if only_one_summary then
          (* Percents are only needed for comparisons between summaries. *)
          `No
        else if Float.is_finite ratio = false then
          (* Nan and infinite percents are ugly. *)
          `Shadow
        else if row_idx = 0 then
          (* The first row of a floor is always 100%, it is prettier without
             displaying it. *)
          `Shadow
        else `Yes
      in
      let pp_percent ppf =
        match show_percent with
        | `Yes -> Format.fprintf ppf " %a" Utils.pp_percent ratio
        | `Shadow -> Format.fprintf ppf "     "
        | `No -> ()
      in
      let pp_scalar ppf = pp_scalar_fixed ppf (scalar_format, v) in
      Fmt.str "%t%t" pp_scalar pp_percent
      |> Pb.text
      |> Pb.align ~h:`Right ~v:`Top
    in
    let rows =
      List.mapi
        (fun row_idx (summary_name, variable) ->
          let a = Pb.text (if row_idx = 0 then floor_name else "") in
          let b = if only_one_summary then [] else [ Pb.text summary_name ] in
          let c =
            let total, min, max, avg, avg_ps = variable in
            [
              box_of_scalar scalar_format_c row_idx total0 total;
              box_of_scalar scalar_format_a row_idx min0 min;
              box_of_scalar scalar_format_a row_idx max0 max;
              box_of_scalar scalar_format_b row_idx avg0 avg;
              box_of_scalar scalar_format_b row_idx avg_ps0 avg_ps;
            ]
          in
          (a :: b) @ c)
        names_and_variables
    in
    rows

  let matrix_of_floor col_count = function
    | `Spacer -> [ Stdlib.List.init col_count (Fun.const "") ] |> Pb.matrix_to_text
    | `Data _ as floor -> matrix_of_data_floor floor
end

module Table3 = struct
  type variable = float * float * float
  (** min, max, avg *)

  type summary_floor =
    [ `Spacer
    | `Data of
      (scalar_format_fixed * scalar_format_fixed)
      * string
      * (string * variable) list ]

  let create_header_rows summaries =
    let only_one_summary = List.length summaries = 1 in
    [
      ("" :: (if only_one_summary then [] else [ "" ]))
      @ [ "min"; "max"; "avg" ];
    ]
    |> Pb.matrix_to_text
    |> Pb.align_matrix `Center

  let floors_of_summaries : string list -> summary list -> summary_floor list =
   fun summary_names summaries ->
    let zip : (summary -> variable) -> (string * variable) list =
     fun variable_of_summary ->
      Stdlib.List.map2
        (fun sname s -> (sname, variable_of_summary s))
        summary_names summaries
    in

    let v : ?f:_ -> string -> (summary -> Summary.bag_stat) -> summary_floor =
     fun ?(f = (`RM, `RM)) stat_name lbs_of_summary ->
      let variables =
        zip (fun s ->
            let vs = (lbs_of_summary s).value_after_commit in
            (fst vs.min_value, fst vs.max_value, vs.mean))
      in
      `Data (f, stat_name, variables)
    in
    let cpu_usage_variables =
      zip (fun s ->
          let vs = s.cpu_usage in
          (fst vs.min_value, fst vs.max_value, vs.mean))
    in
    let span_durations : ?f:_ -> [< Span.Key.t ] -> summary_floor =
     fun ?(f = (`Sm, `Su)) op ->
      let name = Fmt.str "%s duration (s)" (Span.Key.to_string op) in
      let op = (op :> Span.Key.t) in
      let variables =
        let open Summary in
        zip (fun s ->
            let vs = Span.(Map.find op s.span).duration in
            (fst vs.min_value, fst vs.max_value, vs.mean))
      in
      `Data (f, name, variables)
    in
    [
      `Spacer;
      span_durations ~f:(`S3, `Sm) `Block;
      span_durations ~f:(`S3, `Sm) `Buildup;
      span_durations ~f:(`S3, `Sm) `Commit;
      `Spacer;
    ]
    @ List.map span_durations
        (List.filter (( <> ) (`Frequent_op `Init)) Span.Key.all_frequent_ops)
    @ [
        span_durations ~f:(`S3, `S3) (`Frequent_op `Init);
        span_durations ~f:(`S3, `S3) `Close;
        span_durations ~f:(`S3, `Sm) `Unseen;
        `Spacer;
        v "Major heap bytes after commit" (fun s -> s.gc.major_heap_bytes);
        `Spacer;
        `Data ((`P, `P), "CPU Usage", cpu_usage_variables);
      ]

  let matrix_of_data_floor
      (`Data
        ((scalar_format_a, scalar_format_b), floor_name, names_and_variables)) =
    let only_one_summary = List.length names_and_variables = 1 in
    let _, variables = List.split names_and_variables in
    let min0, max0, avg0 = Stdlib.List.hd variables in

    let box_of_scalar scalar_format row_idx v0 v =
      let ratio = v /. v0 in
      let show_percent =
        if only_one_summary then
          (* Percents are only needed for comparisons between summaries. *)
          `No
        else if Float.is_finite ratio = false then
          (* Nan and infinite percents are ugly. *)
          `Shadow
        else if row_idx = 0 then
          (* The first row of a floor is always 100%, it is prettier without
             displaying it. *)
          `Shadow
        else if scalar_format = `P then `Shadow
        else `Yes
      in
      let pp_percent ppf =
        match show_percent with
        | `Yes -> Format.fprintf ppf " %a" Utils.pp_percent ratio
        | `Shadow -> Format.fprintf ppf "     "
        | `No -> ()
      in
      let pp_scalar ppf = pp_scalar_fixed ppf (scalar_format, v) in

      Fmt.str "%t%t" pp_scalar pp_percent
      |> Pb.text
      |> Pb.align ~h:`Right ~v:`Top
    in
    let rows =
      List.mapi
        (fun row_idx (summary_name, variable) ->
          let a = Pb.text (if row_idx = 0 then floor_name else "") in
          let b = if only_one_summary then [] else [ Pb.text summary_name ] in
          let c =
            let min, max, avg = variable in
            [
              box_of_scalar scalar_format_b row_idx min0 min;
              box_of_scalar scalar_format_a row_idx max0 max;
              box_of_scalar scalar_format_b row_idx avg0 avg;
            ]
          in
          (a :: b) @ c)
        names_and_variables
    in
    rows

  let matrix_of_floor col_count = function
    | `Spacer -> [ Stdlib.List.init col_count (Fun.const "") ] |> Pb.matrix_to_text
    | `Data _ as floor -> matrix_of_data_floor floor
end

(** Curves *)
module Table4 = struct
  type scalar_format_auto = [ `R | `S ]
  (** Real / Seconds *)

  type scalar_format = [ scalar_format_auto | scalar_format_fixed ]

  type summary_floor =
    [ `Spacer | `Data of scalar_format * string * (string * curve) list ]
  (** A [summary_floor] of tag [`Data] contains all the data necessary in order
      to print a bunch of rows, 1 per summary, all displaying the same summary
      entry. *)

  let sum_curves curves =
    curves
    |> Pb.transpose_matrix
    |> List.map
         (List.fold_left
            (fun acc v -> if Float.is_nan v then acc else acc +. v)
            0.)

  let div_curves a b = Stdlib.List.map2 ( /. ) a b
  let mul_curves a b = Stdlib.List.map2 ( *. ) a b
  let mul_curve_scalar a v = List.map (( *. ) v) a

  let create_header_rows sample_count summaries =
    let only_one_summary = List.length summaries = 1 in
    let s = Stdlib.List.hd summaries in
    let played_count_curve =
      Stdlib.List.init s.curves_sample_count (fun i ->
          float_of_int i
          /. float_of_int (s.curves_sample_count - 1)
          *. float_of_int s.block_count)
    in
    let played_count_curve =
      Utils.Resample.resample_vector `Next_neighbor played_count_curve
        sample_count
      |> Array.of_list
    in
    let block_level_curve =
      Utils.Resample.resample_vector `Next_neighbor
        s.block_specs.level_over_blocks sample_count
      |> Array.of_list
    in
    let header_cells_per_col_idx col_idx =
      let played_count = played_count_curve.(col_idx) in
      let block_level = block_level_curve.(col_idx) in
      let progress_blocks = played_count /. float_of_int s.block_count in
      let h0 =
        if progress_blocks = 0. then "0 (before)"
        else if progress_blocks = 1. then
          Printf.sprintf "%.0f (end)" played_count
        else if Float.is_integer played_count then
          Printf.sprintf "%.0f" played_count
        else Printf.sprintf "%.1f" played_count
      in
      let h1 = Printf.sprintf "%.0f%%" (progress_blocks *. 100.) in
      let h2 = Printf.sprintf "%#d" (int_of_float block_level) in
      [ h0; h1; h2 ]
    in
    let col_a =
      [ [ "Block played count *C"; "Blocks progress *C"; "Block level" ] ]
      |> Pb.matrix_to_text
    in
    let col_b =
      (if only_one_summary then [] else [ [ ""; ""; "" ] ])
      |> Pb.matrix_to_text
      |> Pb.align_matrix `Center
    in
    let cols_c =
      Stdlib.List.init sample_count header_cells_per_col_idx
      |> Pb.matrix_to_text
      |> Pb.align_matrix `Center
    in
    col_a @ col_b @ cols_c |> Pb.transpose_matrix

  let floors_of_summaries : string list -> summary list -> summary_floor list =
   fun summary_names summaries ->
    (* Step 1/3 - Prepare the "/data/..." directories floors *)
    let floor_per_node : summary_floor list =
      List.map
        (fun key ->
          let path = Stdlib.List.assoc key Def.path_per_watched_node in
          let name = Printf.sprintf "%s *S" path in
          let curves =
            List.map
              (fun s ->
                (Summary.Watched_node.Map.find key s.store.watched_nodes).value
                  .evolution)
              summaries
          in
          let l = Stdlib.List.combine summary_names curves in
          `Data (`R, name, l))
        Def.watched_nodes
    in

    (* Step 2/3 - Prepare the functions to build all the simple floors *)
    let zip : (summary -> curve) -> (string * curve) list =
     fun curve_of_summary ->
      Stdlib.List.map2
        (fun sname s -> (sname, curve_of_summary s))
        summary_names summaries
    in
    let zip_per_block_to_per_sec : (summary -> curve) -> (string * curve) list =
      let sec_per_block =
        List.map
          (fun s -> Summary.(Span.Map.find `Block s.span).duration.evolution)
          summaries
      in
      fun curve_of_summary ->
        Stdlib.List.map2
          (fun (sname, sec_per_block) s ->
            (sname, div_curves (curve_of_summary s) sec_per_block))
          (Stdlib.List.combine summary_names sec_per_block)
          summaries
    in

    let v :
        ?scale:float ->
        ?f:_ ->
        string ->
        (summary -> Summary.bag_stat) ->
        summary_floor =
     fun ?(scale = 1.) ?(f = `R) stat_name lbs_of_summary ->
      let curves =
        zip (fun s ->
            let c = (lbs_of_summary s).value_after_commit.evolution in
            mul_curve_scalar c scale)
      in
      `Data (f, stat_name, curves)
    in
    let pb : ?f:_ -> string -> (summary -> Summary.bag_stat) -> summary_floor =
     fun ?(f = `R) stat_name lbs_of_summary ->
      let curves = zip (fun s -> (lbs_of_summary s).diff_per_block.evolution) in
      `Data (f, stat_name, curves)
    in
    let ps : ?f:_ -> string -> (summary -> Summary.bag_stat) -> summary_floor =
     fun ?(f = `R) stat_name lbs_of_summary ->
      let curves =
        zip_per_block_to_per_sec (fun s ->
            (lbs_of_summary s).diff_per_block.evolution)
      in
      `Data (f, stat_name, curves)
    in

    let span_occu_count : [< Span.Key.t ] -> summary_floor =
     fun op ->
      let name = Fmt.str "%s count per block *LA" (Span.Key.to_string op) in
      let op = (op :> Span.Key.t) in
      let curves =
        zip (fun s -> Summary.(Span.Map.find op s.span).count.evolution)
      in
      `Data (`R3, name, curves)
    in
    let span_duration : _ -> [< Span.Key.t ] -> summary_floor =
     fun f op ->
      let name = Fmt.str "%s duration *LA" (Span.Key.to_string op) in
      let op = (op :> Span.Key.t) in
      let curves =
        zip (fun s -> Summary.(Span.Map.find op s.span).duration.evolution)
      in
      `Data (f, name, curves)
    in

    let ev_count = zip (fun s -> s.block_specs.ev_count.value.evolution) in
    let tz_tx_count =
      zip (fun s -> s.block_specs.tzop_count_tx.value.evolution)
    in
    let tz_ops_count =
      zip (fun s -> s.block_specs.tzop_count.value.evolution)
    in
    let tz_contract_count =
      zip (fun s -> s.block_specs.tzop_count_contract.value.evolution)
    in

    let ev_count_pb =
      zip (fun s -> s.block_specs.ev_count.diff_per_block.evolution)
    in
    let tz_tx_count_pb =
      zip (fun s -> s.block_specs.tzop_count_tx.diff_per_block.evolution)
    in
    let tz_ops_count_pb =
      zip (fun s -> s.block_specs.tzop_count.diff_per_block.evolution)
    in
    let tz_contract_count_pb =
      zip (fun s -> s.block_specs.tzop_count_contract.diff_per_block.evolution)
    in

    (* Step 3/3 - Build the final list of floors *)
    [
      `Spacer;
      `Data
        (`S, "Wall time elapsed *C", zip (fun s -> s.elapsed_wall_over_blocks));
      `Data (`S, "CPU time elapsed *C", zip (fun s -> s.elapsed_cpu_over_blocks));
      `Data (`P, "CPU Usage *LA", zip (fun s -> s.cpu_usage.evolution));
      (* ops counts *)
      `Spacer;
      `Data (`R, "TZ-operations count *C", tz_ops_count);
      `Data (`R, "TZ-transaction count *C", tz_tx_count);
      `Data (`R, "TZ-contrat count *C", tz_contract_count);
      `Data (`R, "Context op count *C", ev_count);
      `Data (`R, "TZ-operations per block *C", tz_ops_count_pb);
      `Data (`R, "TZ-transaction per block *C", tz_tx_count_pb);
      `Data (`R, "TZ-contrat per block *C", tz_contract_count_pb);
      `Data (`R, "Context op per block *C", ev_count_pb);
      (* <op> per sec *)
      `Spacer;
      (* <phase> duration *)
    ]
    @ List.map span_occu_count Span.Key.all_atoms_seen
    @ [
        `Spacer;
        span_duration `Sm `Block;
        span_duration `Sm `Buildup;
        span_duration `Sm `Commit;
        `Spacer;
        (* <op> duration *)
      ]
    @ List.map (span_duration `Su) Span.Key.all_frequent_ops
    @ [
        span_duration `Sm `Close;
        span_duration `Sm `Unseen;
        (* derived from bag_of_stat *)
        `Spacer;
        v "Disk bytes    read *C" (fun s -> s.index.bytes_read);
        v "Disk bytes written *C" (fun s -> s.index.bytes_written);
        v "Disk bytes    both *C" (fun s -> s.index.bytes_both);
        pb ~f:`Ri "Disk bytes read    per block *LA" (fun s ->
            s.index.bytes_read);
        pb ~f:`Ri "Disk bytes written per block *LA" (fun s ->
            s.index.bytes_written);
        pb ~f:`Ri "Disk bytes both    per block *LA" (fun s ->
            s.index.bytes_both);
        ps ~f:`RM "Disk bytes read    per sec *LA *N" (fun s ->
            s.index.bytes_read);
        ps ~f:`RM "Disk bytes written per sec *LA *N" (fun s ->
            s.index.bytes_written);
        ps ~f:`RM "Disk bytes both    per sec *LA *N" (fun s ->
            s.index.bytes_both);
        `Spacer;
        v "Disk read  count *C" (fun s -> s.index.nb_reads);
        v "Disk write count *C" (fun s -> s.index.nb_writes);
        v "Disk both  count *C" (fun s -> s.index.nb_both);
        pb ~f:`R3 "Disk read  count per block *LA" (fun s -> s.index.nb_reads);
        pb ~f:`R3 "Disk write count per block *LA" (fun s -> s.index.nb_writes);
        pb ~f:`R3 "Disk both  count per block *LA" (fun s -> s.index.nb_both);
        ps ~f:`Ri "Disk read  count per sec *LA *N" (fun s -> s.index.nb_reads);
        ps ~f:`Ri "Disk write count per sec *LA *N" (fun s -> s.index.nb_writes);
        ps ~f:`Ri "Disk both  count per sec *LA *N" (fun s -> s.index.nb_both);
        `Spacer;
        pb "pack.finds per block *LA" (fun s -> s.pack.finds);
        pb "pack.cache_misses per block *LA" (fun s -> s.pack.cache_misses);
        pb "pack.appended_hashes per block *LA" (fun s ->
            s.pack.appended_hashes);
        pb "pack.appended_offsets per block *LA" (fun s ->
            s.pack.appended_offsets);
        `Spacer;
        pb "tree.contents_hash per block *LA" (fun s -> s.tree.contents_hash);
        pb "tree.contents_find per block *LA" (fun s -> s.tree.contents_find);
        pb "tree.contents_add per block *LA" (fun s -> s.tree.contents_add);
        pb "tree.node_hash per block *LA" (fun s -> s.tree.node_hash);
        pb "tree.node_mem per block *LA" (fun s -> s.tree.node_mem);
        pb "tree.node_add per block *LA" (fun s -> s.tree.node_add);
        pb "tree.node_find per block *LA" (fun s -> s.tree.node_find);
        pb "tree.node_val_v per block *LA" (fun s -> s.tree.node_val_v);
        pb "tree.node_val_find per block *LA" (fun s -> s.tree.node_val_find);
        pb "tree.node_val_list per block *LA" (fun s -> s.tree.node_val_list);
        `Spacer;
        v "index.nb_merge *C" (fun s -> s.index.nb_merge);
        v "index.cumu_data_bytes *C" (fun s -> s.index.cumu_data_bytes);
        pb "index.cumu_data_bytes per block *LA" (fun s ->
            s.index.cumu_data_bytes);
        `Spacer;
        v "gc.minor_words allocated *C" (fun s -> s.gc.minor_words);
        pb "gc.minor_words allocated per block *LA" (fun s -> s.gc.minor_words);
        v "gc.promoted_words *C" (fun s -> s.gc.promoted_words);
        v "gc.major_words allocated *C" (fun s -> s.gc.major_words);
        pb "gc.major_words allocated per block *LA" (fun s -> s.gc.major_words);
        v "gc.minor_collections *C" (fun s -> s.gc.minor_collections);
        pb "gc.minor_collections per block *LA" (fun s ->
            s.gc.minor_collections);
        v "gc.major_collections *C" (fun s -> s.gc.major_collections);
        pb "gc.major_collections per block *LA" (fun s ->
            s.gc.major_collections);
        v "gc.compactions *C" (fun s -> s.gc.compactions);
        `Spacer;
        v ~f:`RM "gc.major heap bytes top *C" (fun s ->
            s.gc.major_heap_top_bytes);
        v ~f:`RM "gc.major heap bytes *LA" (fun s -> s.gc.major_heap_bytes);
        `Spacer;
        v "rusage.utime *C" ~f:`S (fun s -> s.rusage.utime);
        pb "rusage.utime per block *LA" ~f:`S (fun s -> s.rusage.utime);
        v "rusage.stime *C" ~f:`S (fun s -> s.rusage.stime);
        pb "rusage.stime per block *LA" ~f:`S (fun s -> s.rusage.stime);
        v "rusage.maxrss *C" ~scale:1000. (fun s -> s.rusage.maxrss);
        (* pb "rusage.maxrss per block *LA" (fun s -> s.rusage.maxrss); *)
        v "rusage.minflt *C" (fun s -> s.rusage.minflt);
        pb "rusage.minflt per block *LA" (fun s -> s.rusage.minflt);
        v "rusage.majflt *C" (fun s -> s.rusage.majflt);
        pb "rusage.majflt per block *LA" (fun s -> s.rusage.majflt);
        v "rusage.inblock *C" (fun s -> s.rusage.inblock);
        pb "rusage.inblock per block *LA" (fun s -> s.rusage.inblock);
        v "rusage.oublock *C" (fun s -> s.rusage.oublock);
        pb "rusage.oublock per block *LA" (fun s -> s.rusage.oublock);
        v "rusage.nvcsw *C" (fun s -> s.rusage.nvcsw);
        pb "rusage.nvcsw per block *LA" (fun s -> s.rusage.nvcsw);
        v "rusage.nivcsw *C" (fun s -> s.rusage.nivcsw);
        pb "rusage.nivcsw per block *LA" (fun s -> s.rusage.nivcsw);
        `Spacer;
        v "index_data bytes *S" (fun s -> s.disk.index_data);
        pb "index_data bytes per block *LA" (fun s -> s.disk.index_data);
        v "store_pack bytes *S" (fun s -> s.disk.store_pack);
        pb "store_pack bytes per block *LA" (fun s -> s.disk.store_pack);
        v "index_log bytes *S" (fun s -> s.disk.index_log);
        v "index_log_async *S" (fun s -> s.disk.index_log_async);
        v "store_dict bytes *S" (fun s -> s.disk.store_dict);
        `Spacer;
      ]
    @ floor_per_node

  let resample_curves_of_floor sample_count = function
    | `Data (a, b, names_and_curves) ->
        let names, curves = List.split names_and_curves in
        let curves =
          List.map
            (fun curve ->
              Utils.Resample.resample_vector `Next_neighbor curve sample_count)
            curves
        in
        `Data (a, b, Stdlib.List.combine names curves)
    | `Spacer -> `Spacer

  let matrix_of_data_floor (`Data (scalar_format, floor_name, names_and_curves))
      =
    let only_one_summary = List.length names_and_curves = 1 in
    let _, curves = List.split names_and_curves in
    let pp_real = Utils.create_pp_real (List.concat curves) in
    let pp_seconds = Utils.create_pp_seconds (List.concat curves) in
    let curve0 = Stdlib.List.hd curves in
    let box_of_scalar row_idx col_idx (v0, v) =
      let ratio = v /. v0 in
      let show_percent =
        if only_one_summary then
          (* Percents are only needed for comparisons between summaries. *)
          `No
        else if col_idx = 0 then
          (* The first columns is usually full of NaNs, showing percents there
             is a waste of space. *)
          `No
        else if Float.is_finite ratio = false then
          (* Nan and infinite percents are ugly. *)
          `Shadow
        else if row_idx = 0 then
          (* The first row of a floor is always 100%, it is prettier without
             displaying it. *)
          `Shadow
        else if scalar_format = `P then `Shadow
        else `Yes
      in
      let pp_percent ppf =
        match show_percent with
        | `Yes -> Format.fprintf ppf " %a" Utils.pp_percent ratio
        | `Shadow -> Format.fprintf ppf "     "
        | `No -> ()
      in
      let pp_scalar ppf =
        match scalar_format with
        | `R -> Format.fprintf ppf "%a" pp_real v
        | `S -> Format.fprintf ppf "%a" pp_seconds v
        | #scalar_format_fixed as scalar_format ->
            pp_scalar_fixed ppf (scalar_format, v)
      in
      Fmt.str "%t%t" pp_scalar pp_percent
      |> Pb.text
      |> Pb.align ~h:`Right ~v:`Top
    in
    let rows =
      List.mapi
        (fun row_idx (summary_name, curve) ->
          let a = Pb.text (if row_idx = 0 then floor_name else "") in
          let b = if only_one_summary then [] else [ Pb.text summary_name ] in
          let c =
            List.mapi (box_of_scalar row_idx) (Stdlib.List.combine curve0 curve)
          in
          (a :: b) @ c)
        names_and_curves
    in
    rows

  let matrix_of_floor col_count = function
    | `Spacer -> [ Stdlib.List.init col_count (Fun.const "") ] |> Pb.matrix_to_text
    | `Data _ as floor -> matrix_of_data_floor floor
end

let unsafe_pp sample_count ppf summary_names (summaries : Summary.t list) =
  let block_count =
    let l = List.map (fun s -> s.block_count) summaries in
    let v = Stdlib.List.hd l in
    if List.exists (fun v' -> v' <> v) l then
      Stdlib.failwith "Can't pp together summaries with a different `block_count`";
    v
  in
  let moving_average_half_life_ratio =
    let l = List.map (fun s -> s.moving_average_half_life_ratio) summaries in
    let v = Stdlib.List.hd l in
    if List.exists (fun v' -> v' <> v) l then
      Stdlib.failwith
        "Can't pp together summaries with a different \
         `moving_average_half_life_ratio`";
    v
  in
  let table0 =
    Table0.box_of_summaries_config summary_names summaries
    |> Pb.matrix_with_column_spacers
    |> Pb.grid_l ~bars:false
    |> PrintBox_text.to_string
  in
  let table1 =
    let only_one_summary = List.length summaries = 1 in
    let header_rows =
      (if only_one_summary then [] else [ "" :: summary_names ])
      |> Pb.matrix_to_text
      |> Pb.align_matrix `Center
    in
    let col_count = List.length summaries + 1 in
    let body_rows =
      Table1.rows_of_summaries summaries |> Table1.matrix_of_rows col_count
    in
    header_rows @ body_rows
    |> Pb.matrix_with_column_spacers
    |> Pb.grid_l ~bars:false
    |> PrintBox_text.to_string
  in
  let table2 =
    let header_rows = Table2.create_header_rows summaries in
    let body_rows =
      let col_count = 5 + 1 + if List.length summaries = 1 then 0 else 1 in
      Table2.floors_of_summaries summary_names summaries
      |> List.map (Table2.matrix_of_floor col_count)
      |> List.concat
    in
    header_rows @ body_rows
    |> Pb.matrix_with_column_spacers
    |> Pb.grid_l ~bars:false
    |> PrintBox_text.to_string
  in
  let table3 =
    let header_rows = Table3.create_header_rows summaries in
    let body_rows =
      let col_count = 4 + 1 + if List.length summaries = 1 then 0 else 1 in
      Table3.floors_of_summaries summary_names summaries
      |> List.map (Table3.matrix_of_floor col_count)
      |> List.concat
    in
    header_rows @ body_rows
    |> Pb.matrix_with_column_spacers
    |> Pb.grid_l ~bars:false
    |> PrintBox_text.to_string
  in
  let table4 =
    let header_rows = Table4.create_header_rows sample_count summaries in
    let body_rows =
      let col_count =
        sample_count + 1 + if List.length summaries = 1 then 0 else 1
      in
      Table4.floors_of_summaries summary_names summaries
      |> List.map (Table4.resample_curves_of_floor sample_count)
      |> List.map (Table4.matrix_of_floor col_count)
      |> List.concat
    in
    header_rows @ body_rows
    |> Pb.matrix_with_column_spacers
    |> Pb.grid_l ~bars:false
    |> PrintBox_text.to_string
  in
  fprintf_result ppf table0 table1 table2 table3 table4
    (moving_average_half_life_ratio *. float_of_int (block_count + 1))

let pp sample_count ppf (summary_names, summaries) =
  if List.length summaries = 0 then ()
  else unsafe_pp sample_count ppf summary_names summaries
