(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2021 DaiLambda, Inc. <contact@dailambda.jp>                 *)
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

open Context.Proof
open Lib_test.Qcheck2_helpers
open QCheck2

module Store = struct
  open Tezos_context_encoding.Context
  module Maker = Irmin_pack_unix.Maker (Conf)
  include Maker.Make (Schema)
  module Schema = Tezos_context_encoding.Context.Schema
end

module Proof = Tezos_context_helpers.Context.Make_proof (Store)
open Tezos_context_helpers.Context.Proof_encoding_V1

module Gen = struct
  include Gen

  let step : step t = string_size ~gen:printable (int_range 3 10)

  let value : value t =
    let+ s = string_size ~gen:char (int_range 3 10) in
    Bytes.unsafe_of_string s

  let hash =
    let+ s = string_size ~gen:char (return Context_hash.size) in
    Context_hash.of_string_exn s

  let rec comb n xs =
    match (n, xs) with
    | (0, _) -> Gen.return []
    | (_, []) -> assert false
    | (1, [x]) -> Gen.return [x]
    | (n, x :: xs) ->
        (* prob.  n / length xs *)
        let* m = int_bound (List.length (x :: xs) - 1) in
        if m < n then
          let+ ys = comb (n - 1) xs in
          x :: ys
        else comb n xs

  let inode width gen_a =
    let* length = int_range 1 1000_000 in
    let+ proofs =
      let* n = int_bound (min 32 (max 5 width) - 1) >|= ( + ) 1 in
      let* indices = comb n (Stdlib.List.init 32 (fun x -> x)) in
      flatten_l
      @@ List.map
           (fun i ->
             let+ a = gen_a in
             (i, a))
           indices
    in
    (* no invariant assurance at all :-P *)
    {length; proofs}

  let inode_extender gen_a =
    let* length = int_range 1 10 in
    let* segment = list_size (int_bound 5 >|= ( + ) 1) (int_bound 4) in
    let+ proof = gen_a in
    {length; segment; proof}

  let rec inode_tree (depth, width) =
    if depth <= 0 then
      let+ hash = hash in
      Blinded_inode hash
    else
      int_bound 3 >>= function
      | 0 ->
          let+ hash = hash in
          Blinded_inode hash
      | 1 ->
          let+ xs =
            list_size
              (int_bound 3 >|= ( + ) 1)
              (pair step (tree (depth - 1, width)))
          in
          Inode_values xs
      | 2 ->
          let+ i = inode width (inode_tree (depth - 1, width / 2)) in
          Inode_tree i
      | 3 ->
          let+ i = inode_extender (inode_tree (depth - 1, width)) in
          (Inode_extender i : inode_tree)
      | _ -> assert false

  and tree (depth, width) : tree t =
    if depth <= 0 then
      int_bound 2 >>= function
      | 0 ->
          let+ v = value in
          (Value v : tree)
      | 1 ->
          let+ h = hash in
          Blinded_value h
      | 2 ->
          let+ h = hash in
          Blinded_node h
      | _ -> assert false
    else
      int_bound 5 >>= function
      | 0 ->
          let+ v = value in
          (Value v : tree)
      | 1 ->
          let+ h = hash in
          Blinded_value h
      | 2 ->
          let+ xs =
            list_size (int_bound 4 >|= ( + ) 1)
            @@ pair step (tree (depth - 1, width))
          in
          (Node xs : tree)
      | 3 ->
          let+ h = hash in
          Blinded_node h
      | 4 ->
          let+ i = inode width (inode_tree (depth - 1, width / 2)) in
          (Inode i : tree)
      | 5 ->
          let+ i = inode_extender (inode_tree (depth - 1, width)) in
          Extender i
      | _ -> assert false

  let kinded_hash =
    let* h = hash in
    bool >|= function true -> `Value h | false -> `Node h

  let tree_proof =
    let* version = int_bound 3 in
    let* kh1 = kinded_hash in
    let* kh2 = kinded_hash in
    let+ state = tree (5, 64) in
    {version; before = kh1; after = kh2; state}

  module Stream = struct
    open Stream

    let elt =
      int_bound 3 >>= function
      | 0 ->
          let+ v = value in
          Value v
      | 1 ->
          let+ sks =
            list_size (int_bound 4 >|= ( + ) 1) @@ pair step kinded_hash
          in
          Node sks
      | 2 ->
          let max_indices = 5 in
          let+ i = inode max_indices hash in
          Inode i
      | 3 ->
          let+ i = inode_extender hash in
          Inode_extender i
      | _ -> assert false

    let t : Stream.t Gen.t =
      let+ xs = list_size (int_bound 10 >|= ( + ) 1) elt in
      List.to_seq xs
  end

  let stream_proof =
    let* version = int_bound 3 in
    let* kh1 = kinded_hash in
    let* kh2 = kinded_hash in
    let+ state = Stream.t in
    {version; before = kh1; after = kh2; state}
end

let encoding_test enc eq data =
  let b = Data_encoding.Binary.to_bytes_exn enc data in
  let data' = Data_encoding.Binary.of_bytes_exn enc b in
  if not @@ eq data data' then false
  else
    let j = Data_encoding.Json.construct enc data in
    (* Format.eprintf "%a@." Data_encoding.Json.pp j ; *)
    let data' = Data_encoding.Json.destruct enc j in
    eq data data'

let test_sample () =
  let sample =
    let bytes s = Bytes.of_string s in
    let tree_a : tree = Value (bytes "a") in
    let ch =
      Context_hash.of_bytes_exn (bytes "01234567890123456789012345678901")
    in
    let tree_b =
      Extender
        {
          length = 10;
          segment = [0; 1; 0; 1];
          proof = Inode_tree {length = 8; proofs = [(0, Blinded_inode ch)]};
        }
    in
    let tree_c =
      Extender
        {
          length = 10;
          segment = [1; 2; 3; 4; 5; 6; 7; 8];
          proof = Inode_values [("z", tree_b)];
        }
    in
    let inode_tree : inode_tree =
      Inode_values [("a", tree_a); ("b", tree_b); ("c", tree_c)]
    in
    let tree = Inode {length = 100; proofs = [(0, inode_tree)]} in
    {version = 1; before = `Value ch; after = `Node ch; state = tree}
  in
  assert (encoding_test tree_proof_encoding ( = ) sample) ;

  let sample_large_inode_tree inode_proof_size =
    let bytes s = Bytes.of_string s in
    let tree_a c : tree = Value (bytes @@ "a" ^ c) in
    let ch =
      Context_hash.of_bytes_exn (bytes "01234567890123456789012345678901")
    in
    let inode_tree i : inode_tree =
      let c = string_of_int i in
      Inode_values [(c ^ "a", tree_a c)]
    in
    let proofs =
      let rec aux i acc =
        if i >= inode_proof_size then acc
        else aux (i + 1) ((i * 15 mod 32, inode_tree i) :: acc)
      in
      List.sort (fun (i, _) (j, _) -> compare i j) (aux 0 [])
    in
    let tree = Inode {length = 100; proofs} in
    {version = 1; before = `Value ch; after = `Node ch; state = tree}
  in
  assert (encoding_test tree_proof_encoding ( = ) (sample_large_inode_tree 20)) ;
  assert (encoding_test tree_proof_encoding ( = ) (sample_large_inode_tree 32))

let test_random_tree_proof =
  QCheck2.Test.make
    ~name:"tree_proof_encoding"
    Gen.tree_proof
    (encoding_test tree_proof_encoding ( = ))

let test_random_stream_proof =
  QCheck2.Test.make
    ~name:"stream_proof_encoding"
    Gen.stream_proof
    (encoding_test
       stream_proof_encoding
       (fun (* stream proof uses Seq.t *)
              a b ->
         a.version = b.version && a.before = b.before && a.after = b.after
         && List.of_seq a.state = List.of_seq b.state))

let tests_random = [test_random_tree_proof; test_random_stream_proof]

let () =
  Alcotest.run
    "test_merkle_proof"
    [
      ("sample", [("sample", `Quick, test_sample)]);
      ("random", qcheck_wrap tests_random);
    ]
