(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
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

(** Persistent block store with linear history

    The cemented block store is store where blocks are stored linearly
    (by level) in chunks. Blocks in this store should not be
    reorganized anymore and are thus *cemented*. As these blocks should
    not be accessed regularly and especially their metadata (if
    present), the later are compressed using a zip format to save disk
    space. For each chunk of blocks, a dedicated file is
    used. Moreover, to enable easy access and to prevent too much
    on-disk reading, two indexed maps are used to retrieve blocks hash
    from their level and their level from the block hash.

    The cemented block store contains a set of files updated each time
    a new chunk is added to the store. These files indicate which
    interval of blocks (w.r.t. their levels) are stored in it.

    {1 Invariants}

    This store is expected to respect the following invariants:

    - A cemented chunk of blocks that is represented by the interval
      [ i ; j ] (with i <= j) contains | j - i + 1 | blocks and are
      ordered from i to j in the file.

    - The set F of cemented chunks is always ordered by block level.

    - The cemented store does not contain holes: let F be the cemented
      chunks, if |F| > 1 then:

      ∀f_x=(i,j) ∈ F ∧ x < |F|, ∃f_y =(i', j'), x = y - 1 ∧ j + 1 = j'

      meaning the concatenation of every chunk must be continuous.

    - A metadata zip file is indexed by the same interval as the
      chunks and, when it is the lowest chunk of metadata stored, is not
      assured to contain every block's metadata of the chunk.

    {1 Files format}

    The cemented block store is composed of the following files:

    - file : /<i_j>, a chunk of blocks from level i to level j. The
      format of this file is:

    | <n> × <offset> | <n> × <block> |

    where n is ( j- i + 1), <offset> is 4 bytes integer representing
    the absolute offset of the k-th (with 0 <= k <= n) block in the
    file and with <block>, a {Block_repr.t} value encoded using
    {Block_repr.encoding} (thus prefixed by the its size).

    - dir : /<cemented_block_index_level>, the Hash -> Level key/value
      index ;

    - dir : /<cemented_block_index_hash>, the Level -> Hash key/value
      index.

    - dir : /metadata, the directory containing chunks of compressed
      metadata (present if relevent).

    - files : /metadata/<i_j>.zip, the compressed metadata: where
      every chunk of block's metadata is indexed by their level encoded
      as string (present if relevent).
*)

(** The type of the cemented block store *)
type t

(** The type for cemented block chunks file description *)
type cemented_blocks_file = {
  start_level : int32;
  end_level : int32;
  filename : string;
}

(** [init ~cemented_blocks_dir] create or load an existing cemented
    block store at path [cemented_blocks_dir]. [cemented_blocks_dir]
    will be created if it does not exists. *)
val init : cemented_blocks_dir:string -> t tzresult Lwt.t

(** [close cemented_store] closes the [cemented_store] opened files:
   its indexes. *)
val close : t -> unit

(** [cemented_blocks_dir cemented_store] returns the path of the
    [cemented_store] directory. *)
val cemented_blocks_dir : t -> string

(** [cemented_blocks_files cemented_store] returns the {b current}
    list of cemented blocks chunks files. *)
val cemented_blocks_files : t -> cemented_blocks_file array

(** [load_table ~cemented_blocks_dir] read the [cemented_blocks_dir]
    directory and instanciate the cemented blocks chunks files. *)
val load_table :
  cemented_blocks_dir:string -> cemented_blocks_file array tzresult Lwt.t

(** [find_block_file cemented_store block_level] lookup the
    [cemented_store] to find the cemented block chunk file that include
    the block at level [block_level]. *)
val find_block_file : t -> int32 -> cemented_blocks_file option

(** [is_cemented cemented_store block_hash] check if the [block_hash]
    is stored in the [cemented_store]. *)
val is_cemented : t -> Block_hash.t -> bool

(** [get_cemented_block_level cemented_store block_hash] returns the
    level of the [block_hash] if present in [cemented_store]. Returns
    None otherwise. *)
val get_cemented_block_level : t -> Block_hash.t -> int32 option

(** [get_cemented_block_hash cemented_store block_level] returns the
    hash of the block at [block_level] if present in
    [cemented_store]. Returns None otherwise. *)
val get_cemented_block_hash : t -> int32 -> Block_hash.t option

(** [get_cemented_block_hash cemented_store block_level] returns the
    hash of the block at [block_level] if present in
    [cemented_store]. Returns None otherwise. *)
val read_block_metadata : t -> int32 -> Block_repr.metadata option

(** [cement_blocks_metadata cemented_store chunk] compress and store
    the metadata of blocks present in [chunk] present. If no blocks of
    the given [chunk] contains metadata, nothing is done otherwise, for
    every block containing metadata, an entry is written in the .zip
    metadata file.

    Hypothesis: the blocks containing metadata are contiguous and if
    at least a block has metadata then the last block of [chunk] must
    have metadata. *)
val cement_blocks_metadata : t -> Block_repr.t list -> unit tzresult Lwt.t

(** [get_lowest_cemented_level cemented_store] returns the lowest
    cemented block in [cemented_store] if it exists. *)
val get_lowest_cemented_level : t -> int32 option

(** [get_highest_cemented_level cemented_store] returns the highest
    cemented block in [cemented_store] if it exists. *)
val get_highest_cemented_level : t -> int32 option

(** [get_highest_cemented_by_level cemented_store level] reads the
    cemented block at [level] in [cemented_store] if it exists. *)
val get_cemented_block_by_level :
  t -> read_metadata:bool -> int32 -> Block_repr.block option Lwt.t

(** [get_highest_cemented_by_hash cemented_store hash] reads the
    cemented block of [hash] in [cemented_store] if it exists. *)
val get_cemented_block_by_hash :
  read_metadata:bool -> t -> Block_hash.t -> Block_repr.block option Lwt.t

(** [cemented_blocks cemented_store ~write_metadata chunk] store the
    [chunk] of blocks and write their metadata if the flag
    [write_metadata] is set. Fails if the blocks in [chunk] are not
    contiguous. *)
val cement_blocks :
  t -> write_metadata:bool -> Block_repr.t list -> unit tzresult Lwt.t

(** [trigger_gc cemented_store history_mode] garbage collect metadata
   chunks and/or chunks from [cemented_store] depending on the
   [history_mode]:

    - in {Archive} mode, nothing is done;

    - in {Full <offset>} mode, only [<offset>] chunks of {b metadata}
   are kept;

    - in {Rolling <offset>} mode, only [<offset>] chunks of {b
    metadata and chunks} are kept. {b Important:} when purging chunks
    of blocks, it is necessary to rewrite the index to remove garbage
    collected blocks. Therefore, the higher the offset, the longest the
    GC phase will be. *)
val trigger_gc : t -> History_mode.t -> unit Lwt.t

(** [restore_indexes_consistency ?post_step ?genesis_hash cemented_store history_mode]
    iterate over a partially intialized [cemented_store] that contains
    only chunks rebuilding their indexes while checking the consistency
    : (hashes, predecessors, levels).  The hash is not checked for
    [genesis_hash] and [post_step] is called after each chunk treated.
    This is used for snapshots import.
 *)
val restore_indexes_consistency :
  ?post_step:(unit -> unit Lwt.t) ->
  ?genesis_hash:Block_hash.t ->
  t ->
  unit tzresult Lwt.t
