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

open Error_monad

let () =
  register_error_kind
    `Temporary
    ~id:"unix_error"
    ~title:"Unix error"
    ~description:"An unhandled unix exception"
    ~pp:Format.pp_print_string
    Data_encoding.(obj1 (req "msg" string))
    (function
      | Exn (Unix.Unix_error (err, fn, _)) ->
          Some ("Unix error in " ^ fn ^ ": " ^ Unix.error_message err)
      | _ ->
          None)
    (fun msg -> Exn (Failure msg))

let default_net_timeout = ref (Ptime.Span.of_int_s 8)

let read_bytes ?(timeout = !default_net_timeout) ?file_offset ?(pos = 0) ?len
    fd buf =
  let buflen = Bytes.length buf in
  let len = match len with None -> buflen - pos | Some l -> l in
  if pos < 0 || pos + len > buflen then invalid_arg "read_bytes" ;
  let rec inner nb_read pos len =
    if len = 0 then Lwt.return_unit
    else
      let reader =
        match file_offset with
        | None ->
            Lwt_unix.read
        | Some fo ->
            Lwt_unix.pread ~file_offset:(fo + nb_read)
      in
      Lwt_unix.with_timeout (Ptime.Span.to_float_s timeout) (fun () ->
          reader fd buf pos len)
      >>= function
      | 0 ->
          Lwt.fail End_of_file
          (* other endpoint cleanly closed its connection *)
      | nb_read' ->
          inner (nb_read + nb_read') (pos + nb_read') (len - nb_read')
  in
  inner 0 pos len

let write_bytes ?file_offset ?(pos = 0) ?len descr buf =
  let buflen = Bytes.length buf in
  let len = match len with None -> buflen - pos | Some l -> l in
  if pos < 0 || pos + len > buflen then invalid_arg "write_bytes" ;
  let rec inner nb_written pos len =
    if len = 0 then Lwt.return_unit
    else
      let writer =
        match file_offset with
        | None ->
            Lwt_unix.write
        | Some fo ->
            Lwt_unix.pwrite ~file_offset:(fo + nb_written)
      in
      writer descr buf pos len
      >>= function
      | 0 ->
          Lwt.fail End_of_file
          (* other endpoint cleanly closed its connection *)
      | nb_written' ->
          inner
            (nb_written + nb_written')
            (pos + nb_written')
            (len - nb_written')
  in
  inner 0 pos len

let write_string ?(pos = 0) ?len descr buf =
  let len = match len with None -> String.length buf - pos | Some l -> l in
  let rec inner pos len =
    if len = 0 then Lwt.return_unit
    else
      Lwt_unix.write_string descr buf pos len
      >>= function
      | 0 ->
          Lwt.fail End_of_file
          (* other endpoint cleanly closed its connection *)
      | nb_written ->
          inner (pos + nb_written) (len - nb_written)
  in
  inner pos len

let remove_dir dir =
  let rec remove dir =
    let files = Lwt_unix.files_of_directory dir in
    Lwt_stream.iter_s
      (fun file ->
        if file = "." || file = ".." then Lwt.return_unit
        else
          let file = Filename.concat dir file in
          if Sys.is_directory file then remove file else Lwt_unix.unlink file)
      files
    >>= fun () -> Lwt_unix.rmdir dir
  in
  if Sys.file_exists dir && Sys.is_directory dir then remove dir
  else Lwt.return_unit

let rec create_dir ?(perm = 0o755) dir =
  Lwt_unix.file_exists dir
  >>= function
  | false ->
      create_dir (Filename.dirname dir)
      >>= fun () ->
      Lwt.catch
        (fun () -> Lwt_unix.mkdir dir perm)
        (function
          | Unix.Unix_error (Unix.EEXIST, _, _) ->
              (* This is the case where the directory has been created
                 by another Lwt.t, after the call to Lwt_unix.file_exists. *)
              Lwt.return_unit
          | e ->
              Lwt.fail e)
  | true -> (
      Lwt_unix.stat dir
      >>= function
      | {st_kind = S_DIR; _} ->
          Lwt.return_unit
      | _ ->
          Stdlib.failwith "Not a directory" )

let safe_close fd =
  Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)

let create_file ?(perm = 0o644) name content =
  Lwt_unix.openfile name Unix.[O_TRUNC; O_CREAT; O_WRONLY] perm
  >>= fun fd ->
  Lwt.finalize
    (fun () -> write_string fd ~pos:0 ~len:(String.length content) content)
    (fun () -> safe_close fd)

let read_file fn = Lwt_io.with_file fn ~mode:Input (fun ch -> Lwt_io.read ch)

let copy_file ~src ~dst =
  Lwt_io.with_file ~mode:Output dst (fun dst_ch ->
      Lwt_io.with_file src ~mode:Input (fun src_ch ->
          let rec loop () =
            Lwt_io.read ~count:4096 src_ch
            >>= function
            | "" ->
                Lwt.return_unit
            | buf ->
                Lwt_io.write dst_ch buf >>= fun () -> loop ()
          in
          loop ()))

let copy_dir ?(perm = 0o755) src dst =
  create_dir ~perm dst
  >>= fun () ->
  let rec copy_dir dir dst_dir =
    let files = Lwt_unix.files_of_directory dir in
    Lwt_stream.iter_p
      (fun file ->
        if file = "." || file = ".." then Lwt.return_unit
        else
          let file = Filename.concat dir file in
          let basename = Filename.basename file in
          if Sys.is_directory file then
            let new_dir = Filename.concat dst_dir basename in
            create_dir ~perm new_dir >>= fun () -> copy_dir file new_dir
          else copy_file ~src:file ~dst:(Filename.concat dst_dir basename))
      files
  in
  if Sys.file_exists src && Sys.is_directory src then copy_dir src dst
  else Lwt.return_unit

let safe_close fd =
  Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)

let of_sockaddr = function
  | Unix.ADDR_UNIX _ ->
      None
  | Unix.ADDR_INET (addr, port) -> (
    match Ipaddr_unix.of_inet_addr addr with
    | V4 addr ->
        Some (Ipaddr.v6_of_v4 addr, port)
    | V6 addr ->
        Some (addr, port) )

let getaddrinfo ~passive ~node ~service =
  let open Lwt_unix in
  getaddrinfo
    node
    service
    (AI_SOCKTYPE SOCK_STREAM :: (if passive then [AI_PASSIVE] else []))
  >>= fun addr ->
  let points =
    TzList.filter_map (fun {ai_addr; _} -> of_sockaddr ai_addr) addr
  in
  Lwt.return points

let getpass () =
  let open Unix in
  (* Turn echoing off and fail if we can't. *)
  let tio = tcgetattr stdin in
  let old_echo = tio.c_echo in
  let old_echonl = tio.c_echonl in
  tio.c_echo <- false ;
  tio.c_echonl <- true ;
  tcsetattr stdin TCSAFLUSH tio ;
  (* Read the passwd. *)
  let passwd = read_line () in
  (* Restore terminal. *)
  tio.c_echo <- old_echo ;
  tio.c_echonl <- old_echonl ;
  tcsetattr stdin TCSAFLUSH tio ;
  passwd

module Json = struct
  let to_root = function
    | `O ctns ->
        `O ctns
    | `A ctns ->
        `A ctns
    | `Null ->
        `O []
    | oth ->
        `A [oth]

  let write_file file json =
    let json = to_root json in
    protect (fun () ->
        Lwt_io.with_file ~mode:Output file (fun chan ->
            let str = Data_encoding.Json.to_string ~minify:false json in
            Lwt_io.write chan str >|= ok))

  let read_file file =
    protect (fun () ->
        Lwt_io.with_file ~mode:Input file (fun chan ->
            Lwt_io.read chan
            >>= fun str ->
            return (Ezjsonm.from_string str :> Data_encoding.json)))
end

let with_tempdir name f =
  let base_dir = Filename.temp_file name "" in
  Lwt_unix.unlink base_dir
  >>= fun () ->
  Lwt_unix.mkdir base_dir 0o700
  >>= fun () ->
  Lwt.finalize (fun () -> f base_dir) (fun () -> remove_dir base_dir)

module Socket = struct
  type addr =
    | Unix of string
    | Tcp of string * string * Unix.getaddrinfo_option list

  let handle_literal_ipv6 host =
    (* To strip '[' and ']' when a literal IPv6 is provided *)
    match Ipaddr.of_string host with
    | Error (`Msg _) ->
        host
    | Ok ipaddr ->
        Ipaddr.to_string ipaddr

  let connect ?(timeout = !default_net_timeout) = function
    | Unix path ->
        let addr = Lwt_unix.ADDR_UNIX path in
        let sock = Lwt_unix.socket PF_UNIX SOCK_STREAM 0 in
        Lwt_unix.connect sock addr >>= fun () -> return sock
    | Tcp (host, service, opts) -> (
        let host = handle_literal_ipv6 host in
        Lwt_unix.getaddrinfo host service opts
        >>= function
        | [] ->
            failwith "could not resolve host '%s'" host
        | addrs ->
            let rec try_connect acc = function
              | [] ->
                  Lwt.return
                    (Error
                       ( failure "could not connect to '%s'" host
                       :: List.rev acc ))
              | {Unix.ai_family; ai_socktype; ai_protocol; ai_addr; _} :: addrs
                -> (
                  let sock =
                    Lwt_unix.socket ai_family ai_socktype ai_protocol
                  in
                  protect
                    ~on_error:(fun e ->
                      Lwt_unix.close sock >>= fun () -> Lwt.return_error e)
                    (fun () ->
                      Lwt_unix.with_timeout
                        (Ptime.Span.to_float_s timeout)
                        (fun () ->
                          Lwt_unix.connect sock ai_addr
                          >>= fun () -> return sock))
                  >>= function
                  | Ok sock ->
                      return sock
                  | Error e ->
                      try_connect (e @ acc) addrs )
            in
            try_connect [] addrs )

  let with_connection ?timeout addr f =
    connect ?timeout addr
    >>=? fun conn ->
    protect
      (fun () -> f conn >>=? fun a -> safe_close conn >>= fun () -> return a)
      ~on_error:(fun e -> safe_close conn >>= fun () -> Lwt.return (Error e))

  let bind ?(backlog = 10) = function
    | Unix path ->
        let addr = Lwt_unix.ADDR_UNIX path in
        let sock = Lwt_unix.socket PF_UNIX SOCK_STREAM 0 in
        Lwt_unix.bind sock addr
        >>= fun () ->
        Lwt_unix.listen sock backlog ;
        return [sock]
    | Tcp (host, service, opts) -> (
        Lwt_unix.getaddrinfo
          (handle_literal_ipv6 host)
          service
          (AI_PASSIVE :: opts)
        >>= function
        | [] ->
            failwith "could not resolve host '%s'" host
        | addrs ->
            let do_bind {Unix.ai_family; ai_socktype; ai_protocol; ai_addr; _}
                =
              let sock = Lwt_unix.socket ai_family ai_socktype ai_protocol in
              Lwt_unix.setsockopt sock SO_REUSEADDR true ;
              Lwt_unix.bind sock ai_addr
              >>= fun () ->
              Lwt_unix.listen sock backlog ;
              return sock
            in
            map_s do_bind addrs )

  type error += Encoding_error | Decoding_error

  let () =
    register_error_kind
      `Permanent
      ~id:"signer.encoding_error"
      ~title:"Encoding_error"
      ~description:"Error while encoding a remote signer message"
      ~pp:(fun ppf () ->
        Format.fprintf ppf "Could not encode a remote signer message")
      Data_encoding.empty
      (function Encoding_error -> Some () | _ -> None)
      (fun () -> Encoding_error) ;
    register_error_kind
      `Permanent
      ~id:"signer.decoding_error"
      ~title:"Decoding_error"
      ~description:"Error while decoding a remote signer message"
      ~pp:(fun ppf () ->
        Format.fprintf ppf "Could not decode a remote signer message")
      Data_encoding.empty
      (function Decoding_error -> Some () | _ -> None)
      (fun () -> Decoding_error)

  let message_len_size = 2

  let send fd encoding message =
    let encoded_message_len = Data_encoding.Binary.length encoding message in
    fail_unless
      (encoded_message_len < 1 lsl (message_len_size * 8))
      Encoding_error
    >>=? fun () ->
    (* len is the length of int16 plus the length of the message we want to send *)
    let len = message_len_size + encoded_message_len in
    let buf = Bytes.create len in
    match
      Data_encoding.Binary.write_opt
        encoding
        message
        buf
        message_len_size
        encoded_message_len
    with
    | None ->
        fail Encoding_error
    | Some last ->
        fail_unless (last = len) Encoding_error
        >>=? fun () ->
        (* we set the beginning of the buf with the length of what is next *)
        TzEndian.set_int16 buf 0 encoded_message_len ;
        protect (fun () -> write_bytes fd buf >|= ok)

  let recv ?timeout fd encoding =
    let header_buf = Bytes.create message_len_size in
    protect (fun () ->
        read_bytes ?timeout ~len:message_len_size fd header_buf >|= ok)
    >>=? fun () ->
    let len = TzEndian.get_uint16 header_buf 0 in
    let buf = Bytes.create len in
    protect (fun () -> read_bytes ?timeout ~len fd buf >|= ok)
    >>=? fun () ->
    match Data_encoding.Binary.read_opt encoding buf 0 len with
    | None ->
        fail Decoding_error
    | Some (read_len, message) ->
        if read_len <> len then fail Decoding_error else return message
end

let rec retry ?(log = fun _ -> Lwt.return_unit) ?(n = 5) ?(sleep = 1.) f =
  f ()
  >>= function
  | Ok r ->
      Lwt.return_ok r
  | Error error as x ->
      if n > 0 then
        log error
        >>= fun () ->
        Lwt_unix.sleep sleep >>= fun () -> retry ~log ~n:(n - 1) ~sleep f
      else Lwt.return x

let display_progress ?(every = 1) ?(out = Lwt_unix.stdout) ~pp_print_step f =
  (* pp_print_step must only write on a single-line with no carriage return*)
  Lwt_unix.isatty out
  >>= fun is_a_tty ->
  if not is_a_tty then f (fun () -> Lwt.return_unit)
  else
    let clear_line fmt = Format.fprintf fmt "\027[2K\r" in
    let (stream, notifier) = Lwt_stream.create () in
    let wrapped_notifier () = notifier (Some ()) ; Lwt_unix.yield () in
    let thread =
      Lwt.finalize
        (fun () -> f wrapped_notifier)
        (fun () -> notifier None ; Lwt.return_unit)
    in
    let oc = Unix.out_channel_of_descr (Lwt_unix.unix_file_descr out) in
    let fmt = Format.formatter_of_out_channel oc in
    let cpt = ref 0 in
    let pp_cpt = ref 0 in
    let rec loop () = Lwt_unix.sleep 1. >>= fun () -> incr pp_cpt ; loop () in
    let loop = loop () in
    let dots () = String.init (!pp_cpt mod 4) (fun _ -> '.') in
    let pp () =
      clear_line fmt ;
      Format.fprintf fmt "%a%s%!" pp_print_step !cpt (dots ())
    in
    let pp_done () =
      clear_line fmt ;
      Format.fprintf fmt "%a Done@\n%!" pp_print_step !cpt
    in
    pp () ;
    incr cpt ;
    let printer =
      Lwt_stream.iter_s
        (fun () ->
          ( if !cpt mod every = 0 then (pp () ; Lwt.return_unit)
          else Lwt.return_unit )
          >>= fun () -> incr cpt ; Lwt.return_unit)
        stream
    in
    thread
    >>= fun e ->
    Lwt.cancel loop ;
    printer >>= fun () -> decr cpt ; pp_done () ; Lwt.return e
