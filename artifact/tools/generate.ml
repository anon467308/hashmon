let random_string n =
  let sample_char _ =
    let x = Random.int (10 + 26 + 26) in
    if x < 10 then
      Char.chr (0x30 + x)
    else if x < 10 + 26 then
      Char.chr (0x41 + (x - 10))
    else
      Char.chr (0x61 + (x - 10 - 26))
  in
  String.init n sample_char

let random_tuple spec strlen =
  let i = Random.int (Array.length spec) in
  let (name, arity) = spec.(i) in
  let args = Array.init arity (fun _ -> random_string strlen) in
  (name, args)

let insert_into_db db (name, args) =
  try
    let l = Hashtbl.find db name in
    Hashtbl.replace db name (args::l)
  with Not_found ->
    Hashtbl.add db name [args]

let random_db spec strlen tuples =
  let db = Hashtbl.create (4 * Array.length spec) in
  for _ = 1 to tuples do
    let tuple = random_tuple spec strlen in
    insert_into_db db tuple
  done;
  db

let print_monpoly_tuple os a =
  let n = Array.length a in
  output_char os '(';
  if n > 0 then begin
    output_string os a.(0);
    for i = 1 to n - 1 do
      output_char os ',';
      output_string os a.(i)
    done
  end;
  output_char os ')'

let print_monpoly_tp os ts db =
  Printf.fprintf os "@%d " ts;
  Hashtbl.iter (fun name tuples ->
    output_string os name;
    List.iter (print_monpoly_tuple os) tuples) db;
  output_string os ";\n"

let print_csv_tuple os a =
  let n = Array.length a in
  for i = 0 to n - 1 do
    output_char os ',';
    output_string os a.(i)
  done

let print_csv_tp os ts db =
  Hashtbl.iter (fun name tuples ->
    output_string os name;
    List.iter (print_csv_tuple os) tuples) db;
  Printf.fprintf os ",%d\n" ts

let strlen = ref 4
let index_rate = ref 1
let event_rate = ref 1
let tss = ref 10

let gen_trace monpoly_os dejavu_os spec =
  for ts = 0 to !tss - 1 do
    for _ = 1 to !index_rate do
      let db = random_db spec !strlen !event_rate in
      print_monpoly_tp monpoly_os ts db;
      (match dejavu_os with
      | Some os -> print_csv_tp os ts db
      | None -> ())
    done
  done

let spec = ref [|("P", 1)|]
let dejavu_file = ref None

let parse_spec l =
  let rec go = function
    | [] -> []
    | [_] -> failwith "signature specification must have an even length"
    | name::arity::l -> (name, int_of_string arity) :: go l
  in
  spec := Array.of_list (go l)

let set_dejavu_file fn = dejavu_file := Some fn

let args = Arg.align
  [("-seed", Arg.Int Random.init, "\tRNG seed");
   ("-strlen", Arg.Set_int strlen, "\tLength of string arguments");
   ("-er", Arg.Set_int event_rate,
      "\tEvent rate (number of tuples per time-point)");
   ("-ir", Arg.Set_int index_rate,
      "\tIndex rate (number of time-points with same time-stamp)");
   ("-tss", Arg.Set_int tss, "\tNumber of distinct time-stamps");
   ("-dejavu", Arg.String set_dejavu_file, "\tName of the DejaVu output file");
   ("--", Arg.Rest_all parse_spec, "\tSignature: name1 arity1 name2 arity2 ...")]

let usage = "generate [<options...>] [-- <signature...>]"

let () =
  Arg.parse args (fun _ -> failwith "unexpected argument") usage;
  let dejavu_os = Option.map (fun fn ->
      if !event_rate <> 1 then failwith "event rate must be 1 for DejaVu";
      open_out fn) !dejavu_file
  in
  gen_trace stdout dejavu_os !spec
