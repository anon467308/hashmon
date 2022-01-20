let rec last = function
  | [] -> None
  | [x] -> Some x
  | _::l -> last l

let string_all p s =
  let n = String.length s in
  let rec go i =
    if i < n then p s.[i] && go (i+1) else true
  in
  go 0

let incr c x = (c := !c + x)

let print_list delim_l sep delim_r pf oc l =
  let rec cont = function
    | [] -> ()
    | x::l' -> (Stdlib.output_string oc sep; pf oc x; cont l')
  in
  Stdlib.output_string oc delim_l;
  (match l with
  | [] -> ()
  | x::l' -> (pf oc x; cont l'));
  Stdlib.output_string oc delim_r

module String_map = Map.Make(
  struct
    type t = string
    let compare = String.compare
  end
)


type tuple = string list

module Rel = Set.Make(
  struct
    type t = tuple
    let compare = List.compare String.compare
  end
)

type db = Rel.t String_map.t

let simple_string_char c =
  (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  || c == '_' || c == '[' || c == ']' || c == '/'
  || c == ':' || c == '-' || c == '.' || c == '!'

let simple_string s = string_all simple_string_char s

let print_cst oc s =
  if simple_string s then
    Stdlib.output_string oc s
  else
    begin
      Stdlib.output_char oc '"';
      Stdlib.output_string oc s;
      Stdlib.output_char oc '"'
    end

let print_tuple oc l = print_list "(" "," ")" print_cst oc l

let print_rel oc rel =
  let rec go seq =
    match seq() with
    | Seq.Nil -> ()
    | Seq.Cons (x, seq') -> (print_tuple oc x; go seq')
  in
  go (Rel.to_seq rel)


let array_arg_max f a =
  let acc = ref (~-. Float.infinity) in
  let i = ref (~-1) in
  Array.iteri (fun j x ->
    let y = f x in
    if y > !acc then (acc := y; i := j)) a;
  !i

let matrix_max m =
  let acc = ref (~-. Float.infinity) in
  Array.iter (Array.iter (fun x -> acc := max x !acc)) m;
  !acc

let array_sum a =
  let acc = ref 0. in
  let comp = ref 0. in
  Array.iter
    (fun x ->
      let y = x -. !comp in
      let acc' = !acc +. y in
      comp := (acc' -. !acc) -. y;
      acc := acc'
    ) a;
  !acc

let array_avg a =
  let n = Array.length a in
  if n = 0 then 0. else array_sum a /. float_of_int n

let clamp l u x = if x < l then l else if x > u then u else x

let wilson_interval z s n =
  let s = float_of_int s in
  let n = float_of_int n in
  let f = n -. s in
  let z2 = z *. z in
  let p0 = (s +. z2/.2.) /. (n +. z2) in
  let p1 = z /. (n +. z2) *. Float.sqrt ((s *. f /. n) +. (z2/.4.)) in
  let pl = clamp 0. 1. (p0 -. p1) in
  let pu = clamp 0. 1. (p0 +. p1) in
  pl, pu


let is_hidden s = s <> "" && s.[0] = '.'

let list_dir dirname =
  let handle = Unix.opendir dirname in
  try
    let rec go l =
      try
        let entry = Unix.readdir handle in
        if is_hidden entry then go l else go (entry::l)
      with End_of_file -> List.rev l
    in
    let l = go [] in
    Unix.closedir handle;
    l
  with e ->
    Unix.closedir handle;
    raise e
