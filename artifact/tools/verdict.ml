open Verdict_lexer
open Util

type verdict = {
  tp: int;
  ts: float;
  fp: float;
  fn: float;
  rel: Rel.t;
}

exception Parse_error

let sort_verdicts = List.stable_sort (fun v1 v2 -> Int.compare v1.tp v2.tp)

let parse lexbuf =
  let rec parse_toplevel vl =
    match lex_toplevel lexbuf with
    | TIMEPOINT (tp, ts) ->
        let rel = parse_rel() in
        parse_after_verdict vl {tp; ts; fp = 0.; fn = 0.; rel}
    | ERRBOUND _ -> raise Parse_error
    | SKIP -> parse_toplevel vl
    | EOF -> vl
  and parse_after_verdict vl v =
    match lex_toplevel lexbuf with
    | TIMEPOINT (tp, ts) ->
        let rel = parse_rel() in
        parse_after_verdict (v::vl) {tp; ts; fp = 0.; fn = 0.; rel}
    | ERRBOUND (fp, fn) ->
        let v' = {v with fp; fn} in
        parse_toplevel (v'::vl)
    | SKIP -> parse_toplevel (v::vl)
    | EOF -> v::vl
  and parse_rel () =
    match lex_data lexbuf with
    | STRING s -> parse_special_rel s
    | END -> Rel.empty
    | LPAREN -> parse_tuple Rel.empty
    | _ -> raise Parse_error
  and parse_rel_cont rel =
    match lex_data lexbuf with
    | LPAREN -> parse_tuple rel
    | END -> rel
    | _ -> raise Parse_error
  and parse_special_rel s =
    let rel =
      match s with
      | "true" -> Rel.singleton []
      | "false" -> Rel.empty
      | _ -> raise Parse_error
    in
    match lex_data lexbuf with
    | END -> rel
    | _ -> raise Parse_error
  and parse_tuple rel =
    match lex_data lexbuf with
    | RPAREN -> parse_rel_cont (Rel.add [] rel)
    | STRING s -> parse_tuple_cont rel [s]
    | _ -> raise Parse_error
  and parse_tuple_cont rel tup =
    match lex_data lexbuf with
    | RPAREN -> parse_rel_cont (Rel.add (List.rev tup) rel)
    | COMMA ->
      (match lex_data lexbuf with
      | STRING s -> parse_tuple_cont rel (s::tup)
      | _ -> raise Parse_error)
    | _ -> raise Parse_error
  in
  sort_verdicts (parse_toplevel [])

let parse_file filename =
  let ic = Stdlib.open_in_bin filename in
  try
    let lexbuf = Lexing.from_channel ~with_positions:false ic in
    let vl = parse lexbuf in
    Stdlib.close_in ic;
    vl
  with exn ->
    Stdlib.close_in_noerr ic;
    raise exn


type diff_stats = {
  total_tps: int;
  true_tps: int;
  fp_tps: int;
  fn_tps: int;
  diff_tps: int;
  true_tuples: int;
  fp_tuples: int;
  fn_tuples: int;
}

let empty_verdict v = {
  tp = v.tp;
  ts = v.ts;
  fp = 0.;
  fn = 0.;
  rel = Rel.empty;
}

let diff ?callback ?total_tps rl vl =
  let total_tps =
    match total_tps with
    | Some x -> x
    | None ->
        (match last rl with
        | None -> 0
        | Some r -> r.tp + 1)
  in
  let true_tps = ref 0 in
  let fp_tps = ref 0 in
  let fn_tps = ref 0 in
  let diff_tps = ref 0 in
  let true_tuples = ref 0 in
  let fp_tuples = ref 0 in
  let fn_tuples = ref 0 in

  let check r v =
    if not (Rel.is_empty r.rel) then
      begin
        incr true_tps 1;
        incr true_tuples (Rel.cardinal r.rel)
      end;
    let fp = Rel.diff v.rel r.rel in
    if not (Rel.is_empty fp) then
      begin
        incr fp_tps 1;
        incr fp_tuples (Rel.cardinal fp)
      end;
    let fn = Rel.diff r.rel v.rel in
    if not (Rel.is_empty fn) then
      begin
        incr fn_tps 1;
        incr fn_tuples (Rel.cardinal fn)
      end;
    if not (Rel.is_empty fp) || not (Rel.is_empty fn) then
      incr diff_tps 1;
    match callback with
    | None -> ()
    | Some f -> f r v fp fn
  in

  let rec go = function
    | [], [] -> ()
    | r::rl, [] -> check r (empty_verdict r); go (rl, [])
    | [], v::vl -> check (empty_verdict v) v; go ([], vl)
    | r::rl, v::vl ->
        if r.tp < v.tp then
          (check r (empty_verdict r); go (rl, v::vl))
        else if r.tp > v.tp then
          (check (empty_verdict v) v; go (r::rl, vl))
        else
          (check r v; go (rl, vl))
  in
  go (rl, vl);
  {
    total_tps;
    true_tps = !true_tps;
    fp_tps = !fp_tps;
    fn_tps = !fn_tps;
    diff_tps = !diff_tps;
    true_tuples = !true_tuples;
    fp_tuples = !fp_tuples;
    fn_tuples = !fn_tuples;
  }


type samples = {
  n_tps: int;
  n_samples: int;
  fp_counts: int array;
  fn_counts: int array;
  diff_counts: int array;
  fp_bounds: float array array;
  fn_bounds: float array array;
  diff_bounds: float array array;
}

let create_samples n_tps n_samples =
  {
    n_tps;
    n_samples;
    fp_counts = Array.make n_tps 0;
    fn_counts = Array.make n_tps 0;
    diff_counts = Array.make n_tps 0;
    fp_bounds = Array.make_matrix n_tps n_samples 0.;
    fn_bounds = Array.make_matrix n_tps n_samples 0.;
    diff_bounds = Array.make_matrix n_tps n_samples 0.;
  }

let diff_with_samples samples i rl vl =
  let callback r v fp fn =
    if not (Rel.is_empty fp) then
      samples.fp_counts.(r.tp) <- samples.fp_counts.(r.tp) + 1;
    if not (Rel.is_empty fn) then
      samples.fn_counts.(r.tp) <- samples.fn_counts.(r.tp) + 1;
    if not (Rel.is_empty fp) || not (Rel.is_empty fn) then
      samples.diff_counts.(r.tp) <- samples.diff_counts.(r.tp) + 1;
    samples.fp_bounds.(r.tp).(i) <- v.fp;
    samples.fn_bounds.(r.tp).(i) <- v.fn;
    samples.diff_bounds.(r.tp).(i) <- min 1. (v.fp +. v.fn)
  in
  diff ~callback ~total_tps:samples.n_tps rl vl

type sample_summary = {
  fp_prob: float * float;
  fn_prob: float * float;
  diff_prob: float * float;
  max_fp_bound: float;
  max_fn_bound: float;
  max_diff_bound: float;
}

let estimate_probs z counts n_samples =
  let n_tps = Array.length counts in
  Array.init n_tps (fun i -> wilson_interval z counts.(i) n_samples)

let summarize_samples z samples =
  let fp_probs = estimate_probs z samples.fp_counts samples.n_samples in
  let fp_prob = fp_probs.(array_arg_max snd fp_probs) in
  let fn_probs = estimate_probs z samples.fn_counts samples.n_samples in
  let fn_prob = fn_probs.(array_arg_max snd fn_probs) in
  let diff_probs = estimate_probs z samples.diff_counts samples.n_samples in
  let diff_prob = diff_probs.(array_arg_max snd diff_probs) in
  let max_fp_bound = matrix_max samples.fp_bounds in
  let max_fn_bound = matrix_max samples.fn_bounds in
  let max_diff_bound = matrix_max samples.diff_bounds in
  { fp_prob; fn_prob; diff_prob; max_fp_bound; max_fn_bound; max_diff_bound }
