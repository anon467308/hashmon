open Util

type verdict = {
  tp: int;
  ts: float;
  fp: float;
  fn: float;
  rel: Rel.t;
}

exception Parse_error

val parse: Lexing.lexbuf -> verdict list
val parse_file: string -> verdict list

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

val diff: ?callback:(verdict -> verdict -> Rel.t -> Rel.t -> unit) ->
  ?total_tps:int -> verdict list -> verdict list -> diff_stats

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

type sample_summary = {
  fp_prob: float * float;
  fn_prob: float * float;
  diff_prob: float * float;
  max_fp_bound: float;
  max_fn_bound: float;
  max_diff_bound: float;
}

val create_samples: int -> int -> samples
val diff_with_samples: samples -> int -> verdict list -> verdict list ->
  diff_stats
val summarize_samples: float -> samples -> sample_summary
