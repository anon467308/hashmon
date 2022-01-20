let ref_file = ref ""
let sample_dir = ref ""
let tps = ref ~-1
let verbose = ref false
let wilson_z = 1.96 (* corresponds to 95% confidence *)

let args = Arg.align
  [("-ref", Arg.Set_string ref_file, "\tFile with reference output");
   ("-samples", Arg.Set_string sample_dir, "\tDirectory with output samples");
   ("-tps", Arg.Set_int tps, "\tTotal number of time-points");
   ("-v", Arg.Set verbose, "\tVerbose output")]

let usage = "diff [-v] -tps <time-points> -ref <reference> -samples <sample_dir>"

let print_verbose samples summary =
  let open Verdict in
  Printf.printf "Samples: %d\n" samples.n_samples;
  let fpl, fpu = summary.fp_prob in
  let fnl, fnu = summary.fn_prob in
  let dl, du = summary.diff_prob in
  Printf.printf "FP probability:   %.4e .. %.4e\n" fpl fpu;
  Printf.printf "FN probability:   %.4e .. %.4e\n" fnl fnu;
  Printf.printf "Diff probability: %.4e .. %.4e\n" dl du;
  Printf.printf "Max FP bound:     %.4e\n" summary.max_fp_bound;
  Printf.printf "Max FN bound:     %.4e\n" summary.max_fn_bound;
  Printf.printf "Max diff bound:   %.4e\n" summary.max_diff_bound

let print_concise _ summary =
  let open Verdict in
  let fpl, fpu = summary.fp_prob in
  let fnl, fnu = summary.fn_prob in
  let dl, du = summary.diff_prob in
  Printf.printf "%.4f;%.4f;%.4f;%.4f;%.4f;%.4f;%.4f;%.4f;%.4f\n"
    fpl fpu fnl fnu dl du
    summary.max_fp_bound summary.max_fn_bound summary.max_diff_bound

let () =
  Arg.parse args (fun _ -> failwith "unexpected argument") usage;
  if !ref_file = "" then failwith "argument -ref is mandatory";
  if !sample_dir = "" then failwith "argument -samples is mandatory";
  if !tps < 0 then failwith "argument -tps is mandatory and must be non-negative";
  let ref_verdicts = Verdict.parse_file !ref_file in
  let sample_files = Util.list_dir !sample_dir in
  let samples = Verdict.create_samples !tps (List.length sample_files) in
  List.iteri
    (fun i s ->
      let sample = Verdict.parse_file (!sample_dir ^ "/" ^ s) in
      ignore (Verdict.diff_with_samples samples i ref_verdicts sample))
    sample_files;
  let summary = Verdict.summarize_samples wilson_z samples in
  if !verbose then
    print_verbose samples summary
  else
    print_concise samples summary
