open Util
open Verdict

let parse_verdicts_string s =
  let lexbuf = Lexing.from_string ~with_positions:false s in
  parse lexbuf

let input1 = {|@0. (time point 0): (1)
@0. (time point 1): (a)
@21. (time point 12): ("b c") (123)
|}

let verdicts1 =
  [{tp = 0; ts = 0.; fp = 0.; fn = 0.; rel = Rel.of_list [["1"]]};
   {tp = 1; ts = 0.; fp = 0.; fn = 0.; rel = Rel.of_list [["a"]]};
   {tp = 12; ts = 21.; fp = 0.; fn = 0.;
    rel = Rel.of_list [["b c"]; ["123"]]}]

let input2 = {|@0. (time point 0): true
@0. (time point 1): false
# FP <= 3.45e-06, FN <= 0.00e+00
@21. (time point 12): true
# FP <= 6.78e-09, FN <= 3.21e-10
|}

let verdicts2 =
  [{tp = 0; ts = 0.; fp = 0.; fn = 0.; rel = Rel.of_list [[]]};
   {tp = 1; ts = 0.; fp = 3.45e-6; fn = 0.; rel = Rel.of_list []};
   {tp = 12; ts = 21.; fp = 6.78e-9; fn = 3.21e-10; rel = Rel.of_list [[]]}]

let print_verdict oc v =
  Printf.fprintf oc "{tp = %d; ts = %f; fp = %e; fn = %e; rel = "
    v.tp v.ts v.fp v.fn;
  print_rel oc v.rel;
  Printf.fprintf oc "}"

let print_verdicts = print_list "[" "; " "]" print_verdict

let test_verdict_parser name s expected =
  let verdicts = parse_verdicts_string s in
  if verdicts <> expected then
    begin
      Printf.eprintf "Test failed: test_verdict_parser %s\n" name;
      Printf.eprintf "  expected: ";
      print_verdicts Stdlib.stderr expected;
      Printf.eprintf "\n  actual: ";
      print_verdicts Stdlib.stderr verdicts;
      Printf.eprintf "\n";
      exit 1
    end

let () =
  test_verdict_parser "1" input1 verdicts1;
  test_verdict_parser "2" input2 verdicts2


let ref_verdicts = parse_verdicts_string {|@0. (time point 0): true
@1. (time point 1): true|}

let sampled_verdicts = List.map parse_verdicts_string
  [{|@0. (time point 0): true
@1. (time point 1): true
# FP <= 1.2000e-01, FN <= 0.0000e+00
@2. (time point 2): false
# FP <= 0.0000e+00, FN <= 3.4000e-01
|};
  {|@0. (time point 0): true
@1. (time point 1): false
# FP <= 1.1000e-01, FN <= 0.0000e+00
@2. (time point 2): true
# FP <= 0.0000e+00, FN <= 3.5000e-01
|}]

let () =
  let n_tps = List.length (List.hd sampled_verdicts) in
  let n_samples = List.length sampled_verdicts in
  let samples = create_samples n_tps n_samples in
  List.iteri (fun i vl -> ignore (diff_with_samples samples i ref_verdicts vl))
    sampled_verdicts;
  assert (samples.n_tps = n_tps);
  assert (samples.n_samples = n_samples);
  assert (samples.fp_counts = [|0;0;1|]);
  assert (samples.fn_counts = [|0;1;0|]);
  assert (samples.diff_counts = [|0;1;1|]);
  let summary = summarize_samples 0.67 samples in
  assert (fst summary.fp_prob > 0.1 && snd summary.fp_prob < 0.9);
  assert (fst summary.fn_prob > 0.1 && snd summary.fn_prob < 0.9);
  assert (summary.max_fp_bound = 0.12);
  assert (summary.max_fn_bound = 0.35);
  assert (summary.max_diff_bound = 0.35)
