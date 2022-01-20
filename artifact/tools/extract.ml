let () =
  let filename = Sys.argv.(1) in
  let verdicts = Verdict.parse_file filename in
  let max_fp = ref 0.0 in
  let max_fn = ref 0.0 in
  List.iter (fun v ->
    max_fp := max !max_fp v.Verdict.fp;
    max_fn := max !max_fn v.Verdict.fn) verdicts;
  Printf.printf "%.4e;%.4e\n" !max_fp !max_fn
