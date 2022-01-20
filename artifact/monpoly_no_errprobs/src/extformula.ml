open Relation
open Predicate
open MFOTL
open Tuple

module Sk = Dllist
module Sj = Dllist

type info  = (int * timestamp * relation) Queue.t
type linfo = {
  mutable llast: Neval.cell;
}
type ainfo = {mutable arel: relation option}
type pinfo = {mutable plast: Neval.cell}
type ninfo = {mutable init: bool}
type oainfo = {mutable ores: relation;
         oaauxrels: (timestamp * relation) Mqueue.t}

type agg_info = {op: agg_op; default: cst}

type ozinfo = {mutable oztree: (int, relation) Sliding.stree;
               mutable ozlast: (int * timestamp * relation) Dllist.cell;
               ozauxrels: (int * timestamp * relation) Dllist.dllist}
type oinfo = {mutable otree: (timestamp, relation) Sliding.stree;
              mutable olast: (timestamp * relation) Dllist.cell;
              oauxrels: (timestamp * relation) Dllist.dllist}
type sainfo = {mutable sres: relation;
               mutable sarel2: relation option;
               saauxrels: (timestamp * relation) Mqueue.t}
type sinfo = {mutable srel2: relation option;
              sauxrels: (timestamp * relation) Mqueue.t}
type ezinfo = {mutable ezlastev: Neval.cell;
               mutable eztree: (int, relation) Sliding.stree;
               mutable ezlast: (int * timestamp * relation) Dllist.cell;
               ezauxrels: (int * timestamp * relation) Dllist.dllist}
type einfo = {mutable elastev: Neval.cell;
              mutable etree: (timestamp, relation) Sliding.stree;
              mutable elast: (timestamp * relation) Dllist.cell;
              eauxrels: (timestamp * relation) Dllist.dllist}
type uinfo = {mutable ulast: Neval.cell;
              mutable ufirst: bool;
              mutable ures: relation;
              mutable urel2: relation option;
              raux: (int * timestamp * (int * relation) Sk.dllist) Sj.dllist;
              mutable saux: (int * relation) Sk.dllist;
              uerr1: (int * err_info) Sk.dllist;
              uerr2: (int * timestamp * err_info) Sj.dllist}
type uninfo = {mutable last1: Neval.cell;
               mutable last2: Neval.cell;
               mutable listrel1: (int * timestamp * relation) Dllist.dllist;
               mutable listrel2: (int * timestamp * relation) Dllist.dllist}

type comp_one = relation -> relation
type comp_two = relation -> relation -> relation

type extformula =
  | ERel of relation
  | EPred of predicate * comp_one * info
  | ELet of predicate * comp_one * extformula * extformula * linfo
  | ENeg of extformula
  | EAnd of comp_two * extformula * extformula * ainfo
  | EOr of comp_two * extformula * extformula * ainfo
  | EExists of comp_one * extformula
  | EAggreg of agg_info * Aggreg.aggregator * extformula
  | EAggOnce of agg_info * Aggreg.once_aggregator * extformula
  | EPrev of interval * extformula * pinfo
  | ENext of interval * extformula * ninfo
  | ESinceA of comp_one * comp_two * interval * extformula * extformula * sainfo
  | ESince of comp_one * comp_two * interval * extformula * extformula * sinfo
  | EOnceA of interval * extformula * oainfo
  | EOnceZ of interval * extformula * ozinfo
  | EOnce of interval * extformula * oinfo
  | ENUntil of comp_one * comp_two * bool * interval * extformula * extformula * uninfo
  | EUntil of comp_one * comp_two * interval * extformula * extformula * uinfo
  | EEventuallyZ of interval * extformula * ezinfo
  | EEventually of interval * extformula * einfo


  let rec contains_eventually = function
  | ERel           (rel)                                      -> false
  | EPred          (p, comp, inf)                             -> false
  | ELet           (p, comp, f1, f2, inf)                     -> contains_eventually f1 || contains_eventually f2
  | ENeg           (f1)                                       -> contains_eventually f1
  | EAnd           (c, f1, f2, ainf)                          -> contains_eventually f1 || contains_eventually f2
  | EOr            (c, f1, f2, ainf)                          -> contains_eventually f1 || contains_eventually f2
  | EExists        (c, f1)                                    -> contains_eventually f1
  | EAggreg        (_inf, _comp, f1)                          -> contains_eventually f1
  | EAggOnce       (_inf, _state, f1)                         -> contains_eventually f1
  | EPrev          (dt, f1, pinf)                             -> contains_eventually f1
  | ENext          (dt, f1, ninf)                             -> contains_eventually f1
  | ESinceA        (c1, c2, dt, f1, f2, sainf)                -> contains_eventually f1 || contains_eventually f2
  | ESince         (c1, c2, dt, f1, f2, sinf)                 -> contains_eventually f1 || contains_eventually f2
  | EOnceA         (dt, f1, oainf)                            -> contains_eventually f1
  | EOnceZ         (dt, f1, ozinf)                            -> contains_eventually f1
  | EOnce          (dt, f1, oinf)                             -> contains_eventually f1
  | ENUntil        (c1, c2, _, dt, f1, f2, muninf)            -> contains_eventually f1 || contains_eventually f2
  | EUntil         (c1, c2, dt, f1, f2, muinf)                -> contains_eventually f1 || contains_eventually f2
  | EEventuallyZ   (dt, f1, mezinf)                           -> true
  | EEventually    (dt, f1, meinf)                            -> true

(* 
  Print functions used for debugging
 *)  

let print_bool b =
  if b then
    print_string "true"
  else
    print_string "false"

let print_ainf str ainf =
  print_string str;
  match ainf with
  | None -> print_string "None"
  | Some rel -> Relation.print_rel "" rel

let print_auxel =
  (fun (k,rel) ->
      Printf.printf "(%d->" k;
      Relation.print_rel "" rel;
      print_string ")"
  )
let print_sauxel =
  (fun (tsq,rel) ->
      Printf.printf "(%s," (MFOTL.string_of_ts tsq);
      Relation.print_rel "" rel;
      print_string ")"
  )

let print_rauxel (j,tsj,rrelsj) =
  Printf.printf "(j=%d,tsj=" j;
  MFOTL.print_ts tsj;
  print_string ",r=";
  Misc.print_dllist print_auxel rrelsj;
  print_string "),"


let print_aauxel (q,tsq,rel) =
  Printf.printf "(%d,%s," q (MFOTL.string_of_ts tsq);
  Relation.print_rel "" rel;
  print_string ")"

let print_inf inf =
  Misc.print_queue print_aauxel inf

let print_predinf str inf =
  print_string str;
  print_inf inf;
  print_newline()

let print_linf str inf =
  Printf.printf "%s{llast=%s}\n" str (Neval.string_of_cell inf.llast)

let print_ozinf str inf =
  print_string str;
  if inf.ozlast == Dllist.void then
    print_string "ozlast = None; "
  else
    begin
      let (j,_,_) = Dllist.get_data inf.ozlast in
      Printf.printf "ozlast (index) = %d; " j
    end;
  Misc.print_dllist print_aauxel inf.ozauxrels;
  Sliding.print_stree
    string_of_int
    (Relation.print_rel " ztree = ")
    "; ozinf.ztree = "
    inf.oztree

let print_oinf str inf =
  print_string (str ^ "{");
  if inf.olast == Dllist.void then
    print_string "last = None; "
  else
    begin
      let (ts,_) = Dllist.get_data inf.olast in
      Printf.printf "last (ts) = %s; " (MFOTL.string_of_ts ts)
    end;
  print_string "oauxrels = ";
  Misc.print_dllist print_sauxel inf.oauxrels;
  Sliding.print_stree MFOTL.string_of_ts (Relation.print_rel "") ";\n oinf.tree = " inf.otree;
  print_string "}"


let print_sainf str inf =
  print_string str;
  print_ainf "{srel2 = " inf.sarel2;
  Relation.print_rel "; sres=" inf.sres;
  print_string "; sauxrels=";
  Misc.print_mqueue print_sauxel inf.saauxrels;
  print_string "}"

let print_sinf str inf =
  print_string str;
  print_ainf "{srel2=" inf.srel2  ;
  print_string ", sauxrels=";
  Misc.print_mqueue print_sauxel inf.sauxrels;
  print_string "}"


let print_uinf str inf =
  Printf.printf "%s{first=%b; last=%s; " str inf.ufirst
    (Neval.string_of_cell inf.ulast);
  Relation.print_rel "res=" inf.ures;
  print_string "; raux=";
  Misc.print_dllist print_rauxel inf.raux;
  print_string "; saux=";
  Misc.print_dllist print_auxel inf.saux;
  print_endline "}"

let print_uninf str uninf =
  Printf.printf "%s{last1=%s; last2=%s; " str
    (Neval.string_of_cell uninf.last1) (Neval.string_of_cell uninf.last2);
  print_string "listrel1=";
  Misc.print_dllist print_aauxel uninf.listrel1;
  print_string "; listrel2=";
  Misc.print_dllist print_aauxel uninf.listrel2;
  print_string "}\n"

let print_ezinf str inf =
  Printf.printf "%s{ezlastev=%s; " str (Neval.string_of_cell inf.ezlastev);
  if inf.ezlast == Dllist.void then
    print_string "ezlast = None; "
  else
    begin
      let (_,ts,_) = Dllist.get_data inf.ezlast in
      Printf.printf "elast (ts) = %s; " (MFOTL.string_of_ts ts)
    end;
  print_string "eauxrels=";
  Misc.print_dllist print_aauxel inf.ezauxrels;
  Sliding.print_stree string_of_int (Relation.print_rel "") "; ezinf.eztree = " inf.eztree;
  print_string "}\n"


let print_einf str inf =
  Printf.printf "%s{elastev=%s; " str (Neval.string_of_cell inf.elastev);
  if inf.elast == Dllist.void then
    print_string "elast = None; "
  else
    begin
      let ts = fst (Dllist.get_data inf.elast) in
      Printf.printf "elast (ts) = %s; " (MFOTL.string_of_ts ts)
    end;
  print_string "eauxrels=";
  Misc.print_dllist print_sauxel inf.eauxrels;
  Sliding.print_stree MFOTL.string_of_ts (Relation.print_rel "") "; einf.etree = " inf.etree;
  print_string "}"

let print_einfn str inf =
  print_einf str inf;
  print_newline()

let print_hashtbl pkey pval tbl =
  print_string "[";
  Hashtbl.iter (fun k v ->
    pkey k;
    print_string ",";
    pval v;
    print_string "; ") tbl;
  print_string "]"

let print_agg_inf str inf =
  Printf.printf "%s{op=%s; default=%s}\n" str (string_of_agg_op inf.op)
    (string_of_cst true inf.default)

let print_extf str ff =
  let print_spaces d =
    for i = 1 to d do print_string " " done
  in
  let rec print_f_rec d f =
    print_spaces d;
    (match f with
      | ERel rel ->
        Relation.print_rel "ERel " rel;
        print_newline ()

      | EPred (p,_,inf) ->
        Predicate.print_predicate p;
        print_string ": inf=";
        print_inf inf;
        print_string "\n"

      | ENeg f ->
        print_string "NOT\n";
        print_f_rec (d+1) f;

      | EExists (_,f) ->
        print_string "EXISTS\n";
        print_f_rec (d+1) f;

      | EPrev (intv,f,pinf) ->
        print_string "PREVIOUS";
        MFOTL.print_interval intv;
        print_string ": plast=";
        print_string (Neval.string_of_cell pinf.plast);
        print_string "\n";
        print_f_rec (d+1) f

      | ENext (intv,f,ninf) ->
        print_string "NEXT";
        MFOTL.print_interval intv;
        print_string ": init=";
        print_bool ninf.init;
        print_string "\n";
        print_f_rec (d+1) f

      | EOnceA (intv,f,inf) ->
        print_string "ONCE";
        MFOTL.print_interval intv;
        Relation.print_rel ": rel = " inf.ores;
        print_string "; oaauxrels = ";
        Misc.print_mqueue print_sauxel inf.oaauxrels;
        print_string "\n";
        print_f_rec (d+1) f

      | EOnceZ (intv,f,oinf) ->
        print_string "ONCE";
        MFOTL.print_interval intv;
        print_ozinf ": ozinf=" oinf;
        print_f_rec (d+1) f

      | EOnce (intv,f,oinf) ->
        print_string "ONCE";
        MFOTL.print_interval intv;
        print_oinf ": oinf = " oinf;
        print_string "\n";
        print_f_rec (d+1) f

      | EEventuallyZ (intv,f,einf) ->
        print_string "EVENTUALLY";
        MFOTL.print_interval intv;
        print_ezinf ": ezinf=" einf;
        print_f_rec (d+1) f

      | EEventually (intv,f,einf) ->
        print_string "EVENTUALLY";
        MFOTL.print_interval intv;
        print_einf ": einf=" einf;
        print_string "\n";
        print_f_rec (d+1) f

      | ELet (p,_,f1,f2,linf) ->
        print_string "LET: ";
        Predicate.print_predicate p;
        print_linf " linf=" linf;
        print_f_rec (d+1) f1;
        print_f_rec (d+1) f2

      | EAnd (_,f1,f2,ainf) ->
        print_ainf "AND: ainf=" ainf.arel;
        print_string "\n";
        print_f_rec (d+1) f1;
        print_f_rec (d+1) f2

      | EOr (_,f1,f2,ainf) ->
        print_ainf "OR: ainf=" ainf.arel;
        print_string "\n";
        print_f_rec (d+1) f1;
        print_f_rec (d+1) f2

      | ESinceA (_, _,intv,f1,f2,sinf) ->
        print_string "SINCE";
        MFOTL.print_interval intv;
        print_sainf ": sinf = " sinf;
        print_string "\n";
        print_f_rec (d+1) f1;
        print_f_rec (d+1) f2

      | ESince (_, _,intv,f1,f2,sinf) ->
        print_string "SINCE";
        MFOTL.print_interval intv;
        print_sinf ": sinf=" sinf;
        print_string "\n";
        print_f_rec (d+1) f1;
        print_f_rec (d+1) f2

      | EUntil (_,_,intv,f1,f2,uinf) ->
        print_string "UNTIL";
        MFOTL.print_interval intv;
        print_uinf ": uinf=" uinf;
        print_f_rec (d+1) f1;
        print_f_rec (d+1) f2

      | ENUntil (_,_,_,intv,f1,f2,uninf) ->
        print_string "NUNTIL";
        MFOTL.print_interval intv;
        print_uninf ": uninf=" uninf;
        print_f_rec (d+1) f1;
        print_f_rec (d+1) f2

      | EAggreg (inf, _, f1) ->
        print_agg_inf "AGGREG: inf=" inf;
        print_f_rec (d+1) f1

      | EAggOnce (inf, _, f1) ->
        print_agg_inf "AGG ONCE: inf=" inf;
        print_f_rec (d+1) f1

    )
  in
  print_string str;
  print_f_rec 0 ff


type rel_stats = {
  count: int;
  tuples: int;
  values: int;
}

let zero_rel_stats = { count = 0; tuples = 0; values = 0 }

let add_rel_stats { count = a1; tuples = b1; values = c1 }
  { count = a2; tuples = b2; values = c2 } =
    { count = a1 + a2; tuples = b1 + b2; values = c1 + c2 }

let rel_stats rel =
  let set = Relation.to_set rel in
  let tuples = Relation.Tuple_set.cardinal set in
  let values = if tuples = 0 then 0 else
    tuples * Tuple.length (Relation.Tuple_set.choose set) in
  { count = 1; tuples; values }

let opt_rel_stats = function
  | Some rel -> rel_stats rel
  | None -> zero_rel_stats

let queue3_stats = Queue.fold (fun acc (_, _, rel) ->
  add_rel_stats acc (rel_stats rel)) zero_rel_stats

let mqueue2_stats = Mqueue.fold (fun acc (_, rel) ->
  add_rel_stats acc (rel_stats rel)) zero_rel_stats

let stree_ts_stats = Sliding.fold_res (fun acc rel ->
  add_rel_stats acc (rel_stats rel)) zero_rel_stats

let stree_tp_stats = Sliding.fold_res (fun acc rel ->
  add_rel_stats acc (rel_stats rel)) zero_rel_stats

let dllist3_stats l =
  let acc = ref zero_rel_stats in
  Dllist.iter (fun (_, _, rel) -> acc := add_rel_stats !acc (rel_stats rel)) l;
  !acc

let dllist2_stats l =
  let acc = ref zero_rel_stats in
  Dllist.iter (fun (_, rel) -> acc := add_rel_stats !acc (rel_stats rel)) l;
  !acc

type stats = {
  extf_size: int;
  queue_nodes: int;
  tree_nodes: int;
  rels: rel_stats;
}

let add_stats { extf_size = a1; queue_nodes = b1; tree_nodes = c1; rels = d1 }
  { extf_size = a2; queue_nodes = b2; tree_nodes = c2; rels = d2 } =
    { extf_size = a1 + a2;
      queue_nodes = b1 + b2;
      tree_nodes = c1 + c2;
      rels = add_rel_stats d1 d2 }

let rec stats_of_extf = function
  | ERel rel ->
    { extf_size = 1;
      queue_nodes = 0;
      tree_nodes = 0;
      rels = rel_stats rel }
  | EPred (_pred, _comp, inf) ->
    { extf_size = 1;
      queue_nodes = Queue.length inf;
      tree_nodes = 0;
      rels = queue3_stats inf }
  | ELet (_pred, _comp, f1, f2, _inf) ->
    add_stats (add_stats (stats_of_extf f1) (stats_of_extf f2))
      { extf_size = 1;
        queue_nodes = 0;
        tree_nodes = 0;
        rels = zero_rel_stats; }
  | ENeg f1
  | EExists (_, f1)
  | EPrev (_, f1, _)
  | ENext (_, f1, _) ->
    add_stats (stats_of_extf f1)
      { extf_size = 1;
        queue_nodes = 0;
        tree_nodes = 0;
        rels = zero_rel_stats; }
  | EAnd (_comp, f1, f2, inf)
  | EOr (_comp, f1, f2, inf) ->
    add_stats (add_stats (stats_of_extf f1) (stats_of_extf f2))
      { extf_size = 1;
        queue_nodes = 0;
        tree_nodes = 0;
        rels = opt_rel_stats inf.arel }
  | ESinceA (_comp1, _comp2, _intv, f1, f2, inf) ->
    add_stats (add_stats (stats_of_extf f1) (stats_of_extf f2))
      { extf_size = 1;
        queue_nodes = Mqueue.length (inf.saauxrels);
        tree_nodes = 0;
        rels = add_rel_stats (add_rel_stats (rel_stats inf.sres)
          (opt_rel_stats inf.sarel2)) (mqueue2_stats inf.saauxrels) }
  | ESince (_comp1, _comp2, _intv, f1, f2, inf) ->
    add_stats (add_stats (stats_of_extf f1) (stats_of_extf f2))
      { extf_size = 1;
        queue_nodes = Mqueue.length (inf.sauxrels);
        tree_nodes = 0;
        rels = add_rel_stats (opt_rel_stats inf.srel2)
          (mqueue2_stats inf.sauxrels) }
  | EOnceA (_intv, f1, inf) ->
    add_stats (stats_of_extf f1)
      { extf_size = 1;
        queue_nodes = Mqueue.length (inf.oaauxrels);
        tree_nodes = 0;
        rels = add_rel_stats (rel_stats inf.ores)
          (mqueue2_stats inf.oaauxrels) }
  | EOnceZ (_intv, f1, inf) ->
    add_stats (stats_of_extf f1)
      { extf_size = 1;
        queue_nodes = Dllist.length inf.ozauxrels;
        tree_nodes = Sliding.num_nodes inf.oztree;
        rels = add_rel_stats (stree_tp_stats inf.oztree)
          (dllist3_stats inf.ozauxrels) }
  | EOnce (_intv, f1, inf) ->
    add_stats (stats_of_extf f1)
      { extf_size = 1;
        queue_nodes = Dllist.length inf.oauxrels;
        tree_nodes = Sliding.num_nodes inf.otree;
        rels = add_rel_stats (stree_ts_stats inf.otree)
          (dllist2_stats inf.oauxrels) }
  | _ -> failwith "[Extformula.stats_of_extf] not implemented"

let print_stats0 str { extf_size; queue_nodes; tree_nodes; rels } =
  Printf.printf "%sextf_size = %d; queue_nodes = %d; tree_nodes = %d; "
    str extf_size queue_nodes tree_nodes;
  Printf.printf "rel_count = %d; rel_tuples = %d; rel_values = %d\n"
    rels.count rels.tuples rels.values

let print_stats str ff = print_stats0 str (stats_of_extf ff)
