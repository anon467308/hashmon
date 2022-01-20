open Format
open MFOTL

(* Utilities *)

let rec update_assoc k f = function
  | [] -> raise Not_found
  | (k', v) :: l when k = k' -> (k, f v) :: l
  | x :: l -> x :: update_assoc k f l

module Counter = struct
  type t = int ref

  let create x = ref x
  let get_and_incr ctr = let x = !ctr in ctr := x + 1; x
end

type var = string
type term = Predicate.term

module Var_set = Set.Make (struct
  type t = var
  let compare = Stdlib.compare
end)

module Var_sset = Set.Make (struct
  type t = Var_set.t
  let compare = Var_set.compare
end)

module Var_map = Map.Make (struct
  type t = var
  let compare = Stdlib.compare
end)

let var_of_term = function
  | Predicate.Var x -> x
  | _ -> failwith "[Hashing.var_of_term] term is not a variable"

let vars_of_term t = Var_set.of_list (Predicate.tvars t)

let cardinal rel = Relation.Tuple_set.cardinal (Relation.to_set rel)

module Clhash = struct
  type key
  external random_key: Int64.t -> Int64.t -> key = "clhash_random_key"
  external hash_int: key -> int -> int = "clhash_int"
  external hash_float: key -> float -> int = "clhash_float"
  external hash_string: key -> string -> int = "clhash_string"
  external collision_prob: int -> float = "clhash_collision_prob"
  external gf_mul: int -> int -> int -> int = "clhash_gf_mul"
end


(* Temporal-relational algebra expressions *)

type selectop =
  | SelectEq
  | SelectLess
  | SelectLessEq

type pos = P | N
type dir = Past | Future

type 'a relexpr =
  | RConst of 'a * Relation.relation * var list
  | RInput of 'a * string * var list
  | RLet of 'a * string * 'a relexpr * 'a relexpr
  | RRename of 'a * (var * var) list * 'a relexpr
  | RDrop of 'a * var list * 'a relexpr
  | RExtend of 'a * var * term * 'a relexpr
  | RSelect of 'a * (pos * selectop * term * term) * 'a relexpr
  | RJoin of 'a * 'a relexpr * pos * 'a relexpr
  | RUnion of 'a * 'a relexpr * 'a relexpr
  | RAgg of 'a * Predicate.tsymb * var * agg_op * var * var list * 'a relexpr
  | RNext of 'a * dir * interval * 'a relexpr
  | RUntil of 'a * dir * interval * pos * 'a relexpr * 'a relexpr
  | RHash of 'a * (var * var list) list * 'a relexpr

let rec fv_of_relexpr = function
  | RConst (_, _, attr) -> attr
  | RInput (_, _, attr) -> attr
  | RLet (_, _, _, e2) -> fv_of_relexpr e2
  | RRename (_, ren, _) -> List.map fst ren
  | RDrop (_, prj, e1) -> Misc.diff (fv_of_relexpr e1) prj
  | RExtend (_, x, _, e1) -> Misc.union (fv_of_relexpr e1) [x]
  | RSelect (_, _, e1) -> fv_of_relexpr e1
  | RJoin (_, e1, _, e2) -> Misc.union (fv_of_relexpr e1) (fv_of_relexpr e2)
  | RUnion (_, e1, e2) -> Misc.union (fv_of_relexpr e1) (fv_of_relexpr e2)
  | RAgg (_, _, y, _, _, gl, _) -> Misc.union [y] gl
  | RNext (_, _, _, e1) -> fv_of_relexpr e1
  | RUntil (_, _, _, _, _, e2) -> fv_of_relexpr e2
  | RHash (_, hmap, _) -> List.map fst hmap

let fv_set_of_relexpr e = Var_set.of_list (fv_of_relexpr e)

let rec vars_of_relexpr = function
  | RConst (_, _, attr) -> Var_set.of_list attr
  | RInput (_, _, attr) -> Var_set.of_list attr
  | RLet (_, _, e1, e2) ->
    Var_set.union (vars_of_relexpr e1) (vars_of_relexpr e2)
  | RRename (_, ren, e1) ->
    Var_set.union (Var_set.of_list (List.map fst ren)) (vars_of_relexpr e1)
  | RDrop (_, _, e1) -> vars_of_relexpr e1
  | RExtend (_, x, _, e1) -> Var_set.add x (vars_of_relexpr e1)
  | RSelect (_, _, e1) -> vars_of_relexpr e1
  | RJoin (_, e1, _, e2) ->
    Var_set.union (vars_of_relexpr e1) (vars_of_relexpr e2)
  | RUnion (_, e1, e2) ->
    Var_set.union (vars_of_relexpr e1) (vars_of_relexpr e2)
  | RAgg (_, _, y, _, _, _, e1) -> Var_set.add y (vars_of_relexpr e1)
  | RNext (_, _, _, e1) -> vars_of_relexpr e1
  | RUntil (_, _, _, _, e1, e2) ->
    Var_set.union (vars_of_relexpr e1) (vars_of_relexpr e2)
  | RHash (_, hmap, e1) ->
    Var_set.union (Var_set.of_list (List.map fst hmap)) (vars_of_relexpr e1)

let aux_info_of = function
  | RConst (aux, _, _)
  | RInput (aux, _, _)
  | RLet (aux, _, _, _)
  | RRename (aux, _, _)
  | RDrop (aux, _, _)
  | RExtend (aux, _, _, _)
  | RSelect (aux, _, _)
  | RJoin (aux, _, _, _)
  | RUnion (aux, _, _)
  | RAgg (aux, _, _, _, _, _, _)
  | RNext (aux, _, _, _)
  | RUntil (aux, _, _, _, _, _)
  | RHash (aux, _, _) -> aux

let set_aux_info aux = function
  | RConst (_, rel, attr) -> RConst (aux, rel, attr)
  | RInput (_, name, attr) -> RInput (aux, name, attr)
  | RLet (_, name, e1, e2) -> RLet (aux, name, e1, e2)
  | RRename (_, ren, e1) -> RRename (aux, ren, e1)
  | RDrop (_, prj, e1) -> RDrop (aux, prj, e1)
  | RExtend (_, x, t, e1) -> RExtend (aux, x, t, e1)
  | RSelect (_, cond, e1) -> RSelect (aux, cond, e1)
  | RJoin (_, e1, pos, e2) -> RJoin (aux, e1, pos, e2)
  | RUnion (_, e1, e2) -> RUnion (aux, e1, e2)
  | RAgg (_, ty, y, op, x, gl, e1) -> RAgg (aux, ty, y, op, x, gl, e1)
  | RNext (_, dir, intv, e1) -> RNext (aux, dir, intv, e1)
  | RUntil (_, dir, intv, pos, e1, e2) -> RUntil (aux, dir, intv, pos, e1, e2)
  | RHash (_, hmap, e1) -> RHash (aux, hmap, e1)

let pp_comma ppf () =
  pp_print_string ppf ",";
  pp_print_space ppf ()

let pp_paren_list pp_v ppf l =
  pp_open_hovbox ppf 2;
  pp_print_string ppf "(";
  pp_print_list ~pp_sep:pp_comma pp_v ppf l;
  pp_print_string ppf ")";
  pp_close_box ppf ()

let pp_cst ppf c = pp_print_string ppf (Predicate.string_of_cst true c)

let pp_term ppf t = pp_print_text ppf (Predicate.string_of_term t)

let pp_tuple ppf t = pp_paren_list pp_cst ppf (Tuple.get_constants t)

let pp_relation ppf rel =
  pp_open_hvbox ppf 1;
  pp_print_string ppf "{";
  let elements = Relation.Tuple_set.elements (Relation.to_set rel) in
  pp_print_list ~pp_sep:pp_comma pp_tuple ppf elements;
  pp_print_break ppf 0 (-1);
  pp_print_string ppf "}";
  pp_close_box ppf ()

let pp_hash_pair ppf (x, l) =
  match l with
  | [] -> pp_print_string ppf x
  | _ ->
    pp_print_string ppf x;
    pp_print_string ppf " :=";
    pp_print_space ppf ();
    pp_paren_list pp_print_string ppf l

let relexpr_needs_parens bin_ctxt = function
  | RConst _
  | RInput _ -> false
  | RRename _
  | RDrop _
  | RExtend _
  | RSelect _
  | RAgg _
  | RNext _
  | RHash _ -> bin_ctxt
  | _ -> true

let pp_relexpr ppf e =
  let rec pp = function
    | RConst (_, rel, attr) ->
      if attr = [] && not (Relation.is_empty rel) then
        pp_print_string ppf "TRUE"
      else begin
        pp_open_box ppf 2;
        pp_relation ppf rel;
        pp_print_string ppf ":";
        pp_print_space ppf ();
        pp_paren_list pp_print_string ppf attr;
        pp_close_box ppf ()
      end
    | RInput (_, name, attr) ->
      pp_open_hbox ppf ();
      pp_print_string ppf name;
      pp_paren_list pp_print_string ppf attr;
      pp_close_box ppf ()
    | RLet (_, name, e1, e2) ->
      pp_open_vbox ppf 0;
      pp_open_hvbox ppf 2;
      pp_print_string ppf "LET ";
      pp_print_string ppf name;
      pp_print_string ppf " =";
      pp_print_space ppf ();
      pp e1;
      pp_print_break ppf 1 (-2);
      pp_print_string ppf "IN";
      pp_close_box ppf ();
      pp_print_space ppf ();
      pp e2;
      pp_close_box ppf ()
    | RRename (_, ren, e1) ->
      pp_open_box ppf 0;
      pp' ~bin_ctxt:false e1;
      pp_print_space ppf ();
      pp_open_hbox ppf ();
      pp_print_string ppf "RENAME";
      pp_print_space ppf ();
      let p ppf (x, y) =
        pp_print_string ppf x;
        pp_print_string ppf " := ";
        pp_print_string ppf y
      in
      pp_paren_list p ppf ren;
      pp_close_box ppf ();
      pp_close_box ppf ()
    | RDrop (_, prj, e1) ->
      pp_open_box ppf 0;
      pp' ~bin_ctxt:false e1;
      pp_print_space ppf ();
      pp_open_hbox ppf ();
      pp_print_string ppf "DROP";
      pp_print_space ppf ();
      pp_paren_list pp_print_string ppf prj;
      pp_close_box ppf ();
      pp_close_box ppf ()
    | RExtend (_, x, t, e1) ->
      pp_open_box ppf 0;
      pp' ~bin_ctxt:false e1;
      pp_print_space ppf ();
      pp_open_hbox ppf ();
      pp_print_string ppf "EXTEND";
      pp_print_space ppf ();
      pp_open_hovbox ppf 2;
      pp_print_string ppf "(";
      pp_print_string ppf x;
      pp_print_string ppf " :=";
      pp_print_space ppf ();
      pp_term ppf t;
      pp_print_string ppf ")";
      pp_close_box ppf ();
      pp_close_box ppf ();
      pp_close_box ppf ()
    | RSelect (_, (pos, r, t1, t2), e1) ->
      pp_open_box ppf 0;
      pp' ~bin_ctxt:false e1;
      pp_print_space ppf ();
      pp_open_hbox ppf ();
      pp_print_string ppf "SELECT";
      pp_print_space ppf ();
      pp_open_hovbox ppf 2;
      pp_print_string ppf "(";
      pp_term ppf t1;
      pp_print_string ppf (match r with
        | SelectEq when pos = P -> " ="
        | SelectEq -> " <>"
        | SelectLess when pos = P -> " <"
        | SelectLess -> " >="
        | SelectLessEq when pos = P -> " <="
        | SelectLessEq -> " >"
        );
      pp_print_space ppf ();
      pp_term ppf t2;
      pp_print_string ppf ")";
      pp_close_box ppf ();
      pp_close_box ppf ();
      pp_close_box ppf ()
    | RJoin (_, e1, pos, e2) ->
      pp_open_hvbox ppf 0;
      pp' ~bin_ctxt:true e1;
      pp_print_space ppf ();
      pp_print_string ppf (match pos with P -> "JOIN" | N -> "DIFF");
      pp_print_space ppf ();
      pp' ~bin_ctxt:true e2;
      pp_close_box ppf ()
    | RUnion (_, e1, e2) ->
      pp_open_hvbox ppf 0;
      pp' ~bin_ctxt:true e1;
      pp_print_space ppf ();
      pp_print_string ppf "UNION";
      pp_print_space ppf ();
      pp' ~bin_ctxt:true e2;
      pp_close_box ppf ()
    | RAgg (_, ty, y, op, x, gl, e1) ->
      pp_open_box ppf 0;
      pp' ~bin_ctxt:false e1;
      pp_print_space ppf ();
      pp_open_box ppf 2;
      pp_print_string ppf "AGGREG";
      pp_print_space ppf ();
      pp_print_string ppf y;
      pp_print_string ppf " :=";
      pp_print_space ppf ();
      pp_print_string ppf (string_of_agg_op op ^ " " ^ x);
      pp_print_space ppf ();
      pp_print_string ppf "GROUP BY";
      pp_print_space ppf ();
      pp_paren_list pp_print_string ppf gl;
      pp_close_box ppf ();
      pp_close_box ppf ()
    | RNext (_, dir, intv, e1) ->
      pp_open_box ppf 0;
      pp_open_hbox ppf ();
      pp_print_string ppf (match dir with Past -> "PREV" | Future -> "NEXT");
      pp_print_space ppf ();
      pp_print_string ppf (MFOTL.string_of_interval intv);
      pp_close_box ppf ();
      pp_print_space ppf ();
      pp' ~bin_ctxt:true e1;
      pp_close_box ppf ()
    | RUntil (_, dir, intv, pos, e1, e2) ->
      pp_open_hvbox ppf 0;
      pp' ~bin_ctxt:true e1;
      pp_print_space ppf ();
      pp_open_hbox ppf ();
      pp_print_string ppf (match dir, pos with
        | Past, P -> "SINCE"
        | Past, N -> "NOT SINCE"
        | Future, P -> "UNTIL"
        | Future, N -> "NOT UNTIL");
      pp_print_space ppf ();
      pp_print_string ppf (MFOTL.string_of_interval intv);
      pp_close_box ppf ();
      pp_print_space ppf ();
      pp' ~bin_ctxt:true e2;
      pp_close_box ppf ()
    | RHash (_, hmap, e1) ->
      pp_open_box ppf 0;
      pp' ~bin_ctxt:false e1;
      pp_print_space ppf ();
      pp_open_hbox ppf ();
      pp_print_string ppf "HASH";
      pp_print_space ppf ();
      pp_paren_list pp_hash_pair ppf hmap;
      pp_close_box ppf ();
      pp_close_box ppf ()
  and pp' ~bin_ctxt e =
    if relexpr_needs_parens bin_ctxt e then begin
      pp_print_string ppf "(";
      pp e;
      pp_print_string ppf ")"
    end else
      pp e
  in
  pp_open_box ppf 0;
  pp e;
  pp_close_box ppf ()

let print_relexpr e =
  pp_relexpr std_formatter e;
  print_newline ()


(* Translation from MFOTL to relation algebra *)

let internal_path = "_internal_"

let ext_path name path = path ^ "." ^ name

let true_relexpr =
  let rel = Relation.make_relation [Tuple.make_tuple []] in
  RConst (internal_path, rel, [])

let mk_rename attr ren e =
  if List.for_all2 (fun (x, y) z -> x = y && y = z) ren attr then e
  else RRename (internal_path, ren, e)

let mk_drop prj e = if prj = [] then e else RDrop (internal_path, prj, e)

let rec mk_selects sel e =
  match sel with
  | [] -> e
  | cond :: l -> mk_selects l (RSelect (internal_path, cond, e))

let rec invent_var ctr vl =
  let name = "$" ^ string_of_int (Counter.get_and_incr ctr) in
  if List.mem name vl then invent_var ctr vl else name

let relexpr_of_mfotl f =
  let rec translate path let_preds = function
    | Equal (t1, t2) as f ->
      let attr = MFOTL.free_vars f in
      let rel = Relation.eval_equal t1 t2 in
      RConst (path, rel, attr)
    | Pred (name, _ar, args) as f ->
      (match List.assoc_opt name let_preds with
      | None ->
        let attr = MFOTL.free_vars f in
        let ctr = Counter.create 0 in
        let rec do_args vars sel prj = function
          | [] -> (List.rev vars, List.rev sel, List.rev prj)
          | Predicate.Var x :: l when not (List.mem x vars) ->
            do_args (x :: vars) sel prj l
          | t :: l ->
            let x = invent_var ctr attr in
            let cond = (P, SelectEq, Predicate.Var x, t) in
            do_args (x :: vars) (cond :: sel) (x :: prj) l
        in
        let (in_attr, sel, prj) = do_args [] [] [] args in
        let e = RInput (internal_path, name, in_attr) in
        let e = mk_selects sel e in
        let e = mk_drop prj e in
        set_aux_info path e
      | Some (in_attr, def_attr) ->
        let rec do_args sel prj ren = function
          | [], [] -> List.rev sel, List.rev prj, List.rev ren
          | x :: l1, Predicate.Var y :: l2 when not (List.mem_assoc y ren) ->
            do_args sel prj ((y, x) :: ren) (l1, l2)
          | x :: l1, t :: l2 ->
            let t' = match t with
              | Predicate.Var y -> Predicate.Var (List.assoc y ren)
              | Predicate.Cst c as t -> t
              | _ -> failwith "[Hashing.relexpr_of_mfotl] not monitorable"
            in
            let cond = (P, SelectEq, Predicate.Var x, t') in
            do_args (cond :: sel) (x :: prj) ren (l1, l2)
          | _ -> failwith "[Hashing.relexpr_of_mfotl] internal error"
        in
        let (sel, prj, ren) = do_args [] [] [] (def_attr, args) in
        let e = RInput (internal_path, name, in_attr) in
        let e = mk_selects sel e in
        let e = mk_drop prj e in
        let e = mk_rename in_attr ren e in
        set_aux_info path e
      )
    | Let ((name, _ar, args), f1, f2) ->
      let e1 = translate (ext_path "let.1" path) let_preds f1 in
      let attr1 = fv_of_relexpr e1 in
      let def_attr = List.map var_of_term args in
      let ren = Misc.zip def_attr def_attr in
      let e1 = mk_rename attr1 ren e1 in
      let let_preds2 = (name, (attr1, def_attr)) :: let_preds in
      let e2 = translate (ext_path "let.2" path) let_preds2 f2 in
      RLet (path, name, e1, e2)
    | Neg f1 ->
      let e1 = translate (ext_path "not" path) let_preds f1 in
      RJoin (path, true_relexpr, N, e1)
    | And (f1, f2) ->
      let attr1 = MFOTL.free_vars f1 in
      let attr2 = MFOTL.free_vars f2 in
      let e1 = translate (ext_path "and.1" path) let_preds f1 in
      if Rewriting.is_special_case attr1 attr2 f2 then
        if Misc.subset attr2 attr1 then
          let cond = match f2 with
            | Equal (t1, t2) -> (P, SelectEq, t1, t2)
            | Less (t1, t2) -> (P, SelectLess, t1, t2)
            | LessEq (t1, t2) -> (P, SelectLessEq, t1, t2)
            | Neg (Equal (t1, t2)) -> (N, SelectEq, t1, t2)
            | Neg (Less (t1, t2)) -> (N, SelectLess, t1, t2)
            | Neg (LessEq (t1, t2)) -> (N, SelectLessEq, t1, t2)
            | _ -> failwith "[Hashing.relexpr_of_mfotl] internal error"
          in
          RSelect (path, cond, e1)
        else
          let x, t = match f2 with
            | Equal (Predicate.Var x, t)
            | Equal (t, Predicate.Var x) -> x, t
            | _ -> failwith "[Hashing.relexpr_of_mfotl] internal error"
          in
          RExtend (path, x, t, e1)
      else begin
        match f2 with
        | Neg f2 ->
          let e2 = translate (ext_path "and.2.not" path) let_preds f2 in
          RJoin (path, e1, N, e2)
        | _ ->
          let e2 = translate (ext_path "and.2" path) let_preds f2 in
          RJoin (path, e1, P, e2)
      end
    | Or (f1, f2) ->
      let e1 = translate (ext_path "or.1" path) let_preds f1 in
      let e2 = translate (ext_path "or.2" path) let_preds f2 in
      RUnion (path, e1, e2)
    | Exists (vl, f1) ->
      let e1 = translate (ext_path "ex" path) let_preds f1 in
      set_aux_info path (mk_drop vl e1)
    | Aggreg (ty, y, op, x, gl, f1) ->
      let e1 = translate (ext_path "agg" path) let_preds f1 in
      RAgg (path, ty, y, op, x, gl, e1)
    | Prev (intv, f1) ->
      let e1 = translate (ext_path "prev" path) let_preds f1 in
      RNext (path, Past, intv, e1)
    | Next (intv, f1) ->
      let e1 = translate (ext_path "next" path) let_preds f1 in
      RNext (path, Future, intv, e1)
    | Once (intv, f1) ->
      let e1 = translate (ext_path "once" path) let_preds f1 in
      RUntil (path, Past, intv, P, true_relexpr, e1)
    | Eventually (intv, f1) ->
      let e1 = translate (ext_path "eventually" path) let_preds f1 in
      RUntil (path, Future, intv, P, true_relexpr, e1)
    | Since (intv, f1, f2) ->
      let pos, e1 = match f1 with
        | Neg f1 -> N, translate (ext_path "since.1.not" path) let_preds f1
        | _ -> P, translate (ext_path "since.1" path) let_preds f1
      in
      let e2 = translate (ext_path "since.2" path) let_preds f2 in
      RUntil (path, Past, intv, pos, e1, e2)
    | Until (intv, f1, f2) ->
      let pos, e1 = match f1 with
        | Neg f1 -> N, translate (ext_path "until.1.not" path) let_preds f1
        | _ -> P, translate (ext_path "until.1" path) let_preds f1
      in
      let e2 = translate (ext_path "until.2" path) let_preds f2 in
      RUntil (path, Future, intv, pos, e1, e2)
    | Implies _
    | Equiv _
    | ForAll _
    | Always _
    | PastAlways _
    | Frex _
    | Prex _ -> failwith "[Hashing.relexpr_of_mfotl] not implemented"
    | Less _
    | LessEq _ -> failwith "[Hashing.relexpr_of_mfotl] not monitorable"
  in
  translate "" [] f


(* Naming of hashed variables *)

let hash_var_of_set s =
  let append v acc = acc ^ "#" ^ v in
  Var_set.fold append s ""

let hashed_var ss v =
  match Seq.filter (Var_set.mem v) (Var_sset.to_seq ss) () with
  | Seq.Nil -> v
  | Seq.Cons (s, _) -> hash_var_of_set s

let is_hashed x = (x.[0] = '#')

let unhash1 x = String.sub x 1 (String.length x - 1)

let set_of_hash_var x = Var_set.of_list (List.tl (String.split_on_char '#' x))


(* Insertion of hashing operations into relational algebra *)

(* 1. pass (down): propagate error acceptance and hashing constraints *)

let add_nonempty s ss = if Var_set.is_empty s then ss else Var_sset.add s ss

let add_group ~merge s ss =
  if merge then
    add_nonempty s ss
  else
    Var_set.fold (fun v -> Var_sset.add (Var_set.singleton v)) s ss

let sift s1 ss =
  let aux s2 acc = add_nonempty (Var_set.diff s2 s1)
    (add_nonempty (Var_set.inter s1 s2) acc)
  in Var_sset.fold aux ss Var_sset.empty

let vars_of_groups ss = Var_sset.fold Var_set.union ss Var_set.empty

let restrict_groups scope ss = Var_sset.remove Var_set.empty
  (Var_sset.map (fun s -> Var_set.inter s scope) ss)

let groups_without del ss = Var_sset.remove Var_set.empty
  (Var_sset.map (fun s -> Var_set.diff s del) ss)

let modified_by_rename ren = List.filter_map
  (fun (dest, src) -> if dest = src then None else Some dest) ren

let rename_preimage ren vs = List.fold_left (fun s (dest, src) ->
  if Var_set.mem dest vs then Var_set.add src s else s)
  Var_set.empty ren

let rename_preimages ren vss = Var_sset.map (rename_preimage ren) vss

let vars_of_select (_pos, _op, t1, t2) =
  Var_set.union (vars_of_term t1) (vars_of_term t2)

let exact_of_select accept_err ((_pos, op, t1, t2) as cond) =
  if accept_err then
    (match op with
    | SelectEq -> (match t1, t2 with
        (* TODO: support comparison between hashed variable and term *)
        | Predicate.Var _, Predicate.Var _ -> Var_set.empty
        | Predicate.Var _, t2 when Predicate.is_gterm t2 -> Var_set.empty
        | t1, Predicate.Var _ when Predicate.is_gterm t1 -> Var_set.empty
        | _ -> vars_of_select cond
        )
    | _ -> vars_of_select cond
    )
  else vars_of_select cond

type constr = {
  accept_err: bool;
  out_groups: Var_sset.t;
}

let init_constr ~merge e = {
  accept_err = true;
  out_groups = add_group ~merge (fv_set_of_relexpr e) Var_sset.empty;
}

let update_constr c_in c_out = {
  accept_err = c_in.accept_err && c_out.accept_err;
  out_groups = Var_sset.fold sift c_in.out_groups
    (restrict_groups (vars_of_groups c_in.out_groups) c_out.out_groups)
}

let compute_constr ~merge e =
  let rec down lets constr e =
    let constr = update_constr constr (init_constr ~merge e) in
    match e with
    | RConst (path, rel, attr) -> lets, RConst ((path, None), rel, attr)
    | RInput (path, name, attr) ->
      let lets =
        if List.mem_assoc name lets then
          update_assoc name (update_constr constr) lets
        else lets
      in
      lets, RInput ((path, None), name, attr)
    | RLet (path, name, e1, e2) ->
      let lets1, e2 = down ((name, init_constr ~merge e1) :: lets) constr e2 in
      let constr1 = List.assoc name lets1 in
      let lets1 = List.remove_assoc name lets1 in
      let lets, e1 = down lets1 constr1 e1 in
      lets, RLet ((path, Some constr1), name, e1, e2)
    | RRename (path, ren, e1) ->
      let constr1 = { constr with
        out_groups = rename_preimages ren
          (List.fold_left (fun ss v -> sift (Var_set.singleton v) ss)
            constr.out_groups (modified_by_rename ren));
      } in
      let lets, e1 = down lets constr1 e1 in
      lets, RRename ((path, None), ren, e1)
    | RDrop (path, prj, e1) ->
      let constr1 = { constr with
        out_groups = add_group ~merge (Var_set.of_list prj) constr.out_groups;
      } in
      let lets, e1 = down lets constr1 e1 in
      lets, RDrop ((path, None), prj, e1)
    | RExtend (path, x, t, e1) ->
      let in_groups = match t with
        | Predicate.Var y -> sift (Var_set.singleton y) constr.out_groups
        | _ -> groups_without (vars_of_term t) constr.out_groups
      in
      let constr1 = { constr with out_groups = in_groups } in
      let lets, e1 = down lets constr1 e1 in
      lets, RExtend ((path, None), x, t, e1)
    | RSelect (path, cond, e1) ->
      let in_groups = groups_without
        (exact_of_select constr.accept_err cond)
        (Var_set.fold (fun v -> sift (Var_set.singleton v))
          (vars_of_select cond) constr.out_groups)
      in
      let constr1 = { constr with out_groups = in_groups } in
      let lets, e1 = down lets constr1 e1 in
      lets, RSelect ((path, None), cond, e1)
    | RJoin (path, e1, pos, e2) ->
      let key = Var_set.inter (fv_set_of_relexpr e1) (fv_set_of_relexpr e2) in
      let in_groups =
        if constr.accept_err then
          sift key constr.out_groups
        else
          groups_without key constr.out_groups
      in
      let constr12 = { constr with out_groups = in_groups } in
      let lets, e1 = down lets constr12 e1 in
      let lets, e2 = down lets constr12 e2 in
      lets, RJoin ((path, None), e1, pos, e2)
    | RUnion (path, e1, e2) ->
      let lets, e1 = down lets constr e1 in
      let lets, e2 = down lets constr e2 in
      lets, RUnion ((path, None), e1, e2)
    | RAgg (path, ty, y, op, x, gl, e1) ->
      let groupby = Var_set.of_list gl in
      let prj = Var_set.diff (fv_set_of_relexpr e1) groupby in
      let prj = match op with
        | Cnt -> prj
        | _ -> Var_set.remove x prj
      in
      let in_groups =
        if constr.accept_err then
          add_group ~merge prj (restrict_groups groupby constr.out_groups)
        else
          (match op with
          | Min | Max -> add_group ~merge prj Var_sset.empty
          | _ -> Var_sset.empty)
      in
      let constr1 = { constr with out_groups = in_groups } in
      let lets, e1 = down lets constr1 e1 in
      lets, RAgg ((path, None), ty, y, op, x, gl, e1)
    | RNext (path, dir, intv, e1) ->
      let lets, e1 = down lets constr e1 in
      lets, RNext ((path, None), dir, intv, e1)
    | RUntil (path, dir, intv, pos, e1, e2) ->
      let key = fv_set_of_relexpr e1 in
      let in_groups =
        if snd intv <> Inf && constr.accept_err then
          sift key constr.out_groups
        else
          groups_without key constr.out_groups
      in
      let constr12 = {
        accept_err = snd intv <> Inf && constr.accept_err;
        out_groups = in_groups;
      } in
      let lets, e1 = down lets constr12 e1 in
      let lets, e2 = down lets constr12 e2 in
      lets, RUntil ((path, Some constr12), dir, intv, pos, e1, e2)
    | RHash _ -> failwith "[Hashing.compute_constr] internal error"
  in
  snd (down [] {accept_err = true; out_groups = Var_sset.empty} e)

(* 2. pass (up): propagate actual groups and insert hash operators *)

let rename_image ren vs = List.fold_left (fun s (dest, src) ->
  if Var_set.mem src vs then Var_set.add dest s else s) Var_set.empty ren

let rename_images ren vss = Var_sset.map (rename_image ren) vss

let strict_subset s1 s2 = Var_set.(not (equal s1 s2) && subset s1 s2)

let join_groups ss1 ss2 = Var_sset.union
  (Var_sset.filter (fun s -> not (Var_sset.exists (strict_subset s) ss2)) ss1)
  (Var_sset.filter (fun s -> not (Var_sset.exists (strict_subset s) ss1)) ss2)

let hash_delta dest_groups (src_fv, src_groups) e =
  let containing_dest s =
    match Seq.filter (Var_set.subset s) (Var_sset.to_seq dest_groups) () with
    | Seq.Nil -> None
    | Seq.Cons (s, _) -> Some s
  in
  let is_reusable s s' = Var_set.subset s' s in
  let reusable s = Var_sset.filter (is_reusable s) src_groups in
  let new_group_prj s =
    let reused_groups = reusable s in
    let reused_vars = Var_sset.fold Var_set.union reused_groups
      Var_set.empty in
    let added_vars = Var_set.elements (Var_set.diff s reused_vars) in
    if Var_sset.cardinal reused_groups = 1 && added_vars = [] then None
    else
      let reused_names = List.map hash_var_of_set
        (Var_sset.elements reused_groups) in
      Some (hash_var_of_set s, added_vars @ reused_names)
  in
  let update_var (l, covs) v =
    let src = if is_hashed v then set_of_hash_var v else Var_set.singleton v in
    if Var_set.subset src covs then (l, covs)
    else
      match containing_dest src with
      | None -> ((v, [])::l, Var_set.add v covs)
      | Some group ->
        (match new_group_prj group with
        | None -> ((v, [])::l, Var_set.union src covs)
        | Some x -> (x::l, Var_set.union group covs))
  in
  let hmap = List.rev (fst
    (List.fold_left update_var ([], Var_set.empty) src_fv)) in
  if List.for_all (fun (_,l) -> l = []) hmap then e
  else RHash (aux_info_of e, hmap, e)

let insert_hashing e =
  let rec up lets = function
    | RConst ((path, _), rel, attr) -> Var_sset.empty, RConst (path, rel, attr)
    | RInput ((path, _), name, attr) ->
      let my_out, new_attr =
        match List.assoc_opt name lets with
        | None -> Var_sset.empty, attr
        | Some x -> x
      in
      my_out, RInput (path, name, new_attr)
    | RLet ((path, Some constr1), name, e1, e2) ->
      let out1, e1 = up lets e1 in
      let fv1 = fv_of_relexpr e1 in
      let hashed_e1 = hash_delta constr1.out_groups (fv1, out1) e1 in
      let fv1 = fv_of_relexpr hashed_e1 in
      let out2, e2 = up ((name, (constr1.out_groups, fv1)) :: lets) e2 in
      out2, RLet (path, name, hashed_e1, e2)
    | RRename ((path, _), ren, e1) ->
      let my_in, e1 = up lets e1 in
      let my_out = rename_images ren my_in in
      let new_ren =
        List.rev (List.fold_left (fun l (dest, src) ->
          let hdest = hashed_var my_out dest in
          if List.mem_assoc hdest l then l else
            (hdest, hashed_var my_in src) :: l)
        [] ren) in
      my_out, RRename (path, new_ren, e1)
    | RDrop ((path, _), prj, e1) ->
      let my_in, e1 = up lets e1 in
      let my_out = groups_without (Var_set.of_list prj) my_in in
      let new_prj =
        List.rev (List.fold_left (fun l v ->
          let hv = hashed_var my_in v in
          if List.mem hv l then l else hv :: l)
        [] prj) in
      my_out, RDrop (path, new_prj, e1)
    | RExtend ((path, _), x, t, e1) ->
      let my_in, e1 = up lets e1 in
      let my_out = match t with
        | Predicate.Var y when Var_set.mem y (vars_of_groups my_in) ->
          Var_sset.add (Var_set.singleton x) my_in
        | _ -> my_in
      in
      let new_x = hashed_var my_out x in
      let new_t = match t with
        | Predicate.Var v -> Predicate.Var (hashed_var my_in v)
        | t -> t
      in
      my_out, RExtend (path, new_x, new_t, e1)
    | RSelect ((path, _), (pos, selop, t1, t2), e1) ->
      let my_out, e1 = up lets e1 in
      (* if a hashed variable is compared to a term, the latter will be hashed
         in extformula_of_relexpr *)
      let map_term = function
        | Predicate.Var v -> Predicate.Var (hashed_var my_out v)
        | t -> t
      in
      my_out, RSelect (path, (pos, selop, map_term t1, map_term t2), e1)
    | RJoin ((path, _), e1, pos, e2) ->
      let groups1, e1 = up lets e1 in
      let groups2, e2 = up lets e2 in
      let my_out = join_groups groups1 groups2 in
      let fv1 = fv_of_relexpr e1 in
      let fv2 = fv_of_relexpr e2 in
      let hashed_e1 = hash_delta my_out (fv1, groups1) e1 in
      let hashed_e2 = hash_delta my_out (fv2, groups2) e2 in
      my_out, RJoin (path, hashed_e1, pos, hashed_e2)
    | RUnion ((path, _), e1, e2) ->
      let groups1, e1 = up lets e1 in
      let groups2, e2 = up lets e2 in
      let my_out = join_groups groups1 groups2 in
      let fv1 = fv_of_relexpr e1 in
      let fv2 = fv_of_relexpr e2 in
      let hashed_e1 = hash_delta my_out (fv1, groups1) e1 in
      let hashed_e2 = hash_delta my_out (fv2, groups2) e2 in
      my_out, RUnion (path, hashed_e1, hashed_e2)
    | RAgg ((path, _), ty, y, op, x, gl, e1) ->
      let my_in, e1 = up lets e1 in
      let groupby = Var_set.of_list gl in
      let my_out = restrict_groups groupby my_in in
      let new_x = hashed_var my_in x in
      let new_gl =
        List.rev (List.fold_left (fun l v ->
          let hv = hashed_var my_in v in
          if List.mem hv l then l else hv :: l)
        [] gl) in
      my_out, RAgg (path, ty, y, op, new_x, new_gl, e1)
    | RNext ((path, _), dir, intv, e1) ->
      let my_out, e1 = up lets e1 in
      my_out, RNext (path, dir, intv, e1)
    | RUntil ((path, Some constr12), dir, intv, pos, e1, e2) ->
      let groups1, e1 = up lets e1 in
      let groups2, e2 = up lets e2 in
      let my_out = constr12.out_groups in
      let fv1 = fv_of_relexpr e1 in
      let fv2 = fv_of_relexpr e2 in
      let hashed_e1 = hash_delta my_out (fv1, groups1) e1 in
      let hashed_e2 = hash_delta my_out (fv2, groups2) e2 in
      my_out, RUntil (path, dir, intv, pos, hashed_e1, hashed_e2)
    | _ -> failwith "[Hashing.insert_hashing] internal error"
  in
  snd (up [] e)


(* Static error probability analysis *)

module Poly : sig
  type t
  val pp: formatter -> t -> unit
  val print: t -> unit
  val zero: t
  val const: int -> t
  val qconst: Q.t -> t
  val linear: int -> string -> t
  val ( + ): t -> t -> t
  val ( - ): t -> t -> t
  val ( * ): t -> t -> t
end = struct
  module Powers = Map.Make(struct
    type t = string
    let compare = Stdlib.compare
  end)
  module Coeffs = Map.Make(struct
    type t = int Powers.t
    let compare = Powers.compare (fun x y -> -(Stdlib.compare x y))
  end)

  type t = Q.t Coeffs.t

  let pp ppf x =
    if Coeffs.is_empty x then pp_print_string ppf "0"
    else
      let printp var exp =
        if exp = 0 then () else begin
          pp_print_space ppf ();
          pp_print_string ppf "* ";
          pp_print_string ppf var;
          if exp <> 1 then begin
            pp_print_string ppf "^";
            pp_print_int ppf exp
          end
        end
      in
      let printps m = Powers.iter printp m in
      let first = ref true in
      let print1 m (c:Q.t) =
        let c' =
          if !first then
            (first := false; c)
          else begin
            pp_print_space ppf ();
            if Q.sign c >= 0 then
              (pp_print_string ppf "+ "; c)
            else
              (pp_print_string ppf "- "; Q.neg c)
          end
        in
        pp_open_box ppf 2;
        Q.pp_print ppf c';
        printps m;
        pp_close_box ppf ()
      in
      pp_open_hvbox ppf 0;
      Coeffs.iter print1 x;
      pp_close_box ppf ()

  let print x = pp std_formatter x

  let zero = Coeffs.empty
  let const x = Coeffs.singleton (Powers.empty) (Q.of_int x)
  let qconst x = Coeffs.singleton (Powers.empty) x
  let linear x var = Coeffs.singleton (Powers.singleton var 1) (Q.of_int x)

  let add x y =
    let add1 m c y = Coeffs.update m (function
      | None -> Some c
      | Some c' -> Some (Q.add c c')) y
    in
    Coeffs.fold add1 x y

  let mult x y =
    let mult1 m c z =
      let addp1 var exp = Powers.update var (function
        | None -> Some exp
        | Some exp' -> Some (exp + exp'))
      in
      let addp m' = Powers.fold addp1 m m' in
      let mult11 m' c' = Coeffs.add (addp m') (Q.mul c c') in
      let cmy = Coeffs.fold mult11 y zero in
      add cmy z
    in
    Coeffs.fold mult1 x zero

  let ( + ) = add
  let ( - ) x y = add x (mult (qconst Q.minus_one) y)
  let ( * ) = mult
end

let interval_size (l, u) =
  match l with
  | OBnd a
  | CBnd a -> (match u with
      | OBnd b -> Some (int_of_float (ceil (b -. a)))
      | CBnd b -> Some (int_of_float (ceil (b -. a +. 1.)))
      | Inf -> None)
  | Inf -> Some 0

let upper_bound (_l, u) =
  match u with
  | OBnd b -> Some (int_of_float (ceil b))
  | CBnd b -> Some (int_of_float (ceil b) + 1)
  | Inf -> None

type analysis = {
  size: Poly.t;
  fp: Poly.t;
  fn: Poly.t;
}

let zero_analysis = {size = Poly.zero; fp = Poly.zero; fn = Poly.zero}

let one_half = Q.make Z.one (Z.of_int 2)

let analyze_error e =
  let rec up lets = function
    | RConst (_path, rel, _attr) ->
      let size = Poly.const (cardinal rel) in
      { zero_analysis with size = size }
    | RInput (_path, name, _attr) ->
      (match List.assoc_opt name lets with
      | None -> { zero_analysis with size = Poly.linear 1 ("input." ^ name) }
      | Some a -> a)
    | RLet (_path, name, e1, e2) ->
      let a1 = up lets e1 in
      up ((name, a1) :: lets) e2
    | RRename (_path, _ren, e1) ->
      up lets e1
    | RDrop (_path, _prj, e1) as e ->
      let a1 = up lets e1 in
      let size = if fv_of_relexpr e = [] then Poly.const 1 else a1.size in
      { a1 with size = size }
    | RExtend (_path, _x, _t, e1) ->
      up lets e1
    | RSelect (_path, (pos, selop, t1, t2), e1) ->
      let a1 = up lets e1 in
      let approx = match t1, t2 with
        | Predicate.Var v, _ when is_hashed v -> true
        | _, Predicate.Var v when is_hashed v -> true
        | _ -> false
      in
      if approx then
        (match pos with
        | P -> { a1 with fp = Poly.(a1.fp + a1.size) }
        | N -> { a1 with fn = Poly.(a1.fn + a1.size) })
      else a1
    | RJoin (path, e1, pos, e2) ->
      let a1 = up lets e1 in
      let a2 = up lets e2 in
      let size = Poly.linear 1 ("size" ^ path) in
      let base_fp, base_fn =
        match pos with
        | P -> Poly.(a1.fp + a2.fp, a1.fn + a2.fn)
        | N -> Poly.(a1.fp + a2.fn, a1.fn + a2.fp)
      in
      let key = Var_set.inter (fv_set_of_relexpr e1) (fv_set_of_relexpr e2) in
      if Var_set.is_empty key || not (Var_set.exists is_hashed key) then
        {size = size; fp = base_fp; fn = base_fn}
      else
        let extra_err = Poly.(a1.size * a2.size) in
        (match pos with
        | P -> {size = size; fp = Poly.(base_fp + extra_err); fn = base_fn}
        | N -> {size = size; fp = base_fp; fn = Poly.(base_fn + extra_err)})
    | RUnion (_path, e1, e2) ->
      let a1 = up lets e1 in
      let a2 = up lets e2 in
      Poly.{
        size = a1.size + a2.size;
        fp = a1.fp + a2.fp;
        fn = a1.fn + a2.fn;
      }
    | RAgg (path, _ty, _y, op, x, gl, e1) ->
      let a1 = up lets e1 in
      let size = Poly.linear 1 ("size" ^ path) in
      let groupby_hashed = List.exists is_hashed gl in
      let any_hashed = List.exists is_hashed (fv_of_relexpr e1) in
      let extra_err = match op with
        | Min | Max when groupby_hashed ->
          Poly.(qconst one_half * (a1.size * a1.size - a1.size))
        | Sum | Avg | Med | Cnt when any_hashed ->
          Poly.(qconst one_half * (a1.size * a1.size - a1.size))
        | _ -> Poly.zero
      in
      let err = Poly.(a1.fp + a1.fn + extra_err) in
      {size = size; fp = err; fn = err}
    | RNext (_path, _dir, _intv, e1) ->
      up lets e1
    | RHash (_path, _hmap, e1) ->
      up lets e1
    | RUntil (path, _dir, intv, pos, e1, e2) ->
      let a1 = up lets e1 in
      let a2 = up lets e2 in
      let size = Poly.linear 1 ("size" ^ path) in
      let rate = Poly.linear 1 "rate" in
      let base_fp, base_fn, extra_err =
        match upper_bound intv with
        | None -> Poly.zero, Poly.zero, Poly.zero
        | Some u ->
          let isz = Poly.const (Option.get (interval_size intv)) in
          let f1 = Poly.(rate * const u + const (-1)) in
          let f2 = Poly.(rate * isz) in
          let f3 = Poly.(rate * (const u - isz)) in
          match pos with
          | P -> Poly.(f1 * a1.fp + f2 * a2.fp, f1 * a1.fn + f2 * a2.fn,
              f2 * a1.size * a2.size)
          | N -> Poly.(f1 * a1.fn + f2 * a2.fp, f1 * a1.fp + f2 * a2.fn,
              f2 * (f3 + qconst one_half * (f2 + const 1)) * a1.size * a2.size)
      in
      let key = fv_of_relexpr e1 in
      if key = [] || not (List.exists is_hashed key) then
        {size = size; fp = base_fp; fn = base_fn}
      else
        (match pos with
        | P -> {size = size; fp = Poly.(base_fp + extra_err); fn = base_fn}
        | N -> {size = size; fp = base_fp; fn = Poly.(base_fn + extra_err)})
  in
  up [] e


(* Translation from relation algebra to extformula (initial monitor state) *)

let hash_seed = ref 0

type cst = Predicate.cst

module Multihash = struct
  type key = {
    nbits: int;
    mask: int;
    clkey: Clhash.key;
    vkeys: int array;
  }

  let init_key nbits nvars =
    assert (1 <= nbits && nbits <= Sys.int_size);
    let mask = Int.shift_left 1 nbits - 1 in
    let seed1 = Random.int64 Int64.max_int in
    let seed2 = Random.int64 Int64.max_int in
    let clkey = Clhash.random_key seed1 seed2 in
    let mk_vkey _ = Int64.to_int (Random.int64 Int64.max_int) in
    let vkeys = Array.init nvars mk_vkey in
    {nbits; mask; clkey; vkeys}

  let single {clkey; mask; _} cst =
    let hash = match cst with
      | Predicate.Int x -> Clhash.hash_int clkey x
      | Predicate.Str x -> Clhash.hash_string clkey x
      | Predicate.Float x -> Clhash.hash_float clkey x
      | _ -> failwith "[Hashing.Multihash.single] not implemented"
    in
    Predicate.Int (Int.logand hash mask)

  let lift {nbits; vkeys; _} i = function
    | Predicate.Int x -> Predicate.Int (Clhash.gf_mul nbits vkeys.(i) x)
    | _ -> failwith "[Hashing.Multihash.lift] internal error"

  let combine x y = match x, y with
    | (Predicate.Int x, Predicate.Int y) -> Predicate.Int (Int.logxor x y)
    | _ -> failwith "[Hashing.Multihash.combine] internal error"
end

type mergeinput =
  | Exact of int * int
  | Single of int * int
  | Merged of int

type valspec =
  | Copy of int
  | Hash1 of int
  | Merge of mergeinput list

let hash_tuple key spec t =
  let hash1 i = Multihash.single key (Tuple.get_at_pos t i) in
  let multihash_of = function
    | Exact (j, i) ->
      let h = Multihash.single key (Tuple.get_at_pos t i) in
      Multihash.lift key j h
    | Single (j, i) -> Multihash.lift key j (Tuple.get_at_pos t i)
    | Merged i -> Tuple.get_at_pos t i
  in
  let combine h x = Multihash.combine h (multihash_of x) in
  let rec go acc = function
    | [] -> Tuple.make_tuple acc
    | Copy i :: sl -> go (Tuple.get_at_pos t i :: acc) sl
    | Hash1 i :: sl -> go (hash1 i :: acc) sl
    | Merge [] :: _ -> failwith "[Hashing.hash_tuple] internal error"
    | Merge (x0 :: l) :: sl ->
      go (List.fold_left combine (multihash_of x0) l :: acc) sl
  in
  go [] spec

let mk_hash_spec ~enable index_of_var in_attrs hmap =
  let mergeinput_of v =
    let i = Misc.get_pos v in_attrs in
    if is_hashed v then
      if Var_set.cardinal (set_of_hash_var v) = 1 then
        Single (index_of_var (unhash1 v), i)
      else
        Merged i
    else
      Exact (index_of_var v, i)
  in
  let spec_of (v, l) =
    match l with
    | [] -> Copy (Misc.get_pos v in_attrs)
    | [x] ->
      assert (not (is_hashed x));
      let i = Misc.get_pos x in_attrs in
      if enable then Hash1 i else Copy i
    | l ->
      assert enable;
      Merge (List.map mergeinput_of l)
  in
  List.rev (List.map spec_of hmap)

let hash_term ~enable key tm =
  if enable then
    (match tm with
    | Predicate.Var _ -> tm
    | _ -> Predicate.Cst (Multihash.single key (Predicate.eval_gterm tm)))
  else tm

let is_id_permutation n l =
  let rec check x = function
    | [] -> x = n
    | y::l -> x = y && check (x+1) l
  in
  check 0 l

let hash_pos l =
  let rec go s n = function
    | [] -> s
    | x::l ->
      let s' = if is_hashed x then Relation.Int_set.add n s else s in
      go s' (n+1) l
  in
  go Relation.Int_set.empty 0 l

type mode = Nothing | Single of int | Merged of int

let normalize_bits b = if 1 <= b && b <= Sys.int_size then b else Sys.int_size

let collision_prob_of_mode = function
  | Nothing -> 0.
  | Single b -> Clhash.collision_prob (normalize_bits b)
  | Merged b ->
    let b = normalize_bits b in
    min 1. (Clhash.collision_prob b +. Float.pow 2. (~-. (float_of_int b)))

let extformula_of_relexpr mode e =
  Relation.collision_prob := collision_prob_of_mode mode;
  let enable, bits =
    match mode with
    | Nothing -> false, Sys.int_size
    | Single b | Merged b -> true, normalize_bits b
  in
  let base_vars = Var_set.elements (Var_set.filter (fun v -> not (is_hashed v))
    (vars_of_relexpr e)) in
  let index_of_var v = Misc.get_pos v base_vars in
  let key = Multihash.init_key bits (List.length base_vars) in
  let neval = Neval.create () in
  let neval0 = Neval.get_last neval in
  let no_tuple_map_expected op = function
    | None -> ()
    | Some _ -> failwith ("[Hashing.extformula_of_relexpr] internal error: " ^
        "no RHash expected around " ^ op)
  in
  let rec translate tuple_map = function
    | RConst (_, rel, _) ->
      let rel = match tuple_map with
        | None -> rel
        | Some f -> Relation.map f rel
      in
      Extformula.ERel rel
    | RInput (_, name, attr) ->
      let pred = (name, List.length attr,
        List.map (fun x -> Predicate.Var x) attr) in
      let comp = match tuple_map with
        | None -> (fun rel -> rel)
        | Some f -> Relation.map f
      in
      Extformula.EPred (pred, comp, Queue.create ())
    | RLet (_, name, e1, e2) ->
      let attr1 = fv_of_relexpr e1 in
      let f1 = translate None e1 in
      let f2 = translate tuple_map e2 in
      let pred = (name, List.length attr1,
        List.map (fun x -> Predicate.Var x) attr1) in
      Extformula.ELet (pred, (fun rel -> rel), f1, f2, {llast = neval0})
    | RRename (_, ren, e1) as e0 ->
      let attr1 = fv_of_relexpr e1 in
      let new_pos = List.map (fun (_,x) -> Misc.get_pos x attr1) ren in
      if is_id_permutation (List.length attr1) new_pos then
        translate tuple_map e1
      else
        let f1 = translate None e1 in
        let hashed = hash_pos (fv_of_relexpr e0) in
        let comp = Relation.reorder hashed tuple_map new_pos in
        Extformula.EExists (comp, f1)
    | RDrop (_, prj, e1) as e0 ->
      let attr1 = fv_of_relexpr e1 in
      let pos = List.filter_map (fun v ->
        try Some (Misc.get_pos v attr1)
        with Not_found -> None) prj
      in
      if pos = [] then translate tuple_map e1
      else
        let pos = List.sort Stdlib.compare pos in
        let f1 = translate None e1 in
        let hashed = hash_pos (fv_of_relexpr e0) in
        let comp = Relation.project_away hashed tuple_map pos in
        Extformula.EExists (comp, f1)
    | RExtend (_, x, t, e1) as e0 ->
      let attr1 = fv_of_relexpr e1 in
      let f1 = translate None e1 in
      let f2 = Extformula.ERel Relation.empty in
      let pos_term = Tuple.get_pos_term attr1 t in
      let process_tuple t =
        Tuple.add_last t (Tuple.eval_term_on_tuple t pos_term) in
      let process_tuple = match tuple_map with
        | None -> Relation.{fn = process_tuple;
                            hashed = hash_pos (fv_of_relexpr e0)}
        | Some f -> Relation.{fn = (fun t -> f.fn (process_tuple t));
                              hashed = f.hashed}
      in
      let comp rel1 _rel2 = Relation.map process_tuple rel1 in
      Extformula.EAnd (comp, f1, f2, {arel = None})
    | RSelect (_, (pos, selop, t1, t2), e1) ->
      let attr1 = fv_of_relexpr e1 in
      let f1 = translate None e1 in
      let f2 = Extformula.ERel Relation.empty in
      let is_hashed_var = function
        | Predicate.Var x -> is_hashed x
        | _ -> false
      in
      let t1, t2, hint =
        if is_hashed_var t1 || is_hashed_var t2 then
          (hash_term ~enable key t1, hash_term ~enable key t2,
            match pos with P -> Relation.HashEq | N -> Relation.HashNotEq)
        else
          (t1, t2, Relation.Exact)
      in
      let sel_f = match selop with
        | SelectEq -> Equal (t1, t2)
        | SelectLess -> Less (t1, t2)
        | SelectLessEq -> LessEq (t1, t2)
      in
      let sel_f = match pos with P -> sel_f | N -> Neg sel_f in
      let filter_cond = Tuple.get_filter attr1 sel_f in
      let comp rel1 _rel2 = Relation.filter hint tuple_map filter_cond rel1 in
      Extformula.EAnd (comp, f1, f2, {arel = None})
    | RJoin (_, e1, pos, e2) as e0 ->
      let attr1 = fv_of_relexpr e1 in
      let attr2 = fv_of_relexpr e2 in
      let f1 = translate None e1 in
      let f2 = translate None e2 in
      let hashed = hash_pos (fv_of_relexpr e0) in
      let comp =
        match pos with
        | P ->
          begin
            let matches1 = Table.get_matches attr1 attr2 in
            let matches2 = Table.get_matches attr2 attr1 in
            if attr1 = attr2 then
              Relation.inter tuple_map
            else if Misc.subset attr1 attr2 then
              Relation.natural_join_sc1 hashed tuple_map matches2
            else if Misc.subset attr2 attr1 then
              Relation.natural_join_sc2 tuple_map matches1
            else
              Relation.natural_join hashed tuple_map matches1 matches2
          end
        | N ->
          begin
            if attr1 = attr2 then
              Relation.diff tuple_map
            else
              begin
                assert(Misc.subset attr2 attr1);
                let posl = List.map (fun v -> Misc.get_pos v attr1) attr2 in
                Relation.minus tuple_map posl
              end
          end
      in
      Extformula.EAnd (comp, f1, f2, {arel = None})
    | RUnion (_, e1, e2) ->
      no_tuple_map_expected "RUnion" tuple_map;
      let attr1 = fv_of_relexpr e1 in
      let attr2 = fv_of_relexpr e2 in
      let f1 = translate None e1 in
      let f2 = translate None e2 in
      let comp =
        if attr1 = attr2 then
          Relation.union
        else
          let matches = Table.get_matches attr2 attr1 in
          let new_pos = List.map snd matches in
          let hashed1 = hash_pos attr1 in
          (* first reorder rel2 *)
          (fun rel1 rel2 ->
             let rel2' = Relation.reorder hashed1 None new_pos rel2 in
             Relation.union rel1 rel2'
          )
      in
      Extformula.EOr (comp, f1, f2, {arel = None})

    | RAgg (_, t_y, y, op, x, gl, e1) as e0 ->
      let open Extformula in
      let open Predicate in
      let t_y = match t_y with TCst a -> a | _ -> failwith "Internal error" in
      let default = MFOTL.aggreg_default_value op t_y in
      let tuple_map' =
        match tuple_map with
        | None -> Relation.{fn = (fun x -> x);
                            hashed = hash_pos (fv_of_relexpr e0)}
        | Some f -> f
      in
      let e1_as_once = match e1 with
        | RUntil (_, Past, intv, P, e11, e12) ->
          (match e11 with
          | RConst (_, rel, []) when cardinal rel = 1 ->
            Some (intv, e12)
          | _ -> None)
        | _ -> None
      in
      (match e1_as_once with
      | Some (intv, e12) ->
        (* op1 = Once e12 *)
        let attr1 = fv_of_relexpr e12 in
        let f1 = translate None e12 in
        let posx = Misc.get_pos x attr1 in
        let posG = List.map (fun z -> Misc.get_pos z attr1) gl in
        let state =
          match op with
          | Cnt -> Aggreg.cnt_once tuple_map' default intv 0 posG
          | Min -> Aggreg.min_once tuple_map' default intv 0 posx posG
          | Max -> Aggreg.max_once tuple_map' default intv 0 posx posG
          | Sum -> Aggreg.sum_once tuple_map' default intv 0 posx posG
          | Avg -> Aggreg.avg_once tuple_map' default intv 0 posx posG
          | Med -> Aggreg.med_once tuple_map' default intv 0 posx posG
        in
        EAggOnce ({op; default}, state, f1)

      | None ->
        (* op1 <> Once ... *)
        let attr1 = fv_of_relexpr e1 in
        let f1 = translate None e1 in
        let posx = Misc.get_pos x attr1 in
        let posG = List.map (fun z -> Misc.get_pos z attr1) gl in
        let comp =
          match op with
          | Cnt -> Aggreg.cnt tuple_map' default 0 posG
          | Sum -> Aggreg.sum tuple_map' default 0 posx posG
          | Min -> Aggreg.min tuple_map' default 0 posx posG
          | Max -> Aggreg.max tuple_map' default 0 posx posG
          | Avg -> Aggreg.avg tuple_map' default 0 posx posG
          | Med -> Aggreg.med tuple_map' default 0 posx posG
        in
        EAggreg ({op; default}, comp, f1)
      )

    | RNext (_, dir, intv, e1) ->
      no_tuple_map_expected "RNext" tuple_map;
      let f1 = translate None e1 in
      (match dir with
      | Past -> Extformula.EPrev (intv, f1, {plast = neval0})
      | Future -> Extformula.ENext (intv, f1, {init = true}))

    | RUntil (_, dir, intv, pos, e1, e2) ->
      (match e1 with
      | RConst (_, rel, []) when pos = P && cardinal rel = 1 ->
        (* Once/Eventually *)
        no_tuple_map_expected "RUntil" tuple_map;
        let f2 = translate None e2 in
        (match dir with
        | Past ->
          if snd intv = Inf then
            Extformula.(EOnceA (intv, f2, {ores = Relation.empty;
              oaauxrels = Mqueue.create()}))
          else if fst intv = CBnd MFOTL.ts_null then
            Extformula.(EOnceZ (intv, f2, {oztree = LNode {l = -1;
                r = -1;
                res = Some (Relation.empty)};
              ozlast = Dllist.void;
              ozauxrels = Dllist.empty()}))
          else
            Extformula.(EOnce (intv, f2, {otree = LNode {l = MFOTL.ts_invalid;
                r = MFOTL.ts_invalid;
                res = Some (Relation.empty)};
              olast = Dllist.void;
              oauxrels = Dllist.empty()}))
        | Future ->
          if fst intv = CBnd MFOTL.ts_null then
            EEventuallyZ (intv,f2,{eztree = LNode {l = -1;
                                                   r = -1;
                                                   res = Some (Relation.empty)};
                                   ezlast = Dllist.void;
                                   ezlastev = neval0;
                                   ezauxrels = Dllist.empty()})
          else
            EEventually (intv,f2,{etree = LNode {l = MFOTL.ts_invalid;
                                                 r = MFOTL.ts_invalid;
                                                 res = Some (Relation.empty)};
                                  elast = Dllist.void;
                                  elastev = neval0;
                                  eauxrels = Dllist.empty()}))
      | _ ->
        let attr1 = fv_of_relexpr e1 in
        let attr2 = fv_of_relexpr e2 in
        let f1 = translate None e1 in
        let f2 = translate None e2 in
        let comp1 = match tuple_map with
          | None -> (fun rel -> rel)
          | Some f -> Relation.map f
        in
        (match dir with
        | Past ->
          let comp =
            match pos with
            | P ->
              let matches2 = Table.get_matches attr2 attr1 in
              fun relj rel1 -> Relation.natural_join_sc2 None matches2 relj rel1
            | N ->
              let posl = List.map (fun v -> Misc.get_pos v attr2) attr1 in
              assert(Misc.subset attr1 attr2);
              fun relj rel1 -> Relation.minus None posl relj rel1
          in
          if snd intv = Inf then
            let inf = Extformula.{sres = Relation.empty; sarel2 = None;
              saauxrels = Mqueue.create()} in
            Extformula.ESinceA (comp1,comp,intv,f1,f2,inf)
          else
            let inf = Extformula.{srel2 = None; sauxrels = Mqueue.create()} in
            Extformula.ESince (comp1,comp,intv,f1,f2,inf)
        | Future ->
          (match pos with
          | P ->
            let matches2 = Table.get_matches attr2 attr1 in
            let comp = Relation.natural_join_sc2 None matches2 in
            let inf = Extformula.{
              ulast = neval0;
              ufirst = false;
              ures = Relation.empty;
              urel2 = None;
              raux = Sj.empty();
              saux = Sk.empty();
              uerr1 = Sk.empty();
              uerr2 = Sj.empty()}
            in
            Extformula.EUntil (comp1,comp,intv,f1,f2,inf)
          | N ->
            let comp =
              let posl = List.map (fun v -> Misc.get_pos v attr2) attr1 in
              assert(Misc.subset attr1 attr2);
              fun relj rel1 -> Relation.minus None posl relj rel1
            in
            let inf = Extformula.{
              last1 = neval0;
              last2 = neval0;
              listrel1 = Dllist.empty();
              listrel2 = Dllist.empty()}
            in
            Extformula.ENUntil (comp1,comp,false,intv,f1,f2,inf))
        )
      )

    | RHash (_, hmap, e1) as e0 ->
      no_tuple_map_expected "RHash" tuple_map;
      let attr1 = fv_of_relexpr e1 in
      let spec = mk_hash_spec ~enable index_of_var attr1 hmap in
      let tuple_map' = Relation.{fn = hash_tuple key spec;
                                 hashed = hash_pos (fv_of_relexpr e0)}
      in
      translate (Some tuple_map') e1
  in
  (translate None e, fv_of_relexpr e, neval, ref neval0)

let hashed_relexpr_of_mfotl mode f =
  let relexpr = relexpr_of_mfotl f in
  let merge = match mode with Merged _ -> true | _ -> false in
  let constr_relexpr = compute_constr ~merge relexpr in
  insert_hashing constr_relexpr

let analyze mode f =
  print_string "Analyzing formula:";
  print_newline ();
  MFOTL.print_formula "" f;
  print_newline ();
  print_newline ();
  let hashed_relexpr =
    match mode with
    | None -> relexpr_of_mfotl f
    | Some m -> hashed_relexpr_of_mfotl m f
  in
  print_string "Rewritten temporal-relational expression:";
  print_newline ();
  print_relexpr hashed_relexpr;
  print_newline ();
  match mode with
  | None -> print_string "(hashing bypassed)"; print_newline ()
  | Some Nothing -> print_string "(hash function disabled)"; print_newline ()
  | Some m ->
    let a = analyze_error hashed_relexpr in
    let cpr = collision_prob_of_mode m in
    printf "@[<v>@[<hv 8>FP <= %.4e *@ (" cpr;
    Poly.(print a.fp);
    printf ")@]@,@[<hv 8>FN <= %.4e *@ (" cpr;
    Poly.(print a.fn);
    printf ")@]@]@."

let convert mode f =
  let hashed_relexpr = hashed_relexpr_of_mfotl mode f in
  extformula_of_relexpr mode hashed_relexpr
