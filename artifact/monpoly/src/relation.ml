(*
 * This file is part of MONPOLY.
 *
 * Copyright (C) 2011 Nokia Corporation and/or its subsidiary(-ies).
 * Contact:  Nokia Corporation (Debmalya Biswas: debmalya.biswas@nokia.com)
 *
 * Copyright (C) 2012 ETH Zurich.
 * Contact:  ETH Zurich (Eugen Zalinescu: eugen.zalinescu@inf.ethz.ch)
 *
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, version 2.1 of the
 * License.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see
 * http://www.gnu.org/licenses/lgpl-2.1.html.
 *
 * As a special exception to the GNU Lesser General Public License,
 * you may link, statically or dynamically, a "work that uses the
 * Library" with a publicly distributed version of the Library to
 * produce an executable file containing portions of the Library, and
 * distribute that executable file under terms of your choice, without
 * any of the additional requirements listed in clause 6 of the GNU
 * Lesser General Public License. By "a publicly distributed version
 * of the Library", we mean either the unmodified Library as
 * distributed by Nokia, or a modified version of the Library that is
 * distributed under the conditions defined in clause 3 of the GNU
 * Lesser General Public License. This exception does not however
 * invalidate any other reasons why the executable file might be
 * covered by the GNU Lesser General Public License.
 *)



open Misc
open Tuple
open Predicate

(*DEBUG*)
let prerr_list p l =
  prerr_string "(";
  (match l with
  | [] -> ()
  | x::l -> p x; List.iter (fun x -> prerr_string ","; p x) l);
  prerr_string ")"

let collision_prob = ref 1.

module Tuple_set = Set.Make (struct
  type t = tuple
  let compare = Tuple.compare
end)

module Int_set = Set.Make (struct
  type t = int
  let compare = Stdlib.compare
end)

let no_hash = Int_set.empty

type relation = {
  set: Tuple_set.t;
  rhashed: Int_set.t;
  bound: Z.t option;
  fp: Z.t;
  fn: Z.t;
}

type err_info = {
  ihashed: Int_set.t;
  ibound: Z.t;
  ifp: Z.t;
  ifn: Z.t;
}


(*** printing functions ***)

let print_set str set =
  print_string str;
  Misc.print_list Tuple.print_tuple (Tuple_set.elements set)

let print_set4 str set =
  print_string str;
  Misc.print_list4 Tuple.print_tuple (Tuple_set.elements set)

let print_setn str set =
  print_set str set;
  print_newline()

let print_rel str rel = print_set str rel.set
let print_rel4 str rel = print_set4 str rel.set
let print_reln str rel = print_setn str rel.set


let print_bigrel rel =
  let rel' = Tuple_set.elements rel.set in
  Misc.print_list3 Tuple.print_tuple rel'

let print_orel = function
  | None -> print_string "N"
  | Some rel -> print_rel "S" rel


(********************************)

let to_set rel = rel.set

let is_empty rel = Tuple_set.is_empty rel.set
let is_certainly_empty rel = (rel.fn = Z.zero) && is_empty rel

let false_pos rel = min 1. (!collision_prob *. Z.to_float rel.fp)
let false_neg rel = min 1. (!collision_prob *. Z.to_float rel.fn)

let size_is_upper set hashed fn = (fn = Z.zero) &&
  (Tuple_set.is_empty set || Int_set.is_empty hashed)

let size_bound rel =
  match rel.bound with
  | None -> Z.of_int (Tuple_set.cardinal rel.set)
  | Some x -> x

let err_info_of rel =
  {ihashed = rel.rhashed; ibound = size_bound rel; ifp = rel.fp; ifn = rel.fn}

let with_err_info {ihashed; ibound; ifp; ifn} rel =
  let bound = if size_is_upper rel.set ihashed ifn then None
    else Some ibound in
  {set = rel.set; rhashed = ihashed; bound; fp = ifp; fn = ifn}

let empty = {
  set = Tuple_set.empty;
  rhashed = Int_set.empty;
  bound = None;
  fp = Z.zero;
  fn = Z.zero;
}

let empty_err_info = {
  ihashed = Int_set.empty;
  ibound = Z.zero;
  ifp = Z.zero;
  ifn = Z.zero;
}

let singleton x = {
  set = Tuple_set.singleton x;
  rhashed = Int_set.empty;
  bound = None;
  fp = Z.zero;
  fn = Z.zero;
}

let make_relation l = {
  set = Tuple_set.of_list l;
  rhashed = Int_set.empty;
  bound = None;
  fp = Z.zero;
  fn = Z.zero;
}

let require_exact rel =
  if rel.fp <> Z.zero || rel.fn <> Z.zero || not (Int_set.is_empty rel.rhashed) then
    failwith "exact relation required"


(* Note: [proj] \cup [sel] = all positions and [proj] \cap [sel] = \emptyset
   special case when sel=[]? yes, then selectp is the identity function
   special case when proj=[]? no

   The approach below seems useless, because we have to iterate anyhow
   through all tuples and transform them individually (unless sel=[]).
   And then positions do not help us, see function [Tuple.satisfiesp].
*)
let no_constraints tlist =
  let rec iter vars = function
    | [] -> true
    | (Var x) :: tlist ->
      if List.mem x vars then
        false (* there are constraints *)
      else (* new variable, we record its position *)
        iter (x :: vars) tlist

    | _ :: tlist -> false  (* there are constraints *)
  in
  iter [] tlist


(* Given a predicate [f]=[p(t_1,\dots,t_n)], [eval_pred] returns a
   function from relations to relations; this function transforms
   [|p(x_1,\dots,x_n)|] into [|p(t_1,\dots,t_n)|]
*)
let eval_pred p =
  let tlist = Predicate.get_args p in
  if no_constraints tlist then
    fun rel -> rel
  else
    fun rel ->
      require_exact rel;
      let res = ref Tuple_set.empty in
      Tuple_set.iter
        (fun t ->
           let b, t' = Tuple.satisfiesp tlist t in
           if b then
             res := Tuple_set.add t' !res;
        ) rel.set;
      {set = !res; rhashed = Int_set.empty; bound = None; fp = Z.zero; fn = Z.zero}

(*** rigid predicates ***)

let trel = make_relation [Tuple.make_tuple []]
let frel = empty

let eval_equal t1 t2 =
  match t1,t2 with
  | Var x, Cst c
  | Cst c, Var x -> make_relation [Tuple.make_tuple [c]]
  | Cst c, Cst c' when c = c' -> trel
  | Cst c, Cst c' -> frel
  | _ -> failwith "[Relation.eval_equal] (x=y)"

let eval_not_equal t1 t2 =
  match t1,t2 with
  | Var x, Var y when x = y -> frel
  | Cst c, Cst c' when c = c' -> frel
  | Cst c, Cst c' -> trel
  | _ -> failwith "[Relation.eval_not_equal] (x <> y)"


type rel_map = {
  fn: tuple -> tuple;
  hashed: Int_set.t;
}

let no_hash_map = {fn = (fun x -> x); hashed = Int_set.empty}

let map f rel =
  let set = Tuple_set.map f.fn rel.set in
  let bound = if size_is_upper set f.hashed rel.fn then None
    else Some (size_bound rel) in
  {set; rhashed = f.hashed; bound; fp = rel.fp; fn = rel.fn}

let negate rel =
  let set =
    if Tuple_set.is_empty rel.set then
      Tuple_set.singleton (Tuple.make_tuple [])
    else
      Tuple_set.empty
  in
  let bound = if rel.fp = Z.zero then None else Some Z.one in
  {set; rhashed = rel.rhashed; bound; fp = rel.fn; fn = rel.fp}

let union (*f*) rel1 rel2 =
  let set = Tuple_set.union rel1.set rel2.set in
  let set, rhashed =
    set, Int_set.union rel1.rhashed rel2.rhashed
    (*match f with
    | None -> set, rel1.rhashed
    | Some f -> Tuple_set.map f.fn set, f.hashed*)
  in
  let fp = Z.(rel1.fp + rel2.fp) in
  let fn = Z.(rel1.fn + rel2.fn) in
  let bound = if size_is_upper set rhashed fn then None
    else Some Z.(size_bound rel1 + size_bound rel2) in
  {set; rhashed; bound; fp; fn}

let union_err_info i1 i2 =
  {
    ihashed = Int_set.union i1.ihashed i2.ihashed;
    ibound = Z.(i1.ibound + i2.ibound);
    ifp = Z.(i1.ifp + i2.ifp);
    ifn = Z.(i1.ifn + i2.ifn);
  }

let inter f rel1 rel2 =
  let err = if Int_set.is_empty rel1.rhashed then Z.zero
    else Z.(size_bound rel1 * size_bound rel2) in
  let fp = Z.(rel1.fp + rel2.fp + err) in
  let fn = Z.(rel1.fn + rel2.fn) in
  let set, rhashed =
    match f with
    | None -> Tuple_set.inter rel1.set rel2.set, rel1.rhashed
    | Some f ->
      let card1 = Tuple_set.cardinal rel1.set in
      let card2 = Tuple_set.cardinal rel2.set in
      let process_tuple rel t =
        if Tuple_set.mem t rel then Some (f.fn t) else None in
      (if card1 < card2 then
        Tuple_set.filter_map (process_tuple rel2.set) rel1.set
      else
        Tuple_set.filter_map (process_tuple rel1.set) rel2.set),
      f.hashed
  in
  let bound = if size_is_upper set rhashed fn then None
    else Some (min (size_bound rel1) (size_bound rel2)) in
  {set; rhashed; bound; fp; fn}

let inter_err_info i1 i2 =
  let ihashed = i1.ihashed in
  let ibound = min i1.ibound i2.ibound in
  let err = if Int_set.is_empty i1.ihashed then Z.zero
    else Z.(i1.ibound * i2.ibound) in
  let ifp = Z.(i1.ifp + i2.ifp + err) in
  let ifn = Z.(i1.ifn + i2.ifn) in
  {ihashed; ibound; ifp; ifn}

let diff f rel1 rel2 =
  let err = if Int_set.is_empty rel1.rhashed then Z.zero
    else Z.(size_bound rel1 * size_bound rel2) in
  let fp = Z.(rel1.fp + rel2.fn) in
  let fn = Z.(rel1.fn + rel2.fp + err) in
  let set, rhashed =
    match f with
    | None -> Tuple_set.diff rel1.set rel2.set, rel1.rhashed
    | Some f ->
      let process_tuple t =
        if Tuple_set.mem t rel2.set then None else Some (f.fn t) in
      Tuple_set.filter_map process_tuple rel1.set, f.hashed
  in
  let bound = if size_is_upper set rhashed fn then None
    else Some (size_bound rel1) in
  {set; rhashed; bound; fp; fn}

(** [matches] gives the columns which should match in the two
    relations in form of a list of tuples [(pos2,pos1)]: column [pos2] in
    [rel2] should match column [pos1] in [rel1] *)
let natural_join hashed0 f matches1 matches2 rel1 rel2 =
  let hashed_key = Int_set.inter hashed0 (Int_set.of_list (List.map fst matches2)) in
  let err = if Int_set.is_empty hashed_key then Z.zero
    else Z.(size_bound rel1 * size_bound rel2) in
  let fp = Z.(rel1.fp + rel2.fp + err) in
  let fn = Z.(rel1.fn + rel2.fn) in
  let rhashed =
    match f with
    | None -> hashed0
    | Some f -> f.hashed
  in
  let joinrel = ref Tuple_set.empty in
  let process_rel_tuple join_fun matches rel2 t1 =
    (* For each tuple in [rel1] we compute the columns (i.e. positions)
       in rel2 for which there should be matching values and the values
       tuples in rel2 should have at these columns.

       For instance, for [matches] = [(0,1);(1,2);(3,0)] and t1 =
       [6;7;9] we obtain [(0,7);(1,9);(3,6)]. That is, from [rel2] we
       will select all tuples which have 7 on position 0, 9 on
       position 1, and 6 on position 3.  *)
    let pv = List.map
      (fun (pos2, pos1) -> (pos2, Tuple.get_at_pos t1 pos1))
      matches
    in
    Tuple_set.iter
    (fun t2 ->
       try
         let t = join_fun pv t1 t2 in
           joinrel := Tuple_set.add t !joinrel
       with
           Not_joinable -> ()
    )
    rel2
  in
  let card1 = Tuple_set.cardinal rel1.set in
  let card2 = Tuple_set.cardinal rel2.set in
  if card1 < card2 then
    let join_fun = match f with
      | None -> Tuple.join
      | Some f -> (fun pv t1 t2 -> f.fn (Tuple.join pv t1 t2))
    in
    Tuple_set.iter (process_rel_tuple join_fun matches1 rel2.set) rel1.set
  else
    begin
      let pos2 = List.map fst matches1 in
      let join_fun = match f with
        | None -> Tuple.join_rev pos2
        | Some f -> (fun pv t1 t2 -> f.fn (Tuple.join_rev pos2 pv t1 t2))
      in
      Tuple_set.iter (process_rel_tuple join_fun matches2 rel1.set) rel2.set
    end;
  let bound = if size_is_upper !joinrel rhashed fn then None
    else Some Z.(size_bound rel1 * size_bound rel2) in
  {set = !joinrel; rhashed; bound; fp; fn}

let in_t2_not_in_t1 t2 matches =
  let len  = List.length (Tuple.get_constants t2) in
  (* these are the positions in t2 which also appear in t1 *)
  let t2_pos = List.map snd matches in
  let rec get_aux pos =
    if pos = len then []
    else if not (List.mem pos t2_pos) then
      let v = Tuple.get_at_pos t2 pos in
      v :: (get_aux (pos+1))
    else
      get_aux (pos+1)
  in
  get_aux 0

(* Misc.subset attr1 attr2 *)
(* Note that free_vars in (f1 AND f2) are ordered according to f1 not
   to f2!  Thus, for instance, for p(x,y) AND q(z,y,x) the fields
   should be ordered by (x,y,z).
*)
let natural_join_sc1 hashed0 f matches rel1 rel2 =
  let err = if Int_set.is_empty rel1.rhashed (*key*) then Z.zero
    else Z.(size_bound rel1 * size_bound rel2) in
  let fp = Z.(rel1.fp + rel2.fp + err) in
  let fn = Z.(rel1.fn + rel2.fn) in
  let make_join_tuple, rhashed = match f with
    | None -> Tuple.make_tuple, hashed0
    | Some f -> (fun t -> f.fn (Tuple.make_tuple t)), f.hashed
  in
  let joinrel = ref Tuple_set.empty in
  Tuple_set.iter (fun t2 ->
    let t1_list =
      List.map
        (fun (pos1, pos2) ->
          (* x is at pos1 in t1 and at pos2 in t2 *)
          Tuple.get_at_pos t2 pos2)
        matches
    in
    let t1 = Tuple.make_tuple t1_list in
    if Tuple_set.mem t1 rel1.set then
      let t2_list = in_t2_not_in_t1 t2 matches in
      let t2' = make_join_tuple (t1_list @ t2_list) in
      joinrel := Tuple_set.add t2' !joinrel
  ) rel2.set;
  let bound = if size_is_upper !joinrel rhashed fn then None
    else Some (min (size_bound rel1) (size_bound rel2)) in
  {set = !joinrel; rhashed; bound; fp; fn}

(* Misc.subset attr2 attr1 *)
let natural_join_sc2 f matches rel1 rel2 =
  let err = if Int_set.is_empty rel2.rhashed (*key*) then Z.zero
    else Z.(size_bound rel1 * size_bound rel2) in
  let fp = Z.(rel1.fp + rel2.fp + err) in
  let fn = Z.(rel1.fn + rel2.fn) in
  let make_join_tuple, rhashed = match f with
    | None -> (fun t -> t), rel1.rhashed
    | Some f -> f.fn, f.hashed
  in
  let joinrel = ref Tuple_set.empty in
  Tuple_set.iter (fun t1 ->
    let t2 = Tuple.make_tuple (
      List.map
        (* x is at pos2 in t2 and at pos1 in t1 *)
        (fun (pos2, pos1) -> Tuple.get_at_pos t1 pos1)
        matches
      )
    in
    if Tuple_set.mem t2 rel2.set then
      joinrel := Tuple_set.add (make_join_tuple t1) !joinrel
  ) rel1.set;
  let bound = if size_is_upper !joinrel rhashed fn then None
    else Some (min (size_bound rel1) (size_bound rel2)) in
  {set = !joinrel; rhashed; bound; fp; fn}

let natural_join_sc2_err_info f i1 i2 =
  let err = if Int_set.is_empty i2.ihashed (*key*) then Z.zero
    else Z.(i1.ibound * i2.ibound) in
  let ifp = Z.(i1.ifp + i2.ifp + err) in
  let ifn = Z.(i1.ifn + i2.ifn) in
  let ihashed = match f with
    | None -> i1.ihashed
    | Some f -> f.hashed
  in
  let ibound = min i1.ibound i2.ibound in
  {ihashed; ibound; ifp; ifn}

(* not set difference, but the diff operator as defined in Abiteboul, page 89 *)
let minus f posl rel1 rel2 =
  let err = if Int_set.is_empty rel2.rhashed (*key*) then Z.zero
    else Z.(size_bound rel1 * size_bound rel2) in
  let fp = Z.(rel1.fp + rel2.fn) in
  let fn = Z.(rel1.fn + rel2.fp + err) in
  let set, rhashed =
    match f with
    | None ->
      Tuple_set.filter
      (fun t ->
        let t' = (Tuple.projections posl t) in
        not (Tuple_set.mem t' rel2.set)
      )
      rel1.set, rel1.rhashed
    | Some f ->
      let process_tuple t =
        let t' = (Tuple.projections posl t) in
        if Tuple_set.mem t' rel2.set then None else Some (f.fn t)
      in
      Tuple_set.filter_map process_tuple rel1.set, f.hashed
  in
  let bound = if size_is_upper set rhashed fn then None
    else Some (size_bound rel1) in
  {set; rhashed; bound; fp; fn}


let reorder hashed0 f new_pos rel =
  let set, rhashed =
    match f with
    | None -> Tuple_set.map (Tuple.projections new_pos) rel.set, hashed0
    | Some f -> Tuple_set.map (fun t ->
        f.fn (Tuple.projections new_pos t)) rel.set, f.hashed
  in
  let bound = if size_is_upper set rhashed rel.fn then None
    else Some (size_bound rel) in
  {set; rhashed; bound; fp = rel.fp; fn = rel.fn}

(* the columns in [posl] are eliminated *)
let project_away hashed0 f posl rel =
  let set, rhashed =
    match f with
    | None -> Tuple_set.map (Tuple.project_away posl) rel.set, hashed0
    | Some f -> Tuple_set.map (fun t ->
        f.fn (Tuple.project_away posl t)) rel.set, f.hashed
  in
  let bound = if size_is_upper set rhashed rel.fn then None
    else Some (size_bound rel) in
  {set; rhashed; bound; fp = rel.fp; fn = rel.fn}

type filter_hint = Exact | HashEq | HashNotEq

let filter hint f p rel =
  let fp, fn =
    match hint with
    | Exact -> rel.fp, rel.fn
    | HashEq -> Z.(rel.fp + size_bound rel), rel.fn
    | HashNotEq -> rel.fp, Z.(rel.fn + size_bound rel)
  in
  let set, rhashed =
    match f with
    | None -> Tuple_set.filter p rel.set, rel.rhashed
    | Some f -> Tuple_set.filter_map (fun t ->
        if p t then Some (f.fn t) else None) rel.set, f.hashed
  in
  let bound = if size_is_upper set rhashed fn then None
    else Some (size_bound rel) in
  {set; rhashed; bound; fp; fn}

let two = Z.of_int 2

let approx_agg ~mult rhashed gl rel set =
  let zero_extra_err =
    if mult then
      (* multiplicities count -> no hashed attributes in rel *)
      Int_set.is_empty rel.rhashed
    else
      (* multiplicites do not count -> no hashed attributes in groups *)
      Int_set.disjoint rel.rhashed (Int_set.of_list gl)
  in
  let rel_size = size_bound rel in
  let extra_err =
    if zero_extra_err then Z.zero
    else Z.(cdiv ((rel_size * rel_size) - rel_size) two)
  in
  let err = Z.(rel.fp + rel.fn + extra_err) in
  let bound = if size_is_upper set rhashed err then None else Some rel_size in
  {set; rhashed; bound; fp = err; fn = err}

let add_approx_error rel1 rel2 =
  let fp = Z.(rel1.fp + rel2.fp) in
  let fn = Z.(rel1.fn + rel2.fn) in
  let bound = Some Z.(size_bound rel1 + size_bound rel2) in
  {set = Tuple_set.empty; rhashed = rel2.rhashed; bound; fp; fn}

let del_approx_error rel1 rel2 =
  let fp = Z.(rel1.fp - rel2.fp) in
  let fn = Z.(rel1.fn - rel2.fn) in
  let bound = Some Z.(size_bound rel1 - size_bound rel2) in
  {set = Tuple_set.empty; rhashed = rel1.rhashed; bound; fp; fn}

let strip_set rel =
  {rel with set = Tuple_set.empty; bound = Some (size_bound rel)}
