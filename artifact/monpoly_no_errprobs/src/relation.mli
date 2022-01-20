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



(**
    This module defines relations and provides operations over them.

    Relations are sets of tuples of the same length. Currently,
    relations are implemented using the
    {{:http://caml.inria.fr/pub/docs/manual-ocaml/libref/Set.html}Set}
    module from the standard library. (The ordering of tuples is given
    by the {!Tuple.compare} function, hence it's lexicographic.)

    This module has an unnamed perspective of relations. Hence,
    columns are identified by their position (from 0 to the number of
    columns minus 1). On the contrary, the {!module:Table} module has
    a named perspective and therein columns are identified by
    attributes.
*)

open Tuple
open Predicate

val collision_prob: float ref

module Tuple_set: Set.S with type elt = tuple

type relation
  (** The type of relations. *)

val to_set: relation -> Tuple_set.t

val is_empty: relation -> bool
val is_certainly_empty: relation -> bool

val false_pos: relation -> float
val false_neg: relation -> float

val empty: relation
val singleton: tuple -> relation
val make_relation: tuple list -> relation
  (** Builds a relation from a list of tuples. *)

val eval_pred: predicate -> (relation -> relation)

val eval_equal: term -> term -> relation
val eval_not_equal: term -> term -> relation

module Int_set: Set.S with type elt = int

val no_hash: Int_set.t

type rel_map = {
  fn: tuple -> tuple;
  hashed: Int_set.t;
}

val no_hash_map: rel_map

val map: rel_map -> relation -> relation

val negate: relation -> relation

(*val union: rel_map option -> relation -> relation -> relation*)
val union: relation -> relation -> relation
val inter: rel_map option -> relation -> relation -> relation
val diff: rel_map option -> relation -> relation -> relation

val natural_join: Int_set.t -> rel_map option -> (int * int) list -> (int * int) list -> relation -> relation -> relation
  (** [natural_join tm matches rel1 rel2] returns the natural join of
      relations [rel1] and [rel2]. The parameter [matches] gives the
      columns which should match in the two relations in form of a
      list of tuples [(pos2,pos1)]: column [pos2] in [rel2] should
      match column [pos1] in [rel1].
      If [tm] is [Some f], the function [f] is applied to each output tuple.
  *)

val natural_join_sc1: Int_set.t -> rel_map option -> (int * int) list -> relation -> relation -> relation
(** [natural_join] special case 1: attr1 are included in attr2 *)

val natural_join_sc2: rel_map option -> (int * int) list -> relation -> relation -> relation
(** [natural_join] special case 2: attr2 are included in attr1 *)

val minus: rel_map option -> int list -> relation -> relation -> relation
  (** [reldiff tm rel1 rel2] returns the set difference between [rel1]
      and the the natural join of [rel1] and [rel2]. *)

val reorder: Int_set.t -> rel_map option -> int list -> relation -> relation

val project_away: Int_set.t -> rel_map option -> int list -> relation -> relation
  (** [project_away tm posl rel] removes the columns in [posl] from
      [rel]. *)

type filter_hint = Exact | HashEq | HashNotEq

val filter: filter_hint -> rel_map option -> (tuple -> bool) -> relation -> relation

val approx_agg: mult:bool -> Int_set.t -> int list -> relation -> Tuple_set.t -> relation
val add_approx_error: relation -> relation -> relation
val del_approx_error: relation -> relation -> relation
val strip_set: relation -> relation

type err_info

val empty_err_info: err_info
val err_info_of: relation -> err_info
val with_err_info: err_info -> relation -> relation

val union_err_info: err_info -> err_info -> err_info
val inter_err_info: err_info -> err_info -> err_info
val natural_join_sc2_err_info: rel_map option -> err_info -> err_info -> err_info

(** Pretty-printing functions: *)

(* val output_rel4: out_channel -> string -> relation -> unit *)

val print_set: string -> Tuple_set.t -> unit
val print_set4: string -> Tuple_set.t -> unit
val print_setn: string -> Tuple_set.t -> unit

val print_rel: string -> relation -> unit
val print_rel4: string -> relation -> unit
val print_reln: string -> relation -> unit
val print_bigrel: relation -> unit
val print_orel: relation option -> unit
