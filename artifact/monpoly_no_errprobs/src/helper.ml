open MFOTL
open Predicate
open Db

module Tuple_set = Relation.Tuple_set

exception Type_error of string

module Constraint_Set = Set.Make (
  struct type t = cst
   let compare = Stdlib.compare
  end)

include Constraint_Set

type constraintSet = Constraint_Set.t

type valueTuple = (string list * int list)

type sconstraint   = { values: cst list list; partitions: int list}

type constraintRelation = sconstraint list

type splitParameters = {keys: string list; constraints: constraintRelation; num_partitions: int }

type commandParameter = 
    | SplitSave       of Domain_set.split_save_parameters
    | SplitParameters of splitParameters
    | Argument        of string

    type dataTuple    = { ts: MFOTL.timestamp; db: Db.db; complete:bool; }
    type commandTuple = { c: string;  parameters: commandParameter option; }
    type slicingTestTuple = { vars: Predicate.var list; tuple: string list; output: int array}
    
    type parser_feed =
        | SlicingTestTuple of slicingTestTuple
        | CommandTuple of commandTuple
        | DataTuple    of dataTuple
        | ErrorTuple   of string
    
    type monpolyData    = { tp: int; ts: MFOTL.timestamp; db: Db.db; }
    type monpolyCommand = { c: string; parameters: commandParameter option}
    type monpolyTestTuple = { vars: Predicate.var list; tuple: string list; output: int array}
    
    type monpoly_feed =
        | MonpolyTestTuple of monpolyTestTuple
        | MonpolyCommand of commandTuple
        | MonpolyData    of monpolyData
        | MonpolyError   of string
    

type 'a atree =
    | ALNode of 'a
    | AINode of ('a * int * int)  
    
type ('a, 'b) stree =  ('a, 'b option) Sliding.node atree    

let pvars_to_rel pvars = 
    Relation.singleton (Tuple.make_tuple (List.map (fun v -> Str v) pvars))

let preds_to_rel preds = 
    Relation.singleton (Tuple.make_tuple (List.map (fun (name, _, _) -> Str name) preds))

let rel_to_pvars rel   =
    List.map (fun e -> match e with
        | Str   s   -> s
        | _   -> raise (Type_error ("rel_to_pvars helper funtion only accepts strings")))
    (Tuple.get_constants (Tuple_set.min_elt (Relation.to_set rel)))

let rel_to_preds rel   =
    List.map (fun e -> match e with
    | Str   s   -> s
    | _  -> raise (Type_error ("rel_to_pvars helper funtion only accepts strings")))
    (Tuple.get_constants (Tuple_set.min_elt (Relation.to_set rel)))

let comp_preds comp (predicates : Predicate.predicate list) =
    let names = rel_to_preds (comp (preds_to_rel predicates)) in
    let filtered = [] in
    let rec filter l filtered =
        if List.length l != 0 then
            let name = List.hd l in
            filter (List.tl l) (List.find (fun (n, _, _) -> n == name) predicates :: filtered)
        else
            filtered
    in
    filter names filtered

let empty = Constraint_Set.empty

let map f set =
    Constraint_Set.map f set
    
let is_empty set =
    Constraint_Set.is_empty set

let add c set =
    Constraint_Set.add c set
    
let singleton c =
    Constraint_Set.singleton c

let find_opt e set =
    Constraint_Set.find_opt e set

let get_max csrel = 
    let max = ref 0 in
    List.iter (fun cs -> List.iter (fun p -> if p > !max then max := p;) cs.partitions) csrel;
    !max

(* It returns the list consisting of the new elements in the new time
   window with respect to the old time window. It is used by once and
   eventually evaluation functions.

   Arguments:
   - [l] the (doubly-linked) list of old elements
   - [last] a pointer to the element of the list from which the
   processing starts
   - [cond] stopping condition
   - [f] a function to be applied on each element
*)
let get_new_elements l last cond f =
    let rec get crt new_last acc =
        let v = Dllist.get_data crt in
        if cond v then
          if Dllist.is_last l crt then
            (f v) :: acc, crt
          else
            get (Dllist.get_next l crt) crt ((f v) :: acc)
        else
          acc, new_last
      in
      if last == Dllist.void then
        get (Dllist.get_first_cell l) Dllist.void []
      else if not (Dllist.is_last l last) then
        get (Dllist.get_next l last) last []
      else
        [], last

let rec is_enum_list n = function
  | [] -> true
  | x::l -> x = n && is_enum_list (n + 1) l

let print_err_estimate rel =
  let fp = Relation.false_pos rel in
  let fn = Relation.false_neg rel in
  if fp <> 0. || fn <> 0. then
    Printf.printf "# FP <= %.4e, FN <= %.4e\n%!" fp fn

(** This function displays the "results" (if any) obtained after
    analyzing event index [i]. The results are those tuples satisfying
    the formula for some index [q<=i]. *)
let show_results posl i q tsq rel =
  let set = Relation.to_set rel in
  let set =
    if is_enum_list 0 posl then set else
      Tuple_set.map (Tuple.projections posl) set
  in
  let set =
    if !Misc.stop_at_first_viol && Tuple_set.cardinal set > 1 then
      Tuple_set.singleton (Tuple_set.choose set)
    else set
  in
  if !Misc.verbose then
    begin
      if posl = [] then
        Printf.printf "@%s (time point %d): %b\n%!"
          (MFOTL.string_of_ts tsq) q (not (Tuple_set.is_empty set))
      else
        begin
          Printf.printf "@%s (time point %d): " (MFOTL.string_of_ts tsq) q;
          Relation.print_setn "" set
        end;
      print_err_estimate rel
    end
  else
    begin
      if Misc.debugging Dbg_perf then
        Perf.show_results q tsq;
      let is_sat = not (Tuple_set.is_empty set) in
      let show_it = is_sat || !Misc.show_error_estimate in
      if show_it then
        begin
          if posl = [] then (* no free variables *)
            Printf.printf "@%s (time point %d): %b\n%!"
              (MFOTL.string_of_ts tsq) q is_sat
          else (* free variables *)
            begin
              Printf.printf "@%s (time point %d): " (MFOTL.string_of_ts tsq) q;
              Relation.print_set4 "" set;
              print_newline()
            end;
          if !Misc.show_error_estimate then
            print_err_estimate rel
        end
    end
