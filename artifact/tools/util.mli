val last: 'a list -> 'a option

val string_all: (char -> bool) -> string -> bool

val incr: int ref -> int -> unit

val print_list: string -> string -> string ->
  (out_channel -> 'a -> unit) -> out_channel -> 'a list -> unit

module String_map: Map.S with type key = string

type tuple = string list
module Rel: Set.S with type elt = tuple
type db = Rel.t String_map.t

val print_rel: out_channel -> Rel.t -> unit

val array_arg_max: ('a -> float) -> 'a array -> int
val matrix_max: float array array -> float
val array_sum: float array -> float
val array_avg: float array -> float

val wilson_interval: float -> int -> int -> float * float

val list_dir: string -> string list
