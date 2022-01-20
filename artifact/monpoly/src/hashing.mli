type mode = Nothing | Single of int | Merged of int

val analyze: mode option -> MFOTL.formula -> unit
val convert: mode -> MFOTL.formula ->
  Extformula.extformula * Predicate.var list * Neval.queue * Neval.cell ref
