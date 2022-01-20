{
  type top_token =
    | TIMEPOINT of int * float
    | ERRBOUND of float * float
    | SKIP
    | EOF

  type data_token =
    | LPAREN
    | RPAREN
    | COMMA
    | STRING of string
    | END
    | ERR
}

let lc = ['a'-'z']
let uc = ['A'-'Z']
let letter = uc | lc
let digit = ['0'-'9']
let integer = digit+
let float = digit+ '.' digit* ('e' ['+' '-'] digit+)?
let simple_string = (letter | digit | '_' | '[' | ']' | '/' | ':' | '-' | '.' | '!')+
let quoted_string = [^'"']*
let eol = "\n" | "\r" | "\r\n"

rule lex_toplevel =
  parse
  | eof                                                            { EOF }
  | "@" (float as ts) " (time point " (integer as tp) "):"         { TIMEPOINT (int_of_string tp, float_of_string ts) }
  | "# FP <= " (float as fp) ", FN <= " (float as fn) (eol | eof)  { ERRBOUND (float_of_string fp, float_of_string fn) }
  | _                                                              { lex_skip lexbuf }
and lex_skip =
  parse
  | eol | eof { SKIP }
  | _         { lex_skip lexbuf}
and lex_data =
  parse
  | eol | eof                    { END }
  | [' ' '\t']                   { lex_data lexbuf }
  | "("                          { LPAREN }
  | ")"                          { RPAREN }
  | ","                          { COMMA }
  | simple_string as s           { STRING s }
  | '"' (quoted_string as s) '"' { STRING s }
  | _                            { ERR }
