#!/bin/bash

monitor=./monpoly/_build/default/src/main.exe

function analyze() {
  local s=$1
  local f=$2
  local title=$3
  echo "========================================================================"
  echo "${title}"
  echo "========================================================================"
  ${monitor} -sig "formulas/$s" -formula "formulas/$f" -unfold_let smart -hash merged -analyze
  echo
}

analyze amazon.sig ex1.mfotl "ex1.mfotl (e_{ex}, using seconds as time unit)"
analyze amazon.sig ex1_days.mfotl "ex1_days.mfotl (e_{ex}, using days as time unit)"
analyze amazon.sig frd.mfotl "frd.mfotl (e_{frd})"
analyze pq.sig e1.mfotl "e1.mfotl (e_1)"
analyze pq.sig e2.mfotl "e2.mfotl (e_2)"
analyze pq2.sig e3.mfotl "e3.mfotl (e_3)"
analyze pq2.sig e3_let.mfotl "e3_let.mfotl (e_3')"
analyze pq2.sig e4.mfotl "e4.mfotl (e_4)"
