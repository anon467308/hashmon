#!/bin/bash
set -e

samples=100
hashbits="8 16 24 32 40 48"

monitor=./monpoly/_build/default/src/main.exe
difftool=./tools/_build/default/diff.exe

function run1() {
  local label=$1
  local sig=$2
  local mfotl=$3
  local log=$4
  local tps=$5
  echo "* running ${label}"

  rm -rf temp
  mkdir temp
  mkdir temp/merged
  ${monitor} -sig "${sig}" -formula "${mfotl}" -unfold_let smart -log "${log}" > temp/ref.txt

  for bits in ${hashbits}; do
    printf "  * $bits bits:"
    for i in $(seq 1 ${samples}); do
      ${monitor} -sig "${sig}" -formula "${mfotl}" -unfold_let smart -log "${log}" -hash "merged,${bits}" -errprobs -seed ${i} > "temp/merged/${i}.txt"
      printf " $i"
    done
    printf "\n"
    printf "${label};${bits};" >> accuracy.csv
    ${difftool} -ref temp/ref.txt -samples temp/merged -tps ${tps} >> accuracy.csv
  done
}

if [[ ! ( -d amazon && -d formulas && -d tools ) ]]; then
  echo "Error: this script script must be run from the archive's root directory"
  exit 2
fi
echo "formula;bits;fpl;fpu;fnl;fnu;dl;du;fpb;fnb;db" > accuracy.csv
run1 ex1 formulas/amazon.sig formulas/ex1.mfotl traces/amazon.log 2889
run1 frd formulas/amazon.sig formulas/frd.mfotl traces/amazon.log 2889
run1 e1 formulas/pq.sig formulas/e1.mfotl traces/pq_m_20000_100_1.log 100
run1 e2 formulas/pq.sig formulas/e2.mfotl traces/pq_m_20000_100_1.log 100
run1 e3 formulas/pq2.sig formulas/e3.mfotl traces/pq2_m_20000_100_1.log 100
run1 e4 formulas/pq2.sig formulas/e4.mfotl traces/pq2_m_50_100_1.log 100
