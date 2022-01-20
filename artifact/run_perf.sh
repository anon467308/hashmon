#!/bin/bash
set -e

repetitions=3
m_modes="bypass off single merged dejavu"
s_modes="bypass merged dejavu"
str_lengths="100"

monitor=./monpoly/_build/default/src/main.exe
monitor_bypass=./monpoly_no_errprobs/_build/default/src/main.exe
dejavu_wrapper=./dejavu_wrapper.sh
time=/usr/bin/time
extract=./tools/_build/default/extract.exe

tempdir=$(mktemp -d)
trap 'rm -rf -- "${tempdir}"' EXIT
echo "using temp directory ${tempdir}"

function run1() {
  local formula=$1
  local sig=$2
  local type=$3
  local rate=$4
  local strlen=$5
  local rep=$6
  local trace=${7:-${sig}_${type}_${rate}_${strlen}_${rep}}

  echo "* formula=${formula} sig=${sig} type=${type} rate=${rate} strlen=${strlen} rep=${i}"

  cp formulas/${sig}.sig "${tempdir}/sig"
  cp formulas/${formula}.mfotl "${tempdir}/mfotl"
  cp traces/${trace}.log "${tempdir}/trace"
  if [[ ${type} = s && -f formulas/${formula}.qtl ]]; then
    local enable_dejavu=1
    cp traces/${trace}.timed.csv "${tempdir}/trace.timed.csv"
  fi
  sync -f "${tempdir}"

  local modes=${m_modes}
  if [[ ${type} = s ]]; then
    local modes=${s_modes}
  fi
  for mode in ${modes}; do
    if [[ ${mode} = dejavu ]]; then
      if [[ -n ${enable_dejavu} ]]; then
        echo "  * ${mode}"
        printf "${formula};${type};${rate};${strlen};${mode};${i};" >> perf.csv
        ${time} -f '%e;%M;0;0' -a -o perf.csv ${dejavu_wrapper} run ${formula} "${tempdir}/trace.timed.csv" > "${tempdir}/dejavu-out" 2> /dev/null
      fi
    else
      echo "  * ${mode}"
      local cmd=${monitor}
      if [[ ${mode} = bypass ]]; then
        local cmd=${monitor_bypass}
      fi
      printf "${formula};${type};${rate};${strlen};${mode};${i};" >> perf.csv
      ${time} -f '%e;%M;' -a -o perf.csv ${cmd} -sig "${tempdir}/sig" -formula "${tempdir}/mfotl" -unfold_let smart -log "${tempdir}/trace" -hash "${mode}" -errprobs -seed ${i} > "${tempdir}/out"
      truncate -s -1 perf.csv  # remove newline
      ${extract} "${tempdir}/out" >> perf.csv
    fi
  done
}

if [[ ! ( -d amazon && -d formulas && -d tools ) ]]; then
  echo "Error: this script must be run from the archive's root directory"
  exit 2
fi
echo "formula;type;rate;strlen;mode;rep;time;memory;fp;fn" > perf.csv

for i in $(seq 1 ${repetitions}); do
  run1 ex1 amazon a 0 0 ${i} amazon
  run1 frd amazon a 0 0 ${i} amazon

  for strlen in ${str_lengths}; do
    run1 e1 pq s 2000 ${strlen} ${i}
    run1 e2 pq s 2000 ${strlen} ${i}
#    run1 e4 pq2 s 50 ${strlen} ${i}

    run1 e1 pq m 20000 ${strlen} ${i}
    run1 e2 pq m 20000 ${strlen} ${i}
    run1 e3 pq2 m 20000 ${strlen} ${i}
    run1 e3_let pq2 m 20000 ${strlen} ${i}
    run1 e4 pq2 m 50 ${strlen} ${i}
  done
done
