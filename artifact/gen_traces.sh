#!/bin/bash
set -e

# Parameters:
repetitions=3
str_lengths="100"
trace_length=100
pq_s_rates="2000"
pq2_s_rates="50"
pq_m_rates="20000"
pq2_m_rates="50 20000"

convert=./amazon/convert.py
generate=./tools/_build/default/generate.exe

function info() {
  echo "$@" >&2
}

function gen_amazon() {
  info "* converting Amazon review data"
  ${convert} --meta amazon/meta_Gift_Cards.json.gz > traces/amazon_meta.log
  local out=traces/amazon.log
  cp traces/amazon_meta.log ${out}
  ${convert} --reviews amazon/Gift_Cards.json.gz >> ${out}
}

function gen_pqX() {
  local x=$1
  local sig=$2
  local s_rates=$3
  local m_rates=$4
  info "* generating random traces for ${sig}, with ordered events"
  for ir in ${s_rates}; do
    info "  * index rate ${ir}"
    for strlen in ${str_lengths}; do
      info "    * string length ${strlen}"
      for i in $(seq 1 ${repetitions}); do
        local label=${sig}_s_${ir}_${strlen}_${i}
        local out=traces/${label}.log
        local dejavu_out=traces/${label}.timed.csv
        ${generate} -strlen ${strlen} -ir ${ir} -er 1 -tss ${trace_length} -seed ${i} -dejavu ${dejavu_out} -- p 1 q ${x} > ${out}
      done
    done
  done

  info "* generating random traces for ${sig}, with simultaneous events"
  for er in ${m_rates}; do
    info "  * event rate ${er}"
    for strlen in ${str_lengths}; do
      info "    * string length ${strlen}"
      for i in $(seq 1 ${repetitions}); do
        local out=traces/${sig}_m_${er}_${strlen}_${i}.log
        ${generate} -strlen ${strlen} -ir 1 -er ${er} -tss ${trace_length} -seed ${i} -- p 1 q ${x} > ${out}
      done
    done
  done
}

if [[ ! ( -d amazon && -d formulas && -d tools ) ]]; then
  info "Error: this script must be run from the archive's root directory"
  exit 2
fi
mkdir -p traces
gen_amazon
gen_pqX 1 pq  "${pq_s_rates}" "${pq_m_rates}"
gen_pqX 2 pq2 "${pq2_s_rates}" "${pq2_m_rates}"
