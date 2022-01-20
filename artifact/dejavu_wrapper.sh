#!/bin/bash
set -e

BDDSIZE=15

formuladir=formulas
dejavudir=dejavu
jarfile=dejavu.jar

cd ${dejavudir}

function compile() {
  local name=$1
  mkdir -p ${name}
  cd ${name}
  java -cp ../${jarfile} dejavu.Verify ../../${formuladir}/${name}.qtl
  scalac -cp ../${jarfile} TraceMonitor.scala
}

function run() {
  local name=$1
  local logfile=$2
  # We set a low initial heap size to obtain more accurate information about the
  # monitor's actual memory usage, at the expense of more garbage collections.
  if scala -J-Xms8m -J-Xmx2g -cp ${name}:${jarfile} TraceMonitor "${logfile}" $BDDSIZE 2>&1 | grep -E '\*\*\* .*(Error|Exception)'; then
    exit 1
  fi
}

mode=$1
shift
case ${mode} in
  compile) compile "$@" ;;
  run) run "$@" ;;
  *)
    echo "Error: unknown mode ${mode}" >&2
    exit 1
    ;;
esac
