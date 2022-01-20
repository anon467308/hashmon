Randomized First-Order Monitoring With Hashing
==============================================

I. SYNOPSIS
-----------

This artifact accompanies the paper "Randomized First-Order Monitoring With
Hashing" submitted to CAV 2022. It provides (1) the full source code of the
monitor implementation, and (2) all data and scripts necessary to replicate the
experimental results, specifically those reported in Table 3 and Figure 1 in the
paper.


II. CONTENT
-----------

The `artifact` folder is structured as follows.

```
  - monpoly/: Source code of the monitor, which is modification of the
    open-source MonPoly tool. The extensions described in the paper are mainly
    implemented in the modules
      - src/hashing.ml: conversion of MFOTL to TRA expressions, rewriting
        algorithm, static error probability analysis, implementation of hashing
        operator;
      - src/relation.ml: tracking of error probability bounds at run-time;
      - src/clhash_stubs.c: CLHASH wrapper, GF(2^k) multiplication.
    See section V below for using the monitor directly.

  - monpoly_no_errprobs/: A patched version of the monitor without the run-time
    error tracking. This serves as the baseline for comparison.

  - formulas/: All example expressions from the paper as MFOTL formulas.
    Where possible, the expressions are also given in DejaVu's logic (*.qtl
    files).
      - ex1.mfotl: e_{ex}, closed variant of Example 1
      - ex1_days.mfotl: same as ex1.mfotl except using days as time units
        instead of seconds
      - frd.mfotl: e_{frd}, described in Appendix C of the paper
      - e1.mfotl: e_1, described in Section 7 of the paper
      - e2.mfotl: e_2 (ditto)
      - e3.mfotl: e_3 (ditto)
      - e3_let.mfotl: e_3' (ditto, see also section III below)
      - e4.mfotl: e_4 (ditto)
      - *.sig: trace schemas

  - tools/: Utility programs. Entry points:
      - generate.ml: generator for pseudorandom traces
      - extracts.ml: extracts the maximum error probability bounds from the
        monitor's output
      - diff.ml: compares a reference monitor output to a collection of sample
        output (obtained using different hash function seeds) and estimates the
        true error probabilities

  - amazon/: Amazon review data. See README.txt for origin.

  - paper_results/: Raw and processed experiment results obtained by the authors
    and shown in the paper.

  - Shell and Python scripts for setup and experiments, described in section IV
    of this readme.
```


III. FORMAT OF THE EXAMPLE EXPRESSIONS
--------------------------------------

The monitor program reads specifications expressed in metric first-order
temporal logic (MFOTL) and this is therefore the language used by the
specifications in the `formulas` folder. Most aspects of the syntax are
described in [1]. Internally, the monitor converts MFOTL to temporal-relational
algebra (TRA) expressions. This representation can be inspected (see section
IV.6).

The monitor additionally supports LET operators in formulas. Their syntax is

    LET r(x1, x2, ...) = f1 IN f2

where x1, x2, ... are the free variable of subformula f1. The effect of this
operator is that the relation r is (re)defined to contain the satisfying tuples
of f1 within subformula f2. LET operators are fully supported by the internal
TRA representation and the rewriting algorithm. In fact, they are used to
achieve the sharing in the `e3_let` formula (called `e_3'` in the paper). The
monitor can optionally unfold LET operators, i.e., substitute f1 for all
occurrences of r in f2. All experiments use the setting `-unfold_let smart`,
which unfolds those LET operators where r is used at most once in f2. This type
of unfolding does not affect the sharing of data structures in the monitor.

[1] David Basin, Felix Klaedtke, Eugen Zalinescu: The MonPoly Monitoring Tool.
    RV-CuBES 2017: 19-28 (https://doi.org/10.29007/89hs)


IV. REPLICATION
---------------

### 1. Environment

This artifact has been tested on a 64-bit Linux system with at least the
following software installed:

- GCC 11
- libgmp-dev 6.2
- Python 3.10
- OPAM 2.1
  - ocaml-base-compiler 4.12
  - dune 2.9
  - dune-build-info 2.9
  - zarith 1.12
- OpenJDK 11
- Scala 2.13
- Pandas 1.3.4
- matplotlib 3.4.3

Minimal resource requirements:

- 1 CPU core
- 4GB of RAM
- 8GB of free disk space
- The monitor program requires a x86\_64 CPU with SSE2 and PCLMUL instructions.
  All consumer-grade Intel and AMD processors from 2013 and later should do so.

The authors performed the experiments on a laptop (specs below) without using
a virtual machine. It is expected that the runtime and memory measurements
(Table 3) can reproduced approximately and the accuracy results (Fig. 1)
exactly.

- Intel(R) Core(TM) i5-7200U CPU @ 2.50GHz (2 physical cores, 2 threads each)
- Turbo Boost disabled, "performance" governor
- 8 GB of RAM, no swap
- Linux kernel 5.15.13-200.fc35.x86\_64 (Fedora 35)

The following instructions assume that the entire `artifact` folder has been
copied locally. Make sure that the execute permission bit is preserved on all
files. The folder must be writable.

Important: All command lines (indicated by the leading '$' sign, which is not
part of the command) must be executed in a terminal with the `artifact` folder
as the current working directory.


### 2. Preparation

Run the commands

    $ ./build.sh
    $ ./gen_traces.sh

in this order to compile all programs that are part of the artifact and generate
the traces for the experiments. Compiler warnings during the build are expected.
The trace generation may take several minutes. The traces can be found in the
newly created `traces` folder.

You may want to disable swap on your system. This might make the memory
measurements more accurate.

### 3. Running the experiments

Run

    $ ./run_perf.sh
    $ ./run_accuracy.sh

to replicate the full set of experiments. The 'perf' experiments obtain runtime
and peak memory measurements. The 'accuracy' experiments estimate the true error
probability by repeating the same monitoring task many times with different hash
function seeds.

WARNING: The 'perf' experiments are expected to take ~3 hours, the 'accuracy'
experiments around 20 hours (!).

The raw results are written to perf.csv and accuracy.csv, respectively. Each
line corresponds to one run of the monitor; values are separated by semicolons.
The files' columns are

  - formula: name of the formula file without the .mfotl suffix
  - type: experiment group (a = Amazon data, m = synthetic data, s = "thin"
    synthetic data)
  - rate: number of tuples per time-point (m group) or per time-stamp (s group);
    the value 0 is used for the Amazon data
  - strlen: string length for synthetic data
  - mode: hashing mode, either `bypass` (B in the paper), `off` (ID), `single`
    (H), or `merged` (Hm); `dejavu` indicates that DejaVu was used instead of
    MonPoly
  - rep: repetition index
  - time: runtime in seconds
  - memory: peak memory (maximum resident set size) in KiB
  - fp: maximum false-positive error bound output by the monitor
  - fn: maximum false-negative error bound output by the monitor

  - formula: name of the formula file without the .mfotl suffix
  - bits: size of the hash values
  - fpl and fpu: lower bound of the 95% confidence interval for the observed
    false-positive error probability
  - fnl and fnu: similarly for false-negative errors
  - dl and du: similarly for errors of either kind
  - fpb: maximum false-positive error bound output by the monitor
  - fnb: maximum false-negative error bound output by the monitor
  - db: maximum error bound (fpb + fnb) output by the monitor

The scripts should be fairly self-explanatory and the authors encourage
modifications to run other subsets or variants of the experiments. The sample
count in the 'accuracy' experiments should not be reduced further, however, as
the uncertainty about the observed error probabilities would become too large.

### 4. Post-processing

Given that perf.csv has been created, the command

    $ ./perf_table.py

prints the raw version of Table 3 from the paper. The mode labels are the same
as explained above. Given that accuracy.csv has been created, the command

    $ ./accuracy_plot.py

produces a file accuracy.png with a plot similar to Figure 1.

### 5. Inspecting the examples

It is possible to inspect the rewritten TRA expressions for each example.
To this end, run

    $ ./examples.sh | less

Unary TRA operators are written postfix. They map to the notation used in the
paper as follows: RENAME - rho, DROP - pi (with complemented attribute list),
EXTEND - eta, SELECT - sigma, JOIN - bowtie, DIFF - triangle right, SINCE
- S^bowtie, NOT SINCE - S^triangle, and similar for (NOT) UNTIL. All other
operators should be self-explanatory.

The above command also prints the results of the static error probability
analysis. See monpoly/README.md, section "Running", for an explanation of the
variables used in the symbolic bounds.

The rewritten expression shown in Example 3 and the static bounds shown in
Example 4 of the paper correspond to the output for the formula
`ex1_days.mfotl`. (There are additional attributes `_1`, `_2`, etc. because the
Amazon trace contains parameters that this formula ignores. Those were omitted
from the paper for simplicity.)


V. RUNNING THE MONITOR
----------------------

This artifact comes with a fully functional monitor that can be used
independently of the performance and accuracy experiments. After the code has
been compiled, you may invoke the monitor using the wrapper script
monpoly/monpoly. The most important command line arguments, including those that
relate to hashing, are described in monpoly/README.md, section "Running".

For instance, the following command obtains the monitor's output for the "fake
review detection" expression on the Amazon data:

    $ ./monpoly/monpoly -sig formulas/amazon.sig -formula formulas/frd.mfotl -log traces/amazon.log

If the `-hash merged` flag is added to the command, hashing with merging is
used; with the `-errprobs` flag, the monitor prints the dynamically computed
error bounds after every time-point (unless both bounds are zero).
