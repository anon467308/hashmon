#!/bin/bash
set -e

echo "================================================================================"
echo "= Linux kernel version                                                         ="
echo "================================================================================"
uname -a

echo
echo "================================================================================"
echo "= CPU model                                                                    ="
echo "================================================================================"
grep "model name" /proc/cpuinfo

echo
echo "================================================================================"
echo "= CPU frequency scaling (requires cpupower)                                    ="
echo "================================================================================"
if command -v cpupower &> /dev/null; then
  cpupower frequency-info
fi

echo
echo "================================================================================"
echo "= Total memory                                                                 ="
echo "================================================================================"
grep "MemTotal:" /proc/meminfo

echo
echo "================================================================================"
echo "= Swaps                                                                        ="
echo "================================================================================"
swapon

echo
echo "================================================================================"
echo "= Packages installed in current OPAM switch                                    ="
echo "================================================================================"
opam list --installed --color=never

echo
echo "================================================================================"
echo "= Java/Scala                                                                   ="
echo "================================================================================"
java -version 2>&1
scalac -version 2>&1
