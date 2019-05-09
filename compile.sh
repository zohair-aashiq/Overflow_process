#!/usr/bin/env bash
rm Overflow-processor-assembly-0.1.jar;
echo "Compiling & packaging"
sbt "set test in assembly := {}" clean assembly;
cp ./target/scala-2.11/Overflow-processor-assembly-0.1.jar ./
du -sh Overflow-processor-assembly-0.1.jar