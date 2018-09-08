#!/usr/bin/env bash

rm -rf output
if spark-submit friend-recommendation.py $1 $2
then
    echo
    echo "FIRST 10 LINES OF OUTPUT"
    echo
    head $2/part-00000
else
    echo "PROGRAM FAILED. CHECK ABOVE OUTPUT"
fi
