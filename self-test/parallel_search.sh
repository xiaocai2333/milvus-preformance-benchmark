#!/bin/bash


for i in {1..10}
do
	echo $i
	nohup python search.py > 5M_parallel_$i".txt" &
done
