# MapReduce Word Count (INTE 22253)

A MapReduce-style word count in Python. It uses multiprocessing to
simulate distributed workers.

## How it works
Split -> Map (with combiner) -> Shuffle -> Reduce -> Merge

## Run
    python mapreduce_wordcount.py                 # built-in sample
    python mapreduce_wordcount.py input.txt 4     # your file, 4 workers

## Author
Oshan Harischandra , IM/2023/110
