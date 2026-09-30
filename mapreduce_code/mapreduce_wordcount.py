"""
MapReduce-style word count using multiprocessing to simulate distributed workers.
Phases: Split -> Map (parallel) -> Shuffle/Partition -> Reduce (parallel) -> Merge
Usage: python mapreduce_wordcount.py [input.txt] [num_workers]
"""
import re
import sys
import time
from collections import Counter, defaultdict
from multiprocessing import Pool

WORD_RE = re.compile(r"[a-z']+")

def split_input(text, n):
    """Split the text into n roughly equal chunks on line boundaries (like input splits)."""
    lines = text.splitlines()
    size = max(1, len(lines) // n + 1)
    return ["\n".join(lines[i:i + size]) for i in range(0, len(lines), size)]

def map_task(chunk):
    """Map: emit (word, 1) pairs, with a local combiner to cut shuffle traffic."""
    combined = Counter(WORD_RE.findall(chunk.lower()))
    return list(combined.items())

def partition(mapped_outputs, n_reducers):
    """Shuffle: route each key to a reducer by hash so equal keys meet at one reducer."""
    parts = [defaultdict(list) for _ in range(n_reducers)]
    for output in mapped_outputs:
        for word, count in output:
            parts[hash(word) % n_reducers][word].append(count)
    return [dict(p) for p in parts]

def reduce_task(partition_data):
    """Reduce: sum the counts for every key in this partition."""
    return {word: sum(counts) for word, counts in partition_data.items()}

def mapreduce_wordcount(text, workers=4):
    chunks = split_input(text, workers)
    with Pool(workers) as pool:
        mapped = pool.map(map_task, chunks)              # Map phase
        parts = partition(mapped, workers)               # Shuffle phase
        reduced = pool.map(reduce_task, parts)           # Reduce phase
    result = {}
    for r in reduced:
        result.update(r)                                 # Merge (keys are disjoint)
    return result

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    if path:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    else:
        text = ("the quick brown fox jumps over the lazy dog\n"
                "the dog barks and the fox runs away\n"
                "distributed systems process data in parallel\n") * 20000
    start = time.time()
    counts = mapreduce_wordcount(text, workers)
    elapsed = time.time() - start
    # Verify against a plain single-process count
    assert counts == dict(Counter(WORD_RE.findall(text.lower()))), "Mismatch!"
    print(f"Workers: {workers} | Unique words: {len(counts)} | Time: {elapsed:.3f}s | Verified: OK")
    for word, c in sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:10]:
        print(f"{word:12s} {c}")
