📁 B+ Tree External Sorting Project

This project implements an external sorting system using:

    ✅ Replacement Selection (with a min-heap)

    ✅ Multiway Merge

    ✅ B+ Tree Bulk Loading

    ✅ Sequential Insertion to B+ Tree

📌 Features

    Replacement Selection is used to generate sorted runs (frozen/not frozen distinction is handled).

    K-way Merge merges all sorted runs into one final sorted file.

    Bulk Loading builds a B+ Tree directly from sorted data (without splits).

    Sequential Insertion inserts each record one by one into a B+ Tree (with splitting).

    Supports CSV input and outputs detailed logs during processing.

🔧 Files Overview
File	Description
replacement.c	Generates sorted runs using a min-heap.
merge.c	Merges sorted runs into a single sorted file.
bplus.c	Implements B+ Tree logic (bulk & sequential insert).
main.c	Orchestrates the process step-by-step.
run_files/	Stores generated run files.
sorted.csv	Final merged output, used for bulk loading.
🚀 How to Run

gcc main.c replacement.c merge.c bplus.c -o sorter
./sorter input.csv

📊 Example Output

    Run files: run_0.csv, run_1.csv, ...

    Merged file: sorted.csv

    Tree output: Structure of B+ Tree (in-order traversal etc.)

📚 Notes

    ORDER in B+ Tree can be adjusted from the code.

    All heap operations are handled manually without using heapify from libraries.

    Frozen status in replacement selection determines the boundaries of each run.

    Bulk loading builds tree bottom-up without splitChild().
