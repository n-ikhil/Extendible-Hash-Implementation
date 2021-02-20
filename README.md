# Extendible-Hash-Implementation


- Generate dataset by `make generate_data`. This produces `dataset.csv`. 
- To start inserting Records from `dataset.csv` and hence run the implementation run `make run`
- Output is visualized by displaying contents of Directory table as well as SSM. Null entries are omitted from printing.
- To run with custom file either add the file with name `dataset.csv` or change the variable `inputFileName` in logic.cpp in top section.
- To toggle/set various parameters , change the constant variables in `logic.cpp` in top section.
- `make clean` removes bin files.
### Code structure
- `logic.cpp` contains core implementations of extendible hash. As OOPs is used, code is mostly self explainatory.
- `generate.cpp` contains auxillary code for generating csv file.
- `runner.cpp` contains code to run the Extendible hash.

### Assumptions/Features
- Current implementation recovers unused buckets for further use for record storage.
- Directory table entries which overflow are stored in reverse order in SSM.
- Overflow buckets for Records are not reserver in certain section of SSM. Instead allocator allocates new free Bucket nearest to base index.
- Uses MSB for hash finding.
- To run randomly generated records, instead of sequential, uncomment in `runner.cpp`