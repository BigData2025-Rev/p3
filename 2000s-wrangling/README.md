# How to run the scripts.

### The application is divided into four phases
- Pre Process
- Parsing
- Merging
- Combine

Each phase is associated with a single file, allowing it to be run independently. However, all phases except the preprocessing phase depend on the output of the previous phase. Therefore, it is recommended to run the files in the following order:

- final-wrangler/preprocess.py `(Downloads required files, unzips them and parses the header files)`
- final-wrangler/parser.py `(reads the header files generated for files 00001, 00002 and geo and fuses the header with the corresponding data)`
- final-wrangler/merge.py `(merges each state's csv files, into a single csv/orc for the entire state)`
- final-wrangler/combine.py `(combines all csv files from each state into one large csv/ orc)`
