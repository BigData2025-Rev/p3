# How to run the scripts.

### The application is divided into four phases
- Pre Process
- Parsing
- Merging
- Combine

Each phase is associated with a single file so it can be run independently, but every phase except the preprocess phase relies on the previous phases output, hence it is adviced to run the files in the following order:

- final-wrangler/preprocess.py (Downloads required files, unzips them and parses the header files)
- final-wrangler/parser.py (reads the header files generated for files 00001, 00002 and geo and fuses the header with the corresponding data)
- final-wrangler/merge.py (merges each state's csv files, into a single csv/orc for the entire state)
- final-wrangler/combine.py (combines all csv files from each state into one large csv/ orc)
