# crop-testing

This is the testing repository for https://github.com/jonasbovyn/comunica-crop (the CROP implementation is found in the `feat/crop` branch). To generate the WebAssembly binary, check the https://github.com/jonasbovyn/crop-cpp repository.

## Setting up the testing environment

### Preparing the datasets

1. Under the `datasets/` folder, 3 folders are located which must be filled with data generated from https://github.com/comunica/watdiv-docker. 
   1. Under `datasets/dataset100k` use `-s 1 -q 5` as command line arguments which will result in around 100k triples.
   2. Under `datasets/dataset1M` use `-s 10 -q 5` which will result in around 1M triples.
   3. Under `datasets/dataset100k` use `-s 10 -q 5` which will result in around 10M triples.
2. Each folder `datasets/X/` should now have a subfolder `queries/` and a file `dataset.nt`.
3. In addition, use the https://github.com/rdfhdt/hdt-cpp tool  to generate `dataset.hdt` files from the `dataset.nt` files.
   1. The command is `./rdf2hdt dataset.nt dataset.hdt`

### Installing the testing environment

1. Use `yarn install` in the project directory
2. Create a soft link in `node-modules/` pointing to the custom Comunica repository, named `@comunica-crop/`.
   1. In linux/maxOS, the command for this is `ln -s [source] [destination]`
   2. Make sure Comunica is built and checked out in the right branch. Use the `mem-benchmark` branch when testing optimization memory usage, otherwise use the `feat/crop` branch.

### Running the tests

If tests require a dataset to be deployed using Server.js, first run `node runServer.js`. The other test files (`benchmark<XXX>.js`) each have their own purpose and configuration, and if they require additional command line arguments, this is specified in the file. (we used `node v16.13.0`)

Additionally, the data can be analyzed in `results/`, which has a python notebook (`python v3.10` was used)
