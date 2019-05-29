# Twitter Example

This is a code example to show how to ingest, register PSFs, and retrieve data for given properties on Twitter dataset.

In this example, we register several PSFs including field projection, customized predicates, spawn a bunch of ingestion workers to ingest input data into FishStore. We also show how user can use the PSF handle to retrieve all records with a specific property $(f, v)$.

# Usage
The usage of compiled binary is as below:

```bash
$ ./twitter_query <input_file> <lib_file> <n_threads> <memory_budget> <store_taget>
```

You can use the crawler provided [here](../README.md) to crawl sample of Github Archive dataset as your `<input_file>`.

You should use the library file generated from `lib_examples` as your `<lib_file>`. For windows user, you should use `twitter_lib.dll` while Linux user should use `libtwitter_lib.so`.

`<n_threads>` indicates the number of parallelism for data ingestion.

`<memory_budget>` refers to the exponentiation of memory budget bytes with base 2. For example, when `<memory_budget>` is set to 31, the memory budget reserve for FishStore is `2^31B = 2GB`.

`<store_taget>` tells FishStore where to store its log file and checkpoints.

# Notice
This example will read the input file in raw text to main memory first so as to demonstrate the full ingestion speed of FishStore. Therefore, please make sure you have enough main memory to hold both the input data and memory budget reserve for FishStore.
