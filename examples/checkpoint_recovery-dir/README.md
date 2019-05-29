# Checkpoint and Recovery Example

This is a code example to show how to checkpoint FishStore while it is ingesting data and how to recover to a consistent state from checkpoints. Specifically, this example use Github Archive Dataset as an example.

In this example, we first start a parallel ingestion and make a checkpoint while all workers are still ingesting data. Then, we recover from the checkpoint and continue ingestion from where it stops (reported as a serial number and a offset for each ingestion worker). We show the query results from the recovered store and the uninterupted store are the same.

# Usage
The usage of compiled binary is as below:

```bash
$ ./checkpoint_recovery <input_file> <lib_file> <n_threads> <memory_budget> <store_target>
```

You can use the link provided [here](../README.md) to download a sample of Github Archive dataset as your `<input_file>`.

You should use the library file generated from `lib_examples` as your `<lib_file>`. For windows user, you should use `github_lib.dll` while Linux user should use `libgithub_lib.so`.

`<n_threads>` indicates the number of parallelism for data ingestion.

`<memory_budget>` refers to the exponentiation of memory budget bytes with base 2. For example, when `<memory_budget>` is set to 31, the memory budget reserve for FishStore is `2^31B = 2GB`.

`<store_taget>` tells FishStore where to store its log file and checkpoints.

# Notice
This example will read the input file in raw text to main memory first so as to demonstrate the full ingestion speed of FishStore. Therefore, please make sure you have enough main memory to hold both the input data and memory budget reserve for FishStore.
