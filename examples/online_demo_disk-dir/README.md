# Online CLI Demo (Disk Version)
This code example provides a CLI based demo supporting live data ingestion, dynamically de/registering PSFs, and record retrieval.

Specifically, user need to specify an input data file which will be ingested into FishStore in an rotation manner. Once the input file is loaded as raw text, user can use a CLI interface to dynamically manage PSF registration, query data, and check current ingestion throughput.

**This demo will actually ingest data onto disk. Thus, to protect your SSD, we do not suggest you use this as a long running demo. For an in-memory version, please refer to the code [here](../online_demo-dir).**

# Usage
The usage of compiled binary is as below:

```bash
$ ./online_demo_disk <input_file> <n_threads> <memory_budget> <store_taget>
```

You can use any JSON dataset as the `<input_file>` of this demo. Please refer to [here](../README.md) for some sample dataset.

`<n_threads>` indicates the number of parallelism for data ingestion.

`<memory_budget>` refers to the exponentiation of memory budget bytes with base 2. For example, when `<memory_budget>` is set to 31, the memory budget reserve for FishStore is `2^31B = 2GB`.

`<store_taget>` tells FishStore where to store its log file and checkpoints.

Following are the possible commands you can use in the CLI demo:

```
reg-field <field_name>                                     Register a field projection PSF on <field_name>.
dereg-field <field_name>                                   Reregister a field projection PSF on <field_name>.
load-lib <path>                                            Load a PSF Library from <path>.
reg-filter <lib_id> <func_name> <n_fields> <fields>...     Register a filter PSF <func_name> in Library <lib_id> defined over <fields>.
rereg-filter <filter_id>                                   Reregister a filter PSF that has been assigned to <filter_id>
dereg-filter <filter_id>                                   Deregister the filter PSF with ID <filter_id>.
scan filter <filter_id>                                    Do an index scan over Filter PSF #<filter_id>.
scan field <field_name> <value>                            Do an index scan over field <field_name> (need to be registered) over <value>.
print-throughput                                           Print the current ingestion throughput of FishStore.
print-reg                                                  Print the current FishStore registration.
exit                                                       Stop the demo.
help                                                       Show this message
```

Note that this demo shows only a taste of FishStore's capablity. In specific, `reg-field` will only register a projection PSF to a **string field** and return you a field id. Also, we only provide **inlne PSF based filters** (returning either 1 or `null`) in this demo and **DO NOT** recycle `filter_id`. Thus, if you deregister a PSF and want to register it again, you need to use the `filter_id` that has already been assigned.

# Notice
This example will read the input file in raw text to main memory first so as to demonstrate the full ingestion speed of FishStore. Therefore, please make sure you have enough main memory to hold both the input data and memory budget reserve for FishStore.

