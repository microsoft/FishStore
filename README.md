[![Build Status](https://dev.azure.com/ms/FishStore/_apis/build/status/microsoft.FishStore?branchName=master)](https://dev.azure.com/ms/FishStore/_build/latest?definitionId=160&branchName=master)

# Introduction

FishStore is a new ingestion and storage layer for flexible- and fixed-schema datasets. It allows you 
to dynamically register complex predicates over the data, to define interesting subsets of the data. 
Such predicates are called PSFs (for predicated subset functions).

FishStore performs partial parsing of the ingested data (based on active PSFs) in a fast, parallel, and 
micro-batched manner, and hash indexes records for subsequent fast PSF-based retrieval. To accomplish 
its goals, FishStore leverages and extends the [FASTER](https://github.com/microsoft/FASTER) hash 
key-value store, and uses an unmodified parser interface for fast parsing (we use 
[simdjson](https://github.com/lemire/simdjson) in many of our examples).

FishStore is being open-sourced as a research prototype, by researchers from Microsoft Research and the
University of Utah. You can read more about the concepts behind FishStore in the SIGMOD 2019 [research paper](https://badrish.net/papers/fishstore-sigmod19.pdf). Note that the research paper uses [Mison](http://www.vldb.org/pvldb/vol10/p1118-li.pdf) as its parser, whereas this open-source release of FishStore provides a generic parser interface 
model, with simdjson as an out-of-the-box example.

For detailed usage of FishStore, please refer to [our tutorial](tutorial.md).


# Building FishStore

Clone FishStore including submodules:

```sh
git clone https://github.com/microsoft/FishStore.git
cd FishStore
git submodule update --init
```

FishStore uses CMake for builds. To build it, create
one or more build directories and use CMake to set up build scripts for your
target OS. Once CMake has generated the build scripts, it will try to update
them, as needed, during ordinary build.

## Building on Windows

Create new directory "build" off the root directory. From the new
"build" directory, execute:

```sh
cmake .. -G "<MSVC compiler>"
```

To see a list of supported MSVC compiler versions, just run "cmake -G". As of
this writing, we're using Visual Studio 2019, so you would execute:

```sh
cmake .. -G "Visual Studio 16 2019"
```

For Visual Studio 2017, you would execute:

```sh
cmake .. -G "Visual Studio 15 2017 Win64"
```

That will create build scripts inside your new "build" directory, including
a "FishStore.sln" file that you can use inside Visual Studio. CMake will add several
build profiles to FishStore.sln, including Debug/x64 and Release/x64.

## Building on Linux

The Linux build requires several packages (both libraries and header files);
see "CMakeFiles.txt" in the root directory for the list of libraries
being linked to, on Linux.

As of this writing, the required libraries are:

- stdc++fs : for <experimental/filesytem>, used for cross-platform directory
             creation.
- uuid : support for GUIDs.
- tbb : Intel's Thread Building Blocks library, used for concurrent_queue.
- gcc
- aio : Kernel Async I/O, used by QueueFile / QueueIoHandler.
- stdc++
- pthread : thread library.

On Ubuntu, you may install dependencies as follows:

```sh
 sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
 sudo apt update
 sudo apt install -y g++-7 libaio-dev uuid-dev libtbb-dev
```

Also, CMake on Linux, for the gcc compiler, generates build scripts for either
Debug or Release build, but not both; so you'll have to run CMake twice, in two
different directories, to get both Debug and Release build scripts.

Create new directories "build/Debug" and "build/Release" off the root directory.
From "build/Debug", run:

```sh
cmake -DCMAKE_BUILD_TYPE=Debug ../..
```

and from "build/Release", run:

```sh
cmake -DCMAKE_BUILD_TYPE=Release ../..
```

Then you can build Debug or Release binaries by running "make" inside the
relevant build directory.

## Other options

You can try other generators (compilers) supported by CMake. The main CMake
build script is the CMakeLists.txt located in the root directory.

# Extensions

FishStore is a general storage layer supporting different input data formats and general PSFs. Specifically, users can extend FishStore by implementing their own parser adapters and PSF libaries, for more details please refer to:

* [Extending FishStore with Parser Adapters](src/adapters/README.md)
* [Composing PSF Libraries](examples/lib_examples/README.md) 


# Submodules

This project references the following Git submodules:
* [hopscotch-map](https://github.com/Tessil/hopscotch-map): Licensed under MIT
* [simdjson](https://github.com/lemire/simdjson): Licensed under Apache 2.0

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
