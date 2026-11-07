# How to Build Beam

This guide shows how to build Beam from source on **macOS**. It also has a short section for **Linux (Ubuntu)**.

You will get these programs:

| Program | What it is |
|---|---|
| `beam-node` | The Beam node |
| `beam-wallet` | Command-line wallet |
| `wallet-api` | Wallet API server |
| `explorer-node` | Block explorer node |
| `broadcaster` | Broadcaster tool |
| `ethash-service` | Ethash service |

The build takes about 15–30 minutes, depending on your computer.

---

## macOS

### Step 1. Install the tools

You need Homebrew. If you don't have it, install it from https://brew.sh.

Then open Terminal and run:

```bash
xcode-select --install
brew install cmake boost openssl@3
```

- `xcode-select --install` installs the C++ compiler and git. If they are already installed, it tells you so. That is fine.
- You need Boost **1.83 or newer**.

### Step 2. Get the source code

```bash
git clone https://github.com/BeamMW/beam.git
cd beam
git submodule update --init --recursive
```

The last command downloads extra libraries that Beam needs (curl, protobuf, re2 and others). **Don't skip it.** Without it, the build fails.

### Step 3. Set the UTF-8 locale

```bash
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

Without this, the build can fail with this error:

```
Pathname can't be converted from UTF-8 to current locale
```

To check your locale, run `locale`. If it shows `"C"`, you need this step. To keep the setting for all new terminal windows, add both lines to your `~/.zshrc` file.

### Step 4. Configure the build

Run this from the `beam` folder:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBEAM_HW_WALLET=Off \
  -DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3)
```

The output should end with:

```
-- Configuring done
-- Generating done
```

You can ignore warnings like `Policy CMP0167 is not set`.

### Step 5. Build

```bash
cmake --build build --parallel $(sysctl -n hw.ncpu)
```

This is the longest step. At the end you may see warnings like these. They are harmless:

```
ld: warning: ignoring duplicate libraries ...
ld: warning: ... has malformed LC_DYSYMTAB ...
```

The build succeeded if the last line looks like `[100%] Built target ...` and there are no `Error` lines.

### Step 6. Find and run the programs

All programs are in the `build` folder:

```
build/beam/beam-node
build/wallet/cli/beam-wallet
build/wallet/api/wallet-api
build/explorer/explorer-node
build/wallet/broadcaster/broadcaster
build/bvm/ethash_service/ethash-service
```

To check that they work, run:

```bash
./build/beam/beam-node --version
```

It prints the version number, for example `7.5.14613`.

### Step 7 (optional). Run the tests

```bash
cd build
ctest
```

---

## Linux (Ubuntu)

The steps are almost the same as on macOS.

1. Install the tools:

   ```bash
   sudo apt-get update
   sudo apt-get install build-essential git cmake libssl-dev libboost-all-dev curl
   ```

   Check that your Boost version is 1.83 or newer. Older Ubuntu releases may have an older Boost.

2. Get the source code (same as macOS Step 2).

3. Configure and build:

   ```bash
   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBEAM_HW_WALLET=Off
   cmake --build build --parallel $(nproc)
   ```

The programs are in the same places inside the `build` folder.

---

## Useful build options

Add these to the command in Step 4 if you need them.

| Option | What it does |
|---|---|
| `-DCMAKE_BUILD_TYPE=Debug` | Builds a debug version, which is easier to debug but slower. Use a separate folder for it, for example `-B build-debug`. |
| `-DBEAM_HW_WALLET=On` | Adds hardware wallet (Trezor) support. |
| `-DBEAM_LINK_TYPE=Static` | Links libraries statically, as the official releases do. Needs static versions of Boost and OpenSSL. |

---

## Common problems

**Error: `Pathname can't be converted from UTF-8 to current locale`**
Do Step 3, then run Step 5 again.

**Error: CMake can't find Boost or OpenSSL**
Make sure they are installed (Step 1). On macOS, make sure you added `-DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3)` in Step 4.

**Error about missing files in `3rdparty/...`**
You skipped the submodules. Run `git submodule update --init --recursive`, then try again.

**You want to start over**
Delete the `build` folder and repeat Steps 4 and 5:

```bash
rm -rf build
```
