#!/usr/bin/env bash

set -e

install() {
  cargo install rustfilt
  rustup component add llvm-tools-preview
  cargo install cargo-binutils

  bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
  ln -sf /usr/bin/llvm-cov-12 ~/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-cov
}

report() {
  find . -name *.prof* -exec rm -rf {} \;

  RUSTFLAGS="-Zinstrument-coverage" LLVM_PROFILE_FILE="target/cov/%m.profraw" cargo +nightly test --tests

  cargo profdata -- merge -sparse $(find . -name *.profraw) -o target/coverage.profdata

  object=$(
    for file in $(
      RUSTFLAGS="-Zinstrument-coverage" cargo +nightly test --tests --no-run --message-format=json |
        jq -r "select(.profile.test == true) | .filenames[]" |
        grep -v dSYM -
    ); do
      printf "%s %s " -object $file
    done
  )

  cargo cov -- report $object \
    --instr-profile=target/coverage.profdata \
    --ignore-filename-regex='(.cargo|rustc|glommio|scylla-rust-driver|testcontainers)' \
    --use-color

  cargo cov -- show $object \
    --instr-profile=target/coverage.profdata \
    --ignore-filename-regex='(.cargo|rustc|glommio|scylla-rust-driver|testcontainers)' \
    --format=html \
    --output-dir=target/cov/html \
    --use-color
}

${1:-report}
