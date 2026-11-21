#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# mimalloc tuning (musl port of GLIBC_TUNABLES):
#  PURGE_DELAY=-1: never madvise-free arenas -> avoid re-fault+re-zero across phases (arnabxero)
#  THP=1: transparent huge pages for large allocations (kaedeng glibc.malloc.hugetlb analog)
export MIMALLOC_PURGE_DELAY=-1 MIMALLOC_THP=1
exec /app/shopping-sessions
