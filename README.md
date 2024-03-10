# NiChE (Nifty Cached Elements)
# Still under works

NiChE is an ACID-compliant persistent embedded key-value store based on LevelDB, with support for basic networking capabilities.

## Features

- ACID-compliant operations: Supports Atomicity, Consistency, Isolation, and Durability.
- Persistent storage: Data is stored on disk and survives application restarts.
- Embedded: No need for a separate database server.
- Basic networking: Provides a simple TCP/IP server for remote access to the key-value store.
- Write-Ahead Logging (WAL): Improves durability and crash recovery.

## Requirements

- Python 3.x
- rocksdb
- pickle

## Installation

1. Install dependencies:

   ```bash
   pip install rocksdb
