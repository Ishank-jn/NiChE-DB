# NiChE (Nifty Cached Elements)
# Code is not fully tested, only for research/learning purposes.

NiChE is an ACID-compliant in-memory embedded key-value store, with support for basic networking capabilities.

## Features

- ACID-compliant operations: Supports Atomicity, Consistency, Isolation, and Durability.
- Embedded: No need for a separate database server.
- Basic networking: Provides a simple TCP/IP server for remote access to the key-value store (can be accessed on same port where service is running).
- Compression: support ZTSD or Snappy compression.
- Write-Ahead Logging (WAL): Improves durability and crash recovery.

## Requirements

- Golang
