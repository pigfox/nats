# nats

A practical, production-oriented example of using **NATS** and **JetStream** with Go.

This repository demonstrates:
- Core NATS pub/sub and request–reply
- Queue groups (load-balanced consumers)
- JetStream streams and durable pull consumers
- Explicit acknowledgements and redelivery semantics
- Integration tests against a real NATS server
- Deterministic Docker-based test execution
- Chaos testing by restarting NATS mid-test
- High test coverage (80%+)

---

## What this repo is

This is **not** a toy example.

It is a minimal but realistic reference for how you would:
- wrap the NATS Go client in a small abstraction,
- use JetStream correctly for durability and replay,
- write reliable integration tests,
- and run everything consistently via Docker.

---

## Requirements

- Docker + Docker Compose
- No local Go installation required (tests run in Docker)
- Go version used in tests: **1.25.5**

---

## Quick start

Run everything (NATS + tests + chaos test):

```sh
./run.sh
