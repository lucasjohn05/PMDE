# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go service that ingests live order book data from Kalshi and Polymarket over
WebSocket and measures price divergence between venues quoting the same event.

Module: `github.com/lucasjohn05/pmde`, Go 1.27.

## Hard constraints

- **Dependencies**: `gorilla/websocket` and `prometheus/client_golang` only.
  Ask before adding anything else.
- **Prices**: integer basis points (0-10000) everywhere. Never use floats for
  price.
- **No venue abstraction**: no `Exchange` interface, no plugin/registry
  pattern. Two concrete clients, `kalshi` and `polymarket`, each producing a
  shared `Tick` struct.
- **No auto-matching**: venue pairs (which Kalshi market corresponds to which
  Polymarket market) are hand-written in a YAML config. Do not build any
  automatic market-matching logic.
- **Backpressure**: channels between stages are bounded and block on full.
  Never drop order book delta messages.

## Working style

Build in small steps and stop after each one — do not scaffold ahead of what
was asked.
