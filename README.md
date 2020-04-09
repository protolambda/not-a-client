# Not-a-client

ZRNT (Eth2 consensus) + Rumor (Eth2 networking) + Go-remerkleable (State representation) = sync experiment.

Syncing lighthouse testnet at:
- 217 slots / second without BLS
- 49 slots / second with BLS (no signature-sets (e.g. batch all in blocks) or cached deserialized pubkeys).

Very experimental. Use at your own risk.

To sync lighthouse, you will need the `tree-lighthouse` branch of ZRNT (big refactor) and latest ZTYP (Go remerkleable, need a better name) linked in.

## License

MIT, see [`LICENSE`](./LICENSE) file.
