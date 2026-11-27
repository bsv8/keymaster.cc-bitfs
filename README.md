# Keymaster BitFS

TypeScript and Go libraries for BitFS buyers and sellers. Hosts supply authenticated transport, signing, wallet funding, chain observations, a durable workspace and final content publication.

- TypeScript exports `createBitfsBuyer`, `createBitfsSeller` and the seller protocol ports from `keymaster-bitfs`.
- Go hosts call `download.Run`, `download.Delete` and `seller.NewRuntime` in `github.com/bsv8/keymaster.cc-bitfs`.
- Buyer hosts can set the number of seats per seller with `schedulerSeatsPerSeller` in TypeScript or `download.Options.Config.SeatsPerSeller` in Go. The default is one seat.
- Playback hosts can read verified ranges and request missing blocks first through `readPlaybackRange` in TypeScript and the reader supplied by `download.Options.PlaybackReady` in Go. Go hosts can read a durable execution with `download.ReadPlaybackRange`.
- A successful output hook publishes the verified content to the host. The download workspace can then be deleted independently of the published file.

## Standalone commands

Build the Go CLI with `go build ./cmd/bitfs`. Build the TypeScript CLI with `pnpm install --frozen-lockfile` and `pnpm run build` in `typescript/`, then run `node dist/cli/main.js`. The Node CLI uses the system `flock` command for workspace ownership.

Both executables accept the same commands. A stopped buyer keeps its workspace, including approved payments and pending settlement evidence. Deleting the workspace's download data leaves the output path under the host's ownership.

```text
--config FILE serve
--config FILE get SEED
--config FILE resume SEED
--config FILE delete SEED
```

Use `SIGINT` or `SIGTERM` for a normal stop. Use the same identity and trade directory to resume a stopped or interrupted download. The seller indexes the configured directory on start.

Configuration is a JSON object with `Network`, `IdentityFile`, `ChainURL`, `Supplier`, `Listen`, `Arbiters`, `FeeRate`, `TradeDir`, `OutputDir`, `SellerDir`, `MaxBlockPrice`, `SeedPrice`, `BlockPrice` and optional `SeatsPerSeller`. `IdentityFile` names a private local file containing a 32-byte hex key. `Supplier` is the authenticated SSP gateway multiaddr. Each item in `Arbiters` has `publicKeyHex` and `address`. Price fields are decimal satoshi strings, and `FeeRate` is in satoshis per kilobyte.

For TypeScript installs that resolve fresh dependencies, keep `keymaster-multisig-pool@4.0.0` on `@bsv/sdk@2.1.7`, as in the CLI lockfile. This is the BSV SDK pairing used for the BitFS protocol flow. Browser bundles need the host's `crypto.createHmac` adapter for this transitive dependency. cc already supplies it.
