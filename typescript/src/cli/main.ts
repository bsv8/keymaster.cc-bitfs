#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { parseArgs } from "node:util";
import type { Libp2p, Stream } from "@libp2p/interface";
import { multiaddr } from "@multiformats/multiaddr";
import { newMessageID, parseSHA256Hash, publicKeyFromPrivate } from "bsv8-channel-protocol";
import { HASH_REQUEST_CHANNEL, marshal, newMultiaddrLocator, parseAndVerify, sign } from "bsv8-channel-protocol/hash-request";
import {
  BitfsSellerProtocol, type BitfsSellerRuntime, BitfsTransactionBroadcaster,
  createBitfsBuyer, createBitfsBuyerTask, createBitfsSeller, createBitfsFundingLedger, createBitfsJournal,
  createBitfsLocalSellerContentResolver, createBitfsSessionJournal, createBitfsTransactionJournal,
  deleteBitfsDownload, prepareBitfsFunding, SCHEDULER_HOST_DEFAULTS, type BitfsArbiter, type BsvNetwork,
} from "../index.js";
import { directoryStore, indexDirectory, lockDirectory, publishDownload } from "./files.js";
import { arbitrationPrices, BITFS_PROTOCOL_ID, openGateway, openHost, peerIdForKey, publicKeyHex, readArtifacts, sendFrame } from "./network.js";
import { openWallet, parseFundingTransaction } from "./wallet.js";

interface Config {
  Network: BsvNetwork;
  IdentityFile: string;
  ChainURL: string;
  Supplier: string;
  Listen: string;
  Arbiters: Array<{ publicKeyHex: string; address: string }>;
  FeeRate: number;
  MaxBlockPrice: string;
  TradeDir: string;
  OutputDir?: string;
  SellerDir?: string;
  SeedPrice: string;
  BlockPrice: string;
}

type Wallet = ReturnType<typeof openWallet>;
type Gateway = Awaited<ReturnType<typeof openGateway>>;

function emit(value: unknown): void { process.stdout.write(JSON.stringify(value) + "\n"); }

function traceError(error: unknown): void {
  console.error(error instanceof Error ? error.message : String(error));
}

async function get(config: Config, key: Uint8Array, wallet: Wallet, host: Libp2p,
  gateway: Gateway, seed: string, resume: boolean, stop: AbortSignal, lifecycle: AbortController,
): Promise<void> {
  const output = config.OutputDir;
  if (!output) throw new Error("BitFS OutputDir is required");
  const folder = join(config.TradeDir, "buyer", config.Network, wallet.owner);
  const journal = directoryStore(join(folder, "journal"));
  const content = directoryStore(join(folder, "download"));
  const lock = (name: string) => lockDirectory(join(folder, "locks"), name);
  const sessions = createBitfsSessionJournal(journal);
  const ledger = createBitfsFundingLedger(journal);
  const transactions = createBitfsTransactionJournal(journal);
  const broadcaster = new BitfsTransactionBroadcaster({ journal: transactions, chain: wallet.chain, nowMs: Date.now });
  let failure: unknown;
  let finalPath: string | undefined;
  let release!: () => void;
  const done = new Promise<void>(resolve => { release = resolve; });
  let demandId: string | undefined;
  const buyer = createBitfsBuyer({
    identity: () => ({ owner: wallet.owner, network: config.Network, generation: 1 }),
    settings: async () => ({ ...SCHEDULER_HOST_DEFAULTS, maxFullBlockPriceSatoshis: config.MaxBlockPrice,
      buyerAutoPurchaseEnabled: true, maxConcurrentDownloads: 1, refundDelayBlocks: 5 }),
    signer: wallet.signer, content, journal, lifecycle: lifecycle.signal, tryLock: lock,
    publishDemand: async (seedHashHex, signal, prepared) => {
      const address = host.getMultiaddrs()[0];
      if (!address) throw new Error("BitFS listening address unavailable");
      const from_public_key = publicKeyFromPrivate(key);
      const message_id = newMessageID();
      const issued_at_ms = Date.now();
      const expires_at_ms = issued_at_ms + 600_000;
      const signed = sign({ from_public_key, message_id, issued_at_ms, expires_at_ms,
        body: { hash: parseSHA256Hash(seedHashHex), locators: [newMultiaddrLocator(address.toString())] },
      }, key);
      demandId = message_id;
      prepared(message_id);
      await gateway.publish(HASH_REQUEST_CHANNEL, marshal(signed), signal);
      return { requestMessageId: message_id, expiresAtMs: expires_at_ms };
    },
    funding: (seedHashHex, signal) => createBitfsBuyerTask({
      sessions, ledger, transactions, broadcaster, signer: wallet.signer, ownerPublicKeyHex: wallet.owner,
      ownerP2pkhScriptHex: wallet.scriptHex, seedHashHex, network: config.Network, generation: 1,
      parseTransaction: parseFundingTransaction, assertCurrentContext: () => signal.throwIfAborted(), nowMs: Date.now,
      blockHeight: wallet.blockHeight, releasePreparedSubmission: wallet.releasePreparedSubmission,
      prepareFunding: args => prepareBitfsFunding({
        sessionId: args.sessionId, ownerPublicKeyHex: wallet.owner, seedHashHex, network: config.Network, generation: 1,
        openingOutput: args.openingOutput, feeRateSatoshisPerKb: config.FeeRate, maxFeeSatoshis: 100_000,
      }, {
        protocolSpend: wallet.protocolSpend, planFunding: wallet.planFunding, releaseFundingPlan: wallet.releaseFundingPlan,
        resolveOwnerAddress: async () => ({ address: wallet.address, scriptHex: wallet.scriptHex }),
        parseTransaction: parseFundingTransaction, transactions, sessions, ledger,
        assertCurrentContext: () => signal.throwIfAborted(), nowMs: Date.now,
      }),
    }),
    arbitrationServices: async () => config.Arbiters,
    arbitrationPrices: (arbiter, signal) => arbitrationPrices(host, arbiter, signal),
    blockHeight: wallet.blockHeight, feeRate: () => BigInt(config.FeeRate),
    async commit(content) { finalPath = await publishDownload(output, content); },
    onError(_seed, error) { failure = error; traceError(error); },
    onStopped: release,
    onView(_seed, view) { emit({ state: view.execution, funds: view.funds, completed: view.completed, total: view.total }); },
  });
  await host.handle(BITFS_PROTOCOL_ID, async (stream, connection) => {
    const sessionId = crypto.randomUUID();
    const signal = demandId && buyer.registerOffer({ sessionId, seedHashHex: seed,
      publicKeyHex: publicKeyHex(connection), requestMessageId: demandId,
      send: (frame, signal) => sendFrame(stream, frame, signal),
      close: () => stream.close(),
    });
    if (!signal) {
      await stream.close();
      return;
    }
    try {
      const signalForRead = AbortSignal.any([lifecycle.signal, signal]);
      for await (const artifact of readArtifacts(stream, { signal: signalForRead })) {
        await buyer.onTransportEvent({ type: "frame", sessionId, frame: artifact.bytes() });
      }
    } catch (error) {
      if (!signal.aborted) traceError(error);
    } finally {
      await buyer.onTransportEvent({ type: "closed", sessionId });
    }
  });
  const cancel = () => { void buyer.stop(seed).catch(error => { failure = error; release(); }); };
  stop.addEventListener("abort", cancel, { once: true });
  try {
    const snapshot = await (resume ? buyer.resume(seed) : buyer.download(seed));
    if (snapshot.requestMessageId === null) throw new Error("BitFS 下载仍在运行");
    if (stop.aborted) cancel();
    await done;
    if (failure) throw failure;
    if (finalPath) emit({ state: "completed", seed, path: finalPath });
    else if (stop.aborted) emit({ state: "paused", seed });
    else throw new Error("BitFS download stopped before a file was completed");
  } finally {
    stop.removeEventListener("abort", cancel);
    await buyer.close();
  }
}

function routes(request: Parameters<BitfsSellerRuntime["match"]>[0]) {
  const peerSuffix = "/p2p/" + peerIdForKey(request.from_public_key);
  const addresses = request.body.locators.flatMap(locator => locator.kind === "multiaddr" && locator.address.endsWith(peerSuffix) ? [locator.address] : []);
  return addresses.length ? { transport: "multiaddr" as const, addresses } : undefined;
}

async function serve(config: Config, wallet: Wallet, host: Libp2p,
  gateway: Gateway, stop: AbortSignal, lifecycle: AbortController,
): Promise<void> {
  if (!config.SellerDir) throw new Error("BitFS SellerDir is required");
  const folder = join(config.TradeDir, "seller", config.Network, wallet.owner);
  const release = await lockDirectory(join(folder, "locks"), "seller");
  if (!release) throw new Error("BitFS seller is already running");
  try {
    const catalog = await indexDirectory(config.SellerDir);
    const journal = directoryStore(join(folder, "journal"));
    const sessions = createBitfsSessionJournal(journal);
    const transactions = createBitfsTransactionJournal(journal);
    const broadcaster = new BitfsTransactionBroadcaster({ journal: transactions, chain: wallet.chain, nowMs: Date.now });
    const nowMs = Date.now;
    const runtimeInstanceId = crypto.randomUUID();
    const protocol = new BitfsSellerProtocol({
      signer: wallet.signer, ownerPublicKeyHex: wallet.owner, sessions, broadcaster, nowMs,
      generation: () => 1, runtimeInstanceId: () => runtimeInstanceId,
      content: createBitfsLocalSellerContentResolver({ content: catalog.content, nowMs, blockHeight: wallet.blockHeight }),
      blockHeight: wallet.blockHeight, minRefundDelayBlocks: () => 2,
      async arbitrationFee(arbiterPublicKeyHex, itemCount) {
        const endpoint = config.Arbiters.find(item => item.publicKeyHex === arbiterPublicKeyHex);
        if (!endpoint) throw new Error("Missing configured BitFS arbiter");
        const pricing = await arbitrationPrices(host, endpoint, lifecycle.signal);
        return BigInt(pricing.base) + BigInt(itemCount) * BigInt(pricing.perItem);
      },
    });
    const streams = new Map<string, Stream>();
    const workers = new Set<Promise<unknown>>();
    let wake!: () => void;
    const stopping = new Promise<void>(resolve => { wake = resolve; });
    const onStop = () => wake();
    stop.addEventListener("abort", onStop, { once: true });
    const seller = createBitfsSeller({
      discovery: {
      signer: wallet.signer, journal: createBitfsJournal(journal), sessions, nowMs,
      findContent: async hash => catalog.find(hash), selectTransport: routes,
      settings: () => ({ sellerEnabled: !stop.aborted, seedPriceSatoshis: config.SeedPrice,
        fullBlockPriceSatoshis: config.BlockPrice, maxBlocksPerBatch: 64, quoteLifetimeSeconds: 300,
        supportedArbiterPublicKeys: config.Arbiters.map(item => item.publicKeyHex) }),
    },
      peerIdForKey,
      protocol, nowMs, idleTimeoutMs: () => 90_000, maxSessions: () => 32,
      onActiveSessionsChanged() {}, isCurrent: () => !lifecycle.signal.aborted,
      onProtocolError: ({ message }) => traceError(message),
      transport: {
        async open(input) {
          let cause: unknown;
          for (const address of input.addresses) {
            const signal = AbortSignal.any([lifecycle.signal, AbortSignal.timeout(15_000)]);
            let stream: Stream | undefined;
            try {
              const connection = await host.dial(multiaddr(address), { signal });
              if (publicKeyHex(connection) !== input.publicKeyHex) throw new Error("BitFS demand identity mismatch");
              const live = await connection.newStream(BITFS_PROTOCOL_ID, { signal });
              stream = live;
              streams.set(input.sessionId, live);
              const read = (async () => {
                try {
                  for await (const artifact of readArtifacts(live, { signal: lifecycle.signal })) {
                    await seller.handleFrame({ sessionId: input.sessionId, frame: artifact.bytes() });
                  }
                } finally { await seller.closeSession(input.sessionId, "remote_closed"); }
              })();
              workers.add(read);
              void read.catch(traceError).finally(() => workers.delete(read));
              await sendFrame(live, input.firstFrame, signal);
              return;
            } catch (error) {
              cause = error;
              stream?.abort(error instanceof Error ? error : new Error("Failed to open BitFS seller stream"));
              if (stream) break;
            }
          }
          throw cause ?? new Error("BitFS seller found no usable locator");
        },
        async send(id, frame, signal) {
          const stream = streams.get(id);
          if (!stream) throw new Error("BitFS seller stream is closed");
          await sendFrame(stream, frame, signal);
        },
        async close(id) {
          const stream = streams.get(id);
          streams.delete(id);
          await stream?.close();
        },
      },
    });
    await gateway.subscribe(HASH_REQUEST_CHANNEL, (channel, body) => {
      if (stop.aborted || workers.size >= 32) return;
      const handle = (async () => {
        const request = parseAndVerify(channel, body);
        await seller.accept(request);
      })();
      workers.add(handle);
      void handle.catch(error => { if (!stop.aborted) traceError(error); }).finally(() => workers.delete(handle));
    });
    emit({ state: "serving", peer: host.peerId.toString(), indexed: catalog.size });
    if (!stop.aborted) await stopping;
    stop.removeEventListener("abort", onStop);
    await seller.stop();
    lifecycle.abort();
    await Promise.allSettled(workers);
  } finally { release(); }
}

async function main(): Promise<void> {
  const { values, positionals } = parseArgs({
    options: { config: { type: "string" }, help: { type: "boolean", short: "h" } }, allowPositionals: true,
  });
  if (values.help) {
    process.stdout.write("usage: keymaster-bitfs --config FILE get|resume|delete SEED or keymaster-bitfs --config FILE serve\n");
    return;
  }
  const [command, seed] = positionals;
  if (!values.config || !(command === "serve" && positionals.length === 1
    || ["get", "resume", "delete"].includes(command ?? "") && positionals.length === 2 && /^[0-9a-f]{64}$/u.test(seed ?? ""))) {
    throw new Error("Use --help for BitFS command arguments");
  }
  const config: Config = { Network: "test", Listen: "/ip4/127.0.0.1/tcp/0", FeeRate: 100,
    MaxBlockPrice: "1", SeedPrice: "1", BlockPrice: "1", Arbiters: [],
    ...JSON.parse(await readFile(values.config, "utf8")) };
  if (!config.IdentityFile || !config.TradeDir || !config.ChainURL || (config.Network !== "main" && config.Network !== "test")) {
    throw new Error("BitFS IdentityFile, TradeDir, ChainURL and Network are required");
  }
  const keyHex = (await readFile(config.IdentityFile, "utf8")).trim();
  if (!/^[0-9a-f]{64}$/iu.test(keyHex)) throw new Error("BitFS identity file requires a 32-byte hex key");
  const key = Uint8Array.from(Buffer.from(keyHex, "hex"));
  const wallet = openWallet(keyHex, config.Network, config.ChainURL);
  if (command === "delete") {
    const root = join(config.TradeDir, "buyer", config.Network, wallet.owner);
    const result = await deleteBitfsDownload({
      seedHashHex: seed!, ownerPublicKeyHex: wallet.owner, network: config.Network,
      content: directoryStore(join(root, "download")), journal: directoryStore(join(root, "journal")),
      tryLock: name => lockDirectory(join(root, "locks"), name),
    });
    emit({ state: result.recoveryPending ? "recovery-pending" : "deleted", seed });
    return;
  }
  if (!config.Supplier || !config.Arbiters.length) throw new Error("BitFS Supplier and Arbiters are required");
  const stopping = new AbortController();
  const lifecycle = new AbortController();
  const stop = () => stopping.abort();
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  const host = await openHost(key, config.Listen);
  try {
    const gateway = await openGateway(host, config.Supplier, lifecycle.signal);
    try {
      if (command === "serve") await serve(config, wallet, host, gateway, stopping.signal, lifecycle);
      else await get(config, key, wallet, host, gateway, seed!, command === "resume", stopping.signal, lifecycle);
    } finally { await gateway.close(); }
  } finally {
    lifecycle.abort();
    await host.stop();
    key.fill(0);
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
  }
}

main().catch(error => {
  traceError(error);
  process.exitCode = 1;
});
