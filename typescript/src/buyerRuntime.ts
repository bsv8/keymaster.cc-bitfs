import { hexToBytes } from "bitcoin-libp2p/identity";
import { parse, type Signer } from "bitfs-protocol";
import { Digest, verifyBlock } from "masterseed";
import type { Command, Payload, View } from "keymaster-trade-scheduler";
import { acceptBitfsBuyerQuote, type BitfsBuyerQuoteView } from "./buyerTask.js";
import { createBitfsBuyerDownloadPlan } from "./buyerDownloadPlan.js";
import { BitfsBuyerProtocol, readVerifiedSeed, type BitfsBuyerPurchaseProgress, type BitfsVerifiedDownload } from "./buyerProtocol.js";
import { traceBitfs } from "./debug.js";
import { deleteBitfsDownload, downloadDeletionPath, downloadIntentPath, downloadLockKey } from "./deletion.js";
import { bitfsPoolSettlementFee } from "./poolBudget.js";
import { createBitfsFundingLedger } from "./funding.js";
import { selectBitfsMediaType } from "./mediaTypeVote.js";
import { createSchedulerPoolSession, findBuyerPoolResponseSession, readSchedulerPoolSessionId, schedulerPoolSessionId } from "./poolRouting.js";
import { createBitfsPurchaseTasks } from "./purchaseTasks.js";
import { createBitfsSessionJournal } from "./sessionJournal.js";
import { MonotonicClock, SchedulerAdapter, SchedulerCommandExecutor, createSchedulerSnapshotStore, sharedDownloadScheduler, type SchedulerHostSettings } from "./scheduler/index.js";
import { schedulerCloseAmounts, schedulerOpeningBudget } from "./scheduler/funding.js";
import type { BitfsBuyerFunding, BitfsStore, BsvNetwork } from "./ports.js";

export interface BitfsBuyerSettings extends SchedulerHostSettings {
  buyerAutoPurchaseEnabled: boolean;
  maxConcurrentDownloads: number;
  refundDelayBlocks: number;
}

export interface BitfsBuyerOffer {
  sessionId: string;
  seedHashHex: string;
  publicKeyHex: string;
  requestMessageId: string;
  send(frame: Uint8Array, signal: AbortSignal): Promise<void>;
  close(): Promise<void>;
}

export interface BitfsBuyerIdentity {
  owner: string;
  network: BsvNetwork;
  generation: number;
}

export interface BitfsArbiter {
  publicKeyHex: string;
  address: string;
}

export interface BitfsBuyerOptions {
  identity(): BitfsBuyerIdentity;
  settings(): Promise<BitfsBuyerSettings>;
  signer: Signer | ((seedHashHex: string, signal: AbortSignal) => Signer);
  funding(seedHashHex: string, signal: AbortSignal): BitfsBuyerFunding;
  content: BitfsStore;
  journal: BitfsStore;
  lifecycle: AbortSignal;
  tryLock(key: string): Promise<(() => void) | undefined>;
  publishDemand(seedHashHex: string, signal: AbortSignal, onPrepared: (id: string) => void): Promise<{ requestMessageId: string; expiresAtMs: number }>;
  withdrawDemand?(seedHashHex: string, requestMessageId: string): Promise<unknown>;
  arbitrationServices(): Promise<BitfsArbiter[]>;
  arbitrationPrices(arbiter: BitfsArbiter, signal: AbortSignal): Promise<{ base: string; perItem: string }>;
  blockHeight(): Promise<number>;
  feeRate(): bigint;
  commit(content: BitfsVerifiedDownload, signal: AbortSignal): Promise<void>;
  /** Accept an early offer already authenticated and routed by the host. */
  acceptOfferBeforePublishResult?: boolean;
  onView?(seedHashHex: string, view: View): void;
  onError?(seedHashHex: string, error: unknown): void;
  onStopped?(seedHashHex: string, committed: boolean): void;
  stopTimeoutMs?: number;
}

function isValidSeed(seed: string): boolean { return /^[0-9a-f]{64}$/u.test(seed); }

function schedulerSuccessSettled(view: View): boolean {
  return view.execution === "finished" && (view.funds === "settled" || view.funds === "none");
}

function schedulerFailureSettled(view: View): boolean {
  return view.execution === "stopped" && !!view.failure && view.funds !== "pending";
}

const SEARCH_TIMEOUT_MS = 10 * 60_000;

function quoteArbiter(task: Download, quote: BitfsBuyerQuoteView, endpoints: BitfsArbiter[]) {
  return endpoints.find(endpoint => endpoint.publicKeyHex !== task.identity.owner
    && endpoint.publicKeyHex !== quote.sellerPublicKeyHex && quote.supportedArbiterPublicKeys.includes(endpoint.publicKeyHex));
}


export type BitfsBuyerTransportEvent =
  | { type: "frame"; sessionId: string; frame: Uint8Array }
  | { type: "closed"; sessionId: string }
  | { type: "progress"; sessionId: string; bytesIn: number };

type Link = {
  offer: BitfsBuyerOffer;
  quote?: BitfsBuyerQuoteView;
  protocols: Map<string, BitfsBuyerProtocol>;
  closed: boolean;
  bytesIn?: number;
  requestProgress?: { id: string; start: number; sent: number };
};
type Purchase = {
  id: string;
  adapter: SchedulerAdapter;
  executor: SchedulerCommandExecutor;
  plan: ReturnType<typeof createBitfsBuyerDownloadPlan>;
  manifest: { recommendedFilename: string; fileSizeBytes: string };

};
type Download = {
  identity: BitfsBuyerIdentity;
  seed: string;
  settings: BitfsBuyerSettings;
  controller: AbortController;
  release(): void;
  links: Map<string, Link>;
  expiresAtMs: number;
  requestId?: string;
  tail: Promise<void>;
  timer?: ReturnType<typeof setTimeout>;
  wake?: ReturnType<typeof setTimeout>;
  purchase?: Purchase;
  progress?: BitfsBuyerPurchaseProgress;
  committed?: boolean;
  stopped?: boolean;
  closeTimer?: ReturnType<typeof setTimeout>;
  closing?: boolean;
  releaseSlot?: () => void;
  finished: Promise<void>;
  completeStop(): void;
  runId: string;
};

export interface BitfsBuyerSnapshot {
  seedHashHex: string;
  requestMessageId: string | null;
  expiresAtMs: number;
  currentMaxFullBlockPriceSatoshis: string | null;
  quotes: BitfsBuyerQuoteView[];
  purchase: BitfsBuyerPurchaseProgress | null;
  existingPurchaseSessionId: string | null;
}

export function createBitfsBuyer(options: BitfsBuyerOptions) {
  const { content, journal, lifecycle } = options;
  const journalSessions = createBitfsSessionJournal(journal);
  const ledger = createBitfsFundingLedger(journal);
  const clock = new MonotonicClock();
  const downloads = new Map<string, Download>();
  const frames = new Map<string, { task: Download; link: Link }>();

  function assertCurrent(task: Download): void {
    lifecycle.throwIfAborted();
    task.controller.signal.throwIfAborted();
    const current = options.identity();
    if (task.identity.generation !== current.generation || task.identity.owner !== current.owner
      || task.identity.network !== current.network || downloads.get(task.seed) !== task) {
      throw new Error("BitFS 下载身份已变化");
    }
  }

  function signer(task: Download): Signer {
    const port = typeof options.signer === "function" ? options.signer(task.seed, task.controller.signal) : options.signer;
    return {
      publicKey: () => port.publicKey(),
      async sign(request, signal) {
        assertCurrent(task);
        const signature = await port.sign(request, AbortSignal.any(signal
          ? [signal, task.controller.signal] : [task.controller.signal]));
        assertCurrent(task);
        return signature;
      },
    };
  }

  function serial(task: Download, work: () => Promise<void>): Promise<void> {
    const run = task.tail.then(async () => {
      assertCurrent(task);
      await work();
    });
    task.tail = run.catch(() => undefined);
    return run;
  }

  function quoteViews(task: Download): BitfsBuyerQuoteView[] {
    return [...task.links.values()].filter(link => !link.closed && link.quote
      && Number(link.quote.quoteExpiresAtUnixSeconds) * 1_000 > Date.now())
      .map(link => link.quote!);
  }

  const snapshot = (task: Download): BitfsBuyerSnapshot => ({
    seedHashHex: task.seed, requestMessageId: task.requestId ?? null, expiresAtMs: task.expiresAtMs,
    currentMaxFullBlockPriceSatoshis: task.purchase?.adapter.view().price.effective ?? null,
    quotes: quoteViews(task), purchase: task.progress ?? null,
    existingPurchaseSessionId: task.purchase?.id ?? null,
  });

  async function readOther(seed: string): Promise<BitfsBuyerSnapshot> {
    if (!isValidSeed(seed)) throw new Error("BitFS Seed Hash 无效");
    const current = options.identity();
    const active = (await createBitfsPurchaseTasks(journal, current.owner, seed).list()).find(task => task.state === "active");
    return { seedHashHex: seed, requestMessageId: null, expiresAtMs: 0, quotes: [],
      purchase: null, currentMaxFullBlockPriceSatoshis: null, existingPurchaseSessionId: active?.taskId ?? null };
  }

  function notifyFailure(task: Download, cause: unknown): void {
    traceBitfs("buyer.download.failed", () => ({ seedHashHex: task.seed, error: cause }));
    try { options.onError?.(task.seed, cause); } catch (error) { traceBitfs("buyer.observer.failed", () => ({ error })); }
  }

  async function drop(task: Download, cause?: unknown): Promise<void> {
    if (task.stopped) return;
    task.stopped = true;
    task.controller.abort();
    if (task.timer) clearTimeout(task.timer);
    if (task.wake) clearTimeout(task.wake);
    if (task.closeTimer) clearTimeout(task.closeTimer);
    const adapterDrain = task.purchase?.adapter.retire();
    for (const link of task.links.values()) frames.delete(link.offer.sessionId);
    if (cause) notifyFailure(task, cause);
    const closes = [...task.links.values()].map(link => link.offer.close());
    try {
      const routing = task.requestId ? options.withdrawDemand?.(task.seed, task.requestId) : undefined;
      await Promise.allSettled([adapterDrain, routing, ...closes]);
    } finally {
      if (downloads.get(task.seed) === task) downloads.delete(task.seed);
      task.completeStop();
      try { options.onStopped?.(task.seed, task.committed === true); } catch (error) { traceBitfs("buyer.observer.failed", () => ({ error })); }
      task.releaseSlot?.();
      task.release();
    }
  }

  async function commit(task: Download): Promise<void> {
    if (task.committed) return;
    assertCurrent(task);
    const purchase = task.purchase!;

    const seedPath = `bitfs-staging/by-seed/${task.seed}/seed.bin`;
    const seed = await content.get(seedPath, { signal: task.controller.signal });
    if (!seed) throw new Error("BitFS 下载缺少已验收 Seed");
    const ordered = await readVerifiedSeed(task.seed, seed.bytes);
    const verifiedHashes = new Set(ordered);
    const { recommendedFilename, fileSizeBytes } = purchase.manifest;
    await options.commit({
      sessionId: purchase.id, seedHashHex: task.seed, seedBytes: seed.bytes, blockHashes: ordered,
      manifest: { format: "keymaster.bitfs-purchase-manifest", version: 1, seedHashHex: task.seed,
        recommendedFilename, fileSizeBytes, mediaType: selectBitfsMediaType(quoteViews(task)) },
      readBlock: async hash => {
        if (!verifiedHashes.has(hash)) throw new Error("BitFS Block 不在已验收 Seed 中");
        const block = await content.get(`bitfs-staging/by-seed/${task.seed}/blocks/${hash}.bin`, { signal: task.controller.signal });
        if (!block) throw new Error("BitFS 下载缺少已验收 Block");
        verifyBlock(block.bytes, Digest.fromHex(hash));
        return block.bytes;
      },
    }, task.controller.signal);
    assertCurrent(task);
    task.committed = true;
  }

  async function finish(task: Download, reason: "completed" | "cancelled" | "failed"): Promise<void> {
    if (task.purchase) {
      await createBitfsPurchaseTasks(journal, task.identity.owner, task.seed).end(task.purchase.id, reason, Date.now());
    }
    if (reason === "completed") {
      try { await journal.delete(downloadIntentPath(task.seed)); }
      catch (error) { traceBitfs("buyer.intent.cleanup.failed", () => ({ error })); }
    }
    await drop(task);
  }

  async function stopNormally(task: Download): Promise<void> {
    if (task.closing) return;
    if (!task.purchase) { await drop(task); return; }
    task.closing = true;
    task.closeTimer = setTimeout(() => { void drop(task); }, options.stopTimeoutMs ?? 30_000);
    await event(task, `pause-${task.runId}`, { type: "pause" });
  }

  async function event(task: Download, eventId: string, payload: Payload): Promise<void> {
    assertCurrent(task);
    const runtime = task.purchase;
    if (!runtime) return;
    const commands = await runtime.adapter.handle({ eventId, payload });
    await runCommands(task, commands);
    if (task.stopped) return;
    await updateView(task, runtime);
  }

  async function runCommands(task: Download, commands: readonly Command[]): Promise<void> {
    const runtime = task.purchase!;
    for (const command of commands) {
      assertCurrent(task);
      try {
        await runtime.executor.run(command);
      } catch (error) {
        assertCurrent(task);
        traceBitfs("buyer.scheduler.command.failed", () => ({ commandId: command.commandId, type: command.type, error }));
        if (command.type === "open") {
          const saved = await journalSessions.get(schedulerPoolSessionId(runtime.id, command.poolId));
          const signed = saved?.evidence.some(name => name === "kind2-opening-request" || name === "funding-transaction");
          const code = (error as { code?: string })?.code;
          const insufficient = code === "insufficient" || code === "no-utxos" || code === "insufficient_funds";
          if (!signed && !insufficient) {
            await event(task, `open-offline-${command.commandId}`, { type: "seller", sellerId: command.sellerId, online: false });
          }
          await event(task, `open-error-${command.commandId}`, signed
            ? { type: "unknown", poolId: command.poolId, commandId: command.commandId }
            : { type: "open-failed", poolId: command.poolId, commandId: command.commandId, released: true,
                insufficientFunds: insufficient });
        } else if (command.type === "download") {
          const id = schedulerPoolSessionId(runtime.id, command.poolId);
          const record = await journalSessions.get(id);
          const safeToRetry = record?.phase === "funded" && !record.pendingAuthorizationId;
          if (safeToRetry) await runtime.plan.releaseDispatch(id, []);
          await event(task, `request-error-${command.commandId}`, { type: "request-rejected", requestId: command.requestId, safeToRetry });
        } else if (command.type === "close") {
          await event(task, `close-error-${command.commandId}`, { type: "unknown", poolId: command.poolId, commandId: command.commandId });
        } else {
          throw error;
        }
        await runtime.adapter.settle(command.commandId);
      }
    }
  }

  async function updateView(task: Download, runtime: Purchase): Promise<void> {
    const view = runtime.adapter.view();
    try { options.onView?.(task.seed, view); } catch (error) { traceBitfs("buyer.observer.failed", () => ({ error })); }
    if (task.stopped) return;
    if (task.closing && view.execution === "paused" && view.funds !== "pending") {
      await drop(task);
      return;
    }
    if (schedulerSuccessSettled(view) || schedulerFailureSettled(view)) {
      if (schedulerSuccessSettled(view)) await commit(task);
      if (view.failure) notifyFailure(task, new Error(`BitFS download failed: ${view.failure.code}`));
      await finish(task, task.committed ? "completed" : view.failure ? "failed" : "cancelled");
      return;
    }
    if (!task.closing && Date.now() >= task.expiresAtMs && view.reasons.includes("no-eligible-seller")
      && !(view.content === "manifest-unknown" && runtime.adapter.snapshot().completed[task.seed] === "purchased")) {
      await stopNormally(task);
      return;
    }
    if (task.wake) clearTimeout(task.wake);
    const next = runtime.adapter.nextWakeAt();
    if (next !== null) {
      task.wake = setTimeout(() => {
        void serial(task, () => event(task, `tick-${next}-${runtime.adapter.snapshot().revision}`, { type: "tick" }))
          .catch(error => drop(task, error));
      }, Math.max(0, next - Date.now()));
    }
  }

  function stream(task: Download, link: Link) {
    return { async send(frame: Uint8Array) {
      assertCurrent(task);
      if (link.closed) throw new Error("卖家连接已关闭");
      await link.offer.send(frame, task.controller.signal);
    } };
  }

  function scheduleFact(task: Download, record: () => Promise<void>): void {
    void serial(task, record).catch(error => drop(task, error));
  }

  async function protocolFor(task: Download, link: Link, sessionId: string): Promise<BitfsBuyerProtocol> {
    const old = link.protocols.get(sessionId);
    if (old) return old;
    const runtime = task.purchase!;
    const rate = options.feeRate();
    const protocol = new BitfsBuyerProtocol({
      sessions: journalSessions,
      task: options.funding(task.seed, task.controller.signal),
      signer: signer(task),
      contentStore: content,
      transportSignal: task.controller.signal,
      commitContent: () => commit(task),
      downloadPlan: runtime.plan, dispatchMode: true, selectionPriority: "recent-speed",
      refundDelayBlocks: task.settings.refundDelayBlocks, minerFeeRateSatoshisPerKilobyte: BigInt(rate),
      arbitrationServices: async () => {
        const selected = quoteArbiter(task, link.quote!, await options.arbitrationServices());
        return selected ? [selected] : [];
      },
      nowMs: Date.now, assertCurrentContext: () => assertCurrent(task),
      blockHeight: options.blockHeight,
      onProgress: progress => { if (!task.controller.signal.aborted) task.progress = progress; },
      onPoolUsable: () => scheduleFact(task, async () => {
        const pool = Object.values(runtime.adapter.snapshot().pools).find(item => schedulerPoolSessionId(runtime.id, item.id) === sessionId);
        if (!pool || pool.funding !== "opening") return;
        await event(task, `opened-${pool.id}`, { type: "opened", poolId: pool.id, commandId: pool.openCommandId });
        await runtime.adapter.settle(pool.openCommandId);
      }),
      onDispatchedDelivery: delivery => scheduleFact(task, async () => {
        const state = runtime.adapter.snapshot();
        const pool = Object.values(state.pools).find(item => schedulerPoolSessionId(runtime.id, item.id) === sessionId);
        const request = pool?.requestId ? state.requests[pool.requestId] : undefined;
        if (!request) return;
        const endedAt = Date.now();
        const elapsed = Math.max(1, Math.round(delivery.endedAt - delivery.startedAt));
        await event(task, `delivery-${request.id}`, {
          type: "delivery", requestId: request.id, accepted: delivery.accepted, paid: delivery.paidSatoshis,
          acceptedBlockBytes: delivery.acceptedBlockBytes, startedAt: endedAt - elapsed, endedAt,
        });
        await runtime.adapter.settle(request.commandId);
        if (!runtime.adapter.snapshot().manifest && runtime.adapter.snapshot().completed[task.seed] === "purchased") {
          const seed = await content.get(`bitfs-staging/by-seed/${task.seed}/seed.bin`, { signal: task.controller.signal });
          if (!seed) throw new Error("本地缺少已验收的 Seed");
          const blockHashes = await readVerifiedSeed(task.seed, seed.bytes);
          await event(task, `manifest-${sessionId}`, { type: "manifest", blockHashes });
        }
      }),
      onPoolClosed: () => scheduleFact(task, async () => {
        const pool = Object.values(runtime.adapter.snapshot().pools).find(item => schedulerPoolSessionId(runtime.id, item.id) === sessionId);
        if (!pool?.closeCommandId) return;
        const account = await ledger.getAccount({ ownerPublicKeyHex: task.identity.owner, seedHashHex: task.seed, network: task.identity.network, nowMs: Date.now() });
        const walletPool = account.pools.find(item => item.poolId === sessionId);
        const tx = account.transactions.find(item => item.txid === walletPool?.recoveryTxid);
        const opened = account.utxos.find(item => `${item.txid}:${item.vout}` === walletPool?.openingOutpoint);
        const amounts = walletPool?.state === "closed" && tx && opened ? schedulerCloseAmounts({
          amount: BigInt(pool.amount), openingAmount: BigInt(opened.valueSatoshis),
          returned: tx.expectedOutputs.reduce((sum, item) => sum + BigInt(item.valueSatoshis), 0n),
          committed: BigInt(pool.committed),
        }) : undefined;
        await event(task, `closed-${pool.id}`, amounts
          ? { type: "closed", poolId: pool.id, commandId: pool.closeCommandId, ...amounts }
          : { type: "unknown", poolId: pool.id, commandId: pool.closeCommandId });
        await runtime.adapter.settle(pool.closeCommandId);
        if (amounts) {
          await event(task, `refund-available-${pool.id}`, { type: "refund-available", poolId: pool.id });
        }
      }),
    });
    link.protocols.set(sessionId, protocol);
    return protocol;
  }

  async function recoverCommands(task: Download, link: Link): Promise<void> {
    const runtime = task.purchase!;
    const sellerId = link.quote?.sellerPublicKeyHex;
    if (!sellerId) return;
    const states = runtime.adapter.snapshot();
    for (const entry of runtime.adapter.pendingCommands()) {
      const command = entry.command;
      if (command.type === "discover") continue;
      const pool = states.pools[command.poolId];
      if (!pool || pool.sellerId !== sellerId) continue;
      if (pool.funding === "failed" || (pool.funding === "closed" && pool.refundAvailable)) {
        await runtime.adapter.settle(command.commandId);
        continue;
      }
      if (command.type !== "open") {
        if (command.type === "close") {
          const sessionId = await readSchedulerPoolSessionId(journalSessions, runtime.id, pool);
          const protocol = await protocolFor(task, link, sessionId);
          await protocol.replayLatestDispatchedPayment(sessionId, stream(task, link));
        }
        await runCommands(task, [command]);
        continue;
      }
      if (pool.funding !== "opening") {
        await runtime.adapter.settle(command.commandId);
        continue;
      }
      const sessionId = await readSchedulerPoolSessionId(journalSessions, runtime.id, pool);
      const record = await journalSessions.get(sessionId);
      if (record && await journalSessions.getEvidence(sessionId, "kind3-opening-response")) {
        const protocol = await protocolFor(task, link, sessionId);
        try {
          await protocol.recoverDispatchedOpening(sessionId, stream(task, link));
        } catch (error) {
          traceBitfs("buyer.recover-opening.failed", () => ({ sessionId, error }));
          await event(task, `recover-open-unknown-${command.commandId}`, { type: "unknown", poolId: pool.id, commandId: command.commandId });
        }
        continue;
      }
      // The exact seller approval is always saved before dispatching FundingTx.
      // Confirm that the ledger and outbox also permit releasing this opening.
      if (record) {
        const funding = options.funding(task.seed, task.controller.signal);
        if (!["cancel-opening", "cancelled"].includes(record.phase)) {
          await journalSessions.update(sessionId, record.revision, { phase: "cancel-opening" }, Date.now());
        }
        try { await funding.cancelUnfundedOpening(sessionId); }
        catch (error) {
          traceBitfs("buyer.cancel-opening.failed", () => ({ sessionId, error }));
          await event(task, `recover-open-unknown-${command.commandId}`, { type: "unknown", poolId: pool.id, commandId: command.commandId });
          continue;
        }
      }
      await event(task, `recover-open-released-${command.commandId}`, {
        type: "open-failed", poolId: pool.id, commandId: command.commandId, released: true,
      });
      await runtime.adapter.settle(command.commandId);
    }
  }

  async function quoteFact(task: Download, quote: BitfsBuyerQuoteView): Promise<void> {
    const runtime = task.purchase!;
    const endpoint = quoteArbiter(task, quote, await options.arbitrationServices());
    if (!endpoint) return;
    const feeRate = options.feeRate();
    const minerFee = await bitfsPoolSettlementFee({
      buyerPublicKey: hexToBytes(task.identity.owner), sellerPublicKey: hexToBytes(quote.sellerPublicKeyHex),
      arbiterPublicKey: hexToBytes(endpoint.publicKeyHex),
    }, BigInt(feeRate));
    let arbitration: { base: string; perItem: string };
    try {
      arbitration = await options.arbitrationPrices(endpoint, task.controller.signal);
    } catch (error) {
      assertCurrent(task);
      traceBitfs("buyer.quote.arbitration-info.failed", () => ({ sessionId: quote.sessionId, error }));
      return;
    }
    const previous = runtime.adapter.snapshot().sellers[quote.sellerPublicKeyHex]?.quote?.version ?? 0;
    const fees = {
      version: 1, opening: minerFee.toString(), settlement: minerFee.toString(),
      arbitrationBase: arbitration.base, arbitrationPerItem: arbitration.perItem,
    };
    await event(task, `quote-${quote.sessionId}`, { type: "quote", quote: {
      id: quote.sessionId, version: previous + 1, sellerId: quote.sellerPublicKeyHex, sessionId: quote.sessionId,
      expiresAt: Number(quote.quoteExpiresAtUnixSeconds) * 1_000,
      blockPrice: quote.fullBlockPriceSatoshis, seedPrice: quote.seedPriceSatoshis, tailPrice: quote.tailPriceSatoshis,
      maxBlocks: quote.maxBlocksPerBatch, fees,
    } });
    if (!task.stopped) await event(task, `online-${quote.sessionId}`, { type: "seller", sellerId: quote.sellerPublicKeyHex, online: true });
  }

  async function start(task: Download, selectedId: string, maximum?: string): Promise<void> {
    if (task.purchase) return;
    if (task.expiresAtMs <= Date.now()) throw new Error("BitFS 报价等待已结束");
    const quotes = quoteViews(task);
    const selected = quotes.find(quote => quote.sessionId === selectedId);
    if (!selected) throw new Error("报价已失效");
    const maxPrice = maximum ?? task.settings.maxFullBlockPriceSatoshis;
    if (BigInt(selected.fullBlockPriceSatoshis) > BigInt(maxPrice)) throw new Error("报价超过选定的最高价");
    const endpoints = await options.arbitrationServices();
    if (!quoteArbiter(task, selected, endpoints)) {
      throw new Error("报价没有本网络已配置的可信仲裁服务");
    }
    for (let slot = 0; slot < task.settings.maxConcurrentDownloads && !task.releaseSlot; slot++) {
      task.releaseSlot = await options.tryLock(`bitfs:slots:${task.identity.owner}:${slot}`);
    }
    if (!task.releaseSlot) throw new Error(`当前下载已达上限（${task.settings.maxConcurrentDownloads} 个文件）`);
    const tasks = createBitfsPurchaseTasks(journal, task.identity.owner, task.seed);
    const previous = (await tasks.list()).find(item => item.state === "active");
    const started = previous ?? await tasks.begin(selectedId, task.requestId!, Date.now(), selectedId);
    if (previous) await tasks.attach(previous.taskId, selectedId);
    const adapter = await SchedulerAdapter.open({
      store: createSchedulerSnapshotStore({ store: journal, ownerPublicKeyHex: task.identity.owner, network: task.identity.network, taskId: started.taskId }),
      clock, seedHash: task.seed,
      scheduler: sharedDownloadScheduler(`${task.identity.owner}|${task.identity.network}`, { ...task.settings, schedulerRequestTimeoutMs: 5_000 }),
    });
    const original = started.sessionIds[0] && await journalSessions.getEvidence(started.sessionIds[0], "quote-summary");
    const manifest = original ? JSON.parse(new TextDecoder().decode(original)) as BitfsBuyerQuoteView : selected;
    const plan = createBitfsBuyerDownloadPlan({
      taskId: started.legacyPlan ? undefined : started.taskId, ownerPublicKeyHex: task.identity.owner, seedHashHex: task.seed,
      store: journal, fileSizeBytes: manifest.fileSizeBytes, recommendedFilename: manifest.recommendedFilename,
    });
    const target = (seller: string, quoteId?: string): Link => {
      const link = [...task.links.values()].find(item => !item.closed && item.quote?.sellerPublicKeyHex === seller
        && (quoteId === undefined || item.quote?.sessionId === quoteId));
      if (!link) throw new Error("卖家连接已关闭");
      return link;
    };
    const executor = new SchedulerCommandExecutor({
      openPool: async command => {
        const link = target(command.sellerId, command.sessionId);
        const { sessionId } = await createSchedulerPoolSession({
          sessions: journalSessions, hostTaskId: started.taskId, poolId: command.poolId, quoteSessionId: command.sessionId, nowMs: Date.now(),
        });
        await tasks.attach(started.taskId, sessionId);
        const protocol = await protocolFor(task, link, sessionId);
        const budget = schedulerOpeningBudget(command);
        const feeRate = options.feeRate();
        const total = BigInt(command.contentBudget);
        const seed = adapter.snapshot().completed[task.seed] ? 0n : BigInt(command.quote.seedPrice);
        const reserve = await protocol.configureOpeningBudget({ sessionId, feeRate: BigInt(feeRate), arbitrationReserve: budget.arbitrationReserve });
        if (total + reserve !== budget.amount) throw new Error("开池预算与已确认费率不一致");
        await protocol.startPurchase({
          sessionId, stream: stream(task, link), openingAmountSatoshis: budget.amount.toString(),
          contentBudgetSatoshis: total.toString(), seedBudgetSatoshis: seed.toString(),
          seedBudgetReserved: seed > 0n, blockBudgetSatoshis: (total - seed).toString(),
          mediaType: selectBitfsMediaType(quotes),
        });
      },
      dispatchDownload: async command => {
        const pool = adapter.snapshot().pools[command.poolId]!;
        const link = target(pool.sellerId);
        const sessionId = await readSchedulerPoolSessionId(journalSessions, started.taskId, pool);
        const protocol = await protocolFor(task, link, sessionId);
        const quoteRaw = await journalSessions.getEvidence(command.quote.sessionId, "kind1-quote");
        if (!quoteRaw) throw new Error("本次报价缺失");
        link.requestProgress = { id: command.requestId, start: link.bytesIn ?? 0, sent: 0 };
        if (!await protocol.replayDispatchedRequest({ sessionId, stream: stream(task, link) })) {
          await protocol.dispatchRequest({ sessionId, quoteRaw, contentIds: command.contentIds, maxPayment: command.maxPayment, stream: stream(task, link) });
        }
      },
      drainPool: async command => { await adapter.settle(command.commandId); },
      closePool: async command => {
        const pool = adapter.snapshot().pools[command.poolId]!;
        const link = target(pool.sellerId);
        const sessionId = await readSchedulerPoolSessionId(journalSessions, started.taskId, pool);
        const protocol = await protocolFor(task, link, sessionId);
        await protocol.closeDispatchedPool({ sessionId, stream: stream(task, link) });
      },
    });
    task.purchase = { id: started.taskId, adapter, executor, plan, manifest };
    if (previous) {
      for (const seller of Object.values(adapter.snapshot().sellers)) {
        await event(task, `reconnect-${task.runId}-${seller.id}`, { type: "seller", sellerId: seller.id, online: false });
      }
    }
    if (previous) {
      for (const link of task.links.values()) {
        if (link.quote && !link.closed) await recoverCommands(task, link);
      }
    }
    for (const quote of quotes) {
      if (!task.stopped) await quoteFact(task, quote);
    }
    if (task.stopped) return;
    await event(task, `price-${task.runId}`, { type: "slider", slider: "price", value: maxPrice });
    await event(task, `run-${task.runId}`, { type: previous ? "resume" : "start" });
  }

  async function receive(task: Download, link: Link, input: BitfsBuyerTransportEvent): Promise<void> {
    if (input.type === "progress") {
      if (typeof input.bytesIn !== "number") return;
      link.bytesIn = input.bytesIn;
      const progress = link.requestProgress;
      if (progress && task.purchase && input.bytesIn - progress.start > progress.sent) {
        progress.sent = input.bytesIn - progress.start;
        await event(task, `progress-${progress.id}-${progress.sent}`, { type: "progress", requestId: progress.id, bytes: progress.sent });
      }
      return;
    }
    if (input.type === "closed") {
      link.closed = true;
      if (task.purchase && link.quote) {
        await event(task, `offline-${link.offer.sessionId}`, { type: "seller", sellerId: link.quote.sellerPublicKeyHex, online: false });
      }
      return;
    }
    if (!(input.frame instanceof Uint8Array)) return;
    const kind = parse(input.frame).kind;
    if (!link.quote) {
      if (kind !== 1 || task.expiresAtMs <= Date.now()) return;
      link.quote = await acceptBitfsBuyerQuote({
        sessionId: `quote-${crypto.randomUUID()}`, counterpartyPublicKeyHex: link.offer.publicKeyHex, rawKind1: input.frame,
      }, { ownerPublicKeyHex: task.identity.owner, seedHashHex: task.seed, sessions: journalSessions, generation: Date.now(), nowMs: Date.now, assertCurrentContext: () => assertCurrent(task) });
      if (task.purchase) {
        await recoverCommands(task, link);
        if (!task.stopped) await quoteFact(task, link.quote);
      }
      else if (task.settings.buyerAutoPurchaseEnabled
        && BigInt(link.quote.fullBlockPriceSatoshis) <= BigInt(task.settings.maxFullBlockPriceSatoshis)) {
        try { await start(task, link.quote.sessionId); }
        catch (error) {
          if (task.purchase) throw error;
          task.releaseSlot?.();
          task.releaseSlot = undefined;
          task.progress = { sessionId: link.quote.sessionId, phase: "failed", openingAmountSatoshis: "0",
            verifiedBlockCount: 0, totalBlockCount: null, message: error instanceof Error ? error.message : "购买无法开始" };
        }
      }
      return;
    }
    if (!task.purchase || kind === 1) return;
    const sessionId = await findBuyerPoolResponseSession({
      sessions: journalSessions, raw: input.frame, owner: task.identity.owner, counterparty: link.offer.publicKeyHex, seed: task.seed,
      sessionIds: link.protocols.size ? link.protocols.keys() : undefined,
    });
    const protocol = await protocolFor(task, link, sessionId);
    await protocol.onFrame({ sessionId, rawArtifact: input.frame, stream: stream(task, link) });
    if (task.progress?.sessionId === sessionId && ["close-unknown", "cancel-unknown"].includes(task.progress.phase)) {
      const pool = Object.values(task.purchase.adapter.snapshot().pools).find(item => schedulerPoolSessionId(task.purchase!.id, item.id) === sessionId);
      if (pool?.closeCommandId) {
        await event(task, `close-unknown-${pool.id}`, { type: "unknown", poolId: pool.id, commandId: pool.closeCommandId });
      }
    }
  }

  async function publishBitfsDemand(seed: string, requireExisting = false): Promise<BitfsBuyerSnapshot> {
    if (!isValidSeed(seed)) throw new Error("BitFS Seed Hash 无效");
    lifecycle.throwIfAborted();
    if (downloads.has(seed)) return snapshot(downloads.get(seed)!);
    const identity = options.identity();
    const release = await options.tryLock(downloadLockKey(identity.owner, identity.network, seed));
    if (!release) return readOther(seed);
    let task: Download | undefined;
    try {
      if (await journal.get(downloadDeletionPath(seed))) throw new Error("BitFS 下载数据尚待清理或结算");
      const intentPath = downloadIntentPath(seed);
      if (!await journal.get(intentPath)) {
        if (requireExisting && !(await createBitfsPurchaseTasks(journal, identity.owner, seed).list()).some(item => item.state === "active")) {
          throw new Error("BitFS 没有可续下的任务");
        }
        await journal.put(intentPath, new TextEncoder().encode(seed), { ifNoneMatch: true });
      }
      let completeStop!: () => void;
      const finished = new Promise<void>(resolve => { completeStop = resolve; });
      const settings = await options.settings();
      task = {
        identity, seed, settings, release, finished, completeStop, runId: crypto.randomUUID(),
        controller: new AbortController(), links: new Map(), expiresAtMs: Date.now() + SEARCH_TIMEOUT_MS, tail: Promise.resolve(),
      };
      downloads.set(seed, task);
      const currentTask = task;
      assertCurrent(currentTask);
      const demand = await options.publishDemand(seed, currentTask.controller.signal, id => {
        currentTask.requestId = id;
      });
      assertCurrent(currentTask);
      if (currentTask.requestId && currentTask.requestId !== demand.requestMessageId) throw new Error("BitFS 需求编号冲突");
      currentTask.requestId = demand.requestMessageId;
      currentTask.expiresAtMs = demand.expiresAtMs;
      currentTask.timer = setTimeout(() => {
        if (!currentTask.purchase || !currentTask.links.size) {
          void serial(currentTask, () => stopNormally(currentTask)).catch(error => drop(currentTask, error));
        }
      }, Math.max(0, currentTask.expiresAtMs - Date.now()));
      return snapshot(currentTask);
    } catch (error) {
      if (task) await drop(task, error);
      else release();
      throw error;
    }
  }

  lifecycle.addEventListener("abort", () => {
    for (const task of downloads.values()) void drop(task);
  }, { once: true });
  return {
    registerOffer(offer: BitfsBuyerOffer): AbortSignal | undefined {
      const task = downloads.get(offer.seedHashHex);
      if (!task || task.controller.signal.aborted || Date.now() >= task.expiresAtMs
        || (task.requestId ? task.requestId !== offer.requestMessageId : !options.acceptOfferBeforePublishResult)) return undefined;
      task.requestId = offer.requestMessageId;
      const link = { offer, protocols: new Map(), closed: false };
      task.links.set(offer.sessionId, link);
      frames.set(offer.sessionId, { task, link });
      return task.controller.signal;
    },
    async onTransportEvent(input: BitfsBuyerTransportEvent): Promise<boolean> {
      const route = frames.get(input.sessionId);
      if (!route) return false;
      const handling = serial(route.task, () => receive(route.task, route.link, input)).catch(error => drop(route.task, error));
      if (input.type === "frame") await handling;
      else void handling;
      return input.type !== "closed";
    },
    download: (seed: string) => publishBitfsDemand(seed),
    resume: (seed: string) => publishBitfsDemand(seed, true),
    async getDownload(seed: string) {
      const task = downloads.get(seed);
      return task ? snapshot(task) : readOther(seed);
    },
    async startPurchase(seed: string, sessionId: string, max?: string) {
      const task = downloads.get(seed);
      if (!task) throw new Error("BitFS 下载需求不属于当前实例");
      try {
        await serial(task, () => start(task, sessionId, max));
      } catch (error) {
        if (task.purchase) await drop(task, error);
        else {
          task.releaseSlot?.();
          task.releaseSlot = undefined;
        }
        throw error;
      }
      return snapshot(task);
    },
    async withdrawDemand(seed: string) {
      const task = downloads.get(seed);
      if (task && !task.purchase) await serial(task, () => drop(task));
    },
    async stop(seed: string) {
      const task = downloads.get(seed);
      if (!task) return readOther(seed);
      await serial(task, () => stopNormally(task));
      await task.finished;
      return snapshot(task);
    },
    async deleteDownload(seed: string) {
      const identity = options.identity();
      return deleteBitfsDownload({ seedHashHex: seed, ownerPublicKeyHex: identity.owner, network: identity.network,
        content, journal, tryLock: options.tryLock });
    },
    async close() {
      await Promise.all([...downloads.values()].map(task => drop(task)));
    },
  };
}

export type BitfsBuyer = ReturnType<typeof createBitfsBuyer>;
