import { hexToBytes } from "bitcoin-libp2p/identity";
import { parse, type Signer } from "bitfs-protocol";
import { Digest, verifyBlock } from "masterseed";
import type { Command, Payload, State, View } from "keymaster-trade-scheduler";
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

/** 诊断专用归属摘要；不参与调度、付款或恢复判断。 */
function schedulerOwnership(state: State) {
  return {
    revision: state.revision, intent: state.intent, failure: state.failure,
    pools: Object.values(state.pools).map(pool => ({
      id: pool.id, sellerId: pool.sellerId, seatIndex: (pool as typeof pool & { seatIndex?: number }).seatIndex ?? 0,
      funding: pool.funding, scheduling: pool.scheduling, requestId: pool.requestId,
      openCommandId: pool.openCommandId, closeCommandId: pool.closeCommandId,
      openingObserved: pool.openingObserved,
    })),
    requests: Object.values(state.requests).map(request => ({
      id: request.id, poolId: request.poolId, state: request.state,
      commandId: request.commandId, contentIds: request.contentIds,
    })),
  };
}

const SEARCH_TIMEOUT_MS = 10 * 60_000;
const PLAYBACK_BLOCK_BYTES = 256 * 1024;

export interface BitfsPlaybackRange {
  fileSizeBytes?: number;
  mediaType?: string;
  bytes?: Uint8Array;
}

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
  seatIndex?: number;
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
  manualPurchase?: boolean;
  controller: AbortController;
  release(): void;
  links: Map<string, Link>;
  expiresAtMs: number;
  requestId?: string;
  supplementalDemands: Set<string>;
  supplementalPublishing: number;
  tail: Promise<void>;
  timer?: ReturnType<typeof setTimeout>;
  wake?: ReturnType<typeof setTimeout>;
  purchase?: Purchase;
  progress?: BitfsBuyerPurchaseProgress;
  playbackPriority?: string[];
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
    const quotes = new Map<string, BitfsBuyerQuoteView>();
    for (const link of task.links.values()) {
      const quote = link.quote;
      if (!link.closed && quote && Number(quote.quoteExpiresAtUnixSeconds) * 1_000 > Date.now()) {
        if (!quotes.has(quote.sellerPublicKeyHex)) quotes.set(quote.sellerPublicKeyHex, quote);
      }
    }
    return [...quotes.values()];
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
    traceBitfs("buyer.drop.start", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: task.purchase?.id,
      committed: task.committed === true, aborted: task.controller.signal.aborted,
      scheduler: task.purchase ? schedulerOwnership(task.purchase.adapter.snapshot()) : undefined,
      error: cause,
    }));
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
      const demandIds = [...task.supplementalDemands];
      if (task.requestId) demandIds.push(task.requestId);
      const routing = demandIds.map(id => options.withdrawDemand?.(task.seed, id));
      await Promise.allSettled([adapterDrain, ...routing, ...closes]);
      const runtime = task.purchase;
      if (runtime && !lifecycle.aborted) {
        for (const pool of Object.values(runtime.adapter.snapshot().pools)) {
          if (pool.openingObserved || !["opening", "problem"].includes(pool.funding)) continue;
          try {
            const sessionId = await readSchedulerPoolSessionId(journalSessions, runtime.id, pool);
            await cancelPreparedOpening(task, sessionId, lifecycle);
          } catch (error) {
            traceBitfs("buyer.cancel-opening.failed", () => ({ poolId: pool.id, error }));
          }
        }
      }
    } finally {
      if (downloads.get(task.seed) === task) downloads.delete(task.seed);
      task.completeStop();
      try { options.onStopped?.(task.seed, task.committed === true); } catch (error) { traceBitfs("buyer.observer.failed", () => ({ error })); }
      task.releaseSlot?.();
      task.release();
      traceBitfs("buyer.drop.finish", () => ({
        seedHashHex: task.seed, runId: task.runId, taskId: task.purchase?.id,
        committed: task.committed === true,
      }));
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
    traceBitfs("buyer.event.start", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: runtime.id,
      eventId, payload, revision: runtime.adapter.snapshot().revision,
      scheduler: schedulerOwnership(runtime.adapter.snapshot()),
    }));
    const commands = await runtime.adapter.handle({ eventId, payload });
    await runCommands(task, commands);
    if (task.stopped) return;
    await updateView(task, runtime);
    traceBitfs("buyer.event.finish", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: runtime.id,
      eventId, payloadType: payload.type, commandIds: commands.map(command => command.commandId),
      revision: runtime.adapter.snapshot().revision,
      scheduler: schedulerOwnership(runtime.adapter.snapshot()),
    }));
  }

  async function runCommands(task: Download, commands: readonly Command[]): Promise<void> {
    const runtime = task.purchase!;
    for (const command of commands) {
      assertCurrent(task);
      traceBitfs("buyer.scheduler.command.start", () => ({
        seedHashHex: task.seed, runId: task.runId, taskId: runtime.id,
        commandId: command.commandId, commandType: command.type, reason: command.reason,
        poolId: "poolId" in command ? command.poolId : undefined,
        requestId: "requestId" in command ? command.requestId : undefined,
        scheduler: schedulerOwnership(runtime.adapter.snapshot()),
      }));
      try {
        await runtime.executor.run(command);
        traceBitfs("buyer.scheduler.command.finish", () => ({
          seedHashHex: task.seed, runId: task.runId, taskId: runtime.id,
          commandId: command.commandId, commandType: command.type,
          scheduler: schedulerOwnership(runtime.adapter.snapshot()),
        }));
      } catch (error) {
        assertCurrent(task);
        traceBitfs("buyer.scheduler.command.failed", () => ({ commandId: command.commandId, type: command.type, error,
          scheduler: schedulerOwnership(runtime.adapter.snapshot()) }));
        if (command.type === "open") {
          const code = (error as { code?: string })?.code;
          const insufficient = code === "insufficient" || code === "no-utxos" || code === "insufficient_funds";
          if (!insufficient) {
            const seat = runtime.adapter.snapshot().pools[command.poolId]?.seatIndex ?? 0;
            const link = [...task.links.values()].find(item => !item.closed && item.seatIndex === seat
              && item.quote?.sellerPublicKeyHex === command.sellerId);
            if (link) {
              link.closed = true;
              void link.offer.close().catch(error => traceBitfs("buyer.seat.close.failed", () => ({ error })));
            }
            await reportSellerLinks(task, command.sellerId, `open-offline-${command.commandId}`);
            await releaseUnfundedOpening(task, command.poolId, command.commandId);
            continue;
          }
          await event(task, `open-error-${command.commandId}`,
            { type: "open-failed", poolId: command.poolId, commandId: command.commandId, released: true, insufficientFunds: true });
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
      if (view.failure) notifyFailure(task, Object.assign(new Error(`BitFS download failed: ${view.failure.code}`), { code: view.failure.code }));
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

  function assignSeat(task: Download, link: Link): void {
    if (!link.quote || link.seatIndex !== undefined) return;
    const seller = link.quote.sellerPublicKeyHex;
    const occupied = new Set([...task.links.values()].filter(item => !item.closed
      && item.quote?.sellerPublicKeyHex === seller).map(item => item.seatIndex));
    const capacity = task.purchase?.adapter.snapshot().config.seatsPerSeller ?? 1;
    for (let index = 0; index < capacity; index++) {
      if (occupied.has(index)) continue;
      link.seatIndex = index;
      return;
    }
  }

  async function reportSellerLinks(task: Download, sellerId: string, eventId: string): Promise<void> {
    const seatSessions = Object.fromEntries([...task.links.values()].flatMap(item => !item.closed
      && item.quote?.sellerPublicKeyHex === sellerId && item.seatIndex !== undefined ? [[item.seatIndex, item.quote.sessionId]] : []));
    traceBitfs("buyer.seller-links.report", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: task.purchase?.id,
      sellerId, eventId, seatSessions,
      scheduler: task.purchase ? schedulerOwnership(task.purchase.adapter.snapshot()) : undefined,
    }));
    await event(task, eventId, { type: "seller", sellerId, online: Object.keys(seatSessions).length > 0, seatSessions });
  }

  async function publishSupplementalDemands(task: Download): Promise<void> {
    const count = task.purchase?.adapter.snapshot().config.seatsPerSeller ?? 1;
    await Promise.allSettled(Array.from({ length: count - 1 }, async () => {
      task.supplementalPublishing++;
      try {
        const demand = await options.publishDemand(task.seed, task.controller.signal, id => {
          assertCurrent(task);
          task.supplementalDemands.add(id);
        });
        assertCurrent(task);
        task.supplementalDemands.add(demand.requestMessageId);
      } catch (error) {
        if (!task.controller.signal.aborted) traceBitfs("buyer.seat.discovery.failed", () => ({ error }));
      } finally {
        task.supplementalPublishing--;
      }
    }));
  }

  async function cancelPreparedOpening(task: Download, sessionId: string, signal: AbortSignal): Promise<boolean> {
    const record = await journalSessions.get(sessionId);
    if (!record || record.phase === "cancelled") return true;
    if (!["quote-selected", "opening-presign", "funding-prepared", "cancel-opening"].includes(record.phase)
      || await journalSessions.getEvidence(sessionId, "kind3-opening-response")) return false;
    if (record.phase !== "cancel-opening") {
      await journalSessions.update(sessionId, record.revision, { phase: "cancel-opening" }, Date.now());
    }
    await options.funding(task.seed, signal).cancelUnfundedOpening(sessionId);
    return true;
  }

  async function releaseUnfundedOpening(task: Download, poolId: string, commandId: string): Promise<void> {
    const runtime = task.purchase!;
    const pool = runtime.adapter.snapshot().pools[poolId]!;
    const sessionId = await readSchedulerPoolSessionId(journalSessions, runtime.id, pool);
    const record = await journalSessions.get(sessionId);
    traceBitfs("buyer.release-opening.start", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: runtime.id,
      poolId, commandId, sessionId, phase: record?.phase,
      pendingTxid: record?.pendingTxid, pendingAuthorizationId: record?.pendingAuthorizationId,
      evidence: record?.evidence,
      scheduler: schedulerOwnership(runtime.adapter.snapshot()),
    }));
    let released = false;
    try {
      released = await cancelPreparedOpening(task, sessionId, task.controller.signal);
    } catch (error) {
      traceBitfs("buyer.cancel-opening.failed", () => ({ sessionId, error }));
    }
    traceBitfs("buyer.release-opening.decision", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: runtime.id,
      poolId, commandId, sessionId, released,
      scheduler: schedulerOwnership(runtime.adapter.snapshot()),
    }));
    await event(task, `closed-opening-${commandId}`, released
      ? { type: "open-failed", poolId, commandId, released: true }
      : { type: "unknown", poolId, commandId });
    await runtime.adapter.settle(commandId);
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
      if (!pool || pool.sellerId !== sellerId || (pool.seatIndex ?? 0) !== link.seatIndex) continue;
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
      await releaseUnfundedOpening(task, pool.id, command.commandId);
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

  }

  async function start(task: Download, selectedId: string, maximum?: string, requireNew = false): Promise<void> {
    if (task.purchase) {
      if (requireNew) throw new Error("文件已由另一下载任务处理");
      return;
    }
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
    if (requireNew && previous) throw new Error("请先在钱包中续下原下载任务");
    const started = previous ?? await tasks.begin(selectedId, task.requestId!, Date.now(), selectedId);
    if (previous) await tasks.attach(previous.taskId, selectedId);
    traceBitfs("buyer.start.begin", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: started.taskId,
      selectedSessionId: selectedId, demandId: started.demandId,
      resumed: Boolean(previous), maxPrice, quoteSessionIds: quotes.map(quote => quote.sessionId),
    }));
    const adapter = await SchedulerAdapter.open({
      store: createSchedulerSnapshotStore({ store: journal, ownerPublicKeyHex: task.identity.owner, network: task.identity.network, taskId: started.taskId }),
      clock, seedHash: task.seed,
      scheduler: sharedDownloadScheduler(`${task.identity.owner}|${task.identity.network}`, { ...task.settings, schedulerRequestTimeoutMs: 5_000 }),
    }).catch(error => {
      traceBitfs("buyer.scheduler.open.failed", () => ({
        seedHashHex: task.seed, runId: task.runId, taskId: started.taskId, error,
      }));
      throw error;
    });
    traceBitfs("buyer.scheduler.open.finish", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: started.taskId,
      scheduler: schedulerOwnership(adapter.snapshot()),
    }));
    const original = started.sessionIds[0] && await journalSessions.getEvidence(started.sessionIds[0], "quote-summary");
    const manifest = original ? JSON.parse(new TextDecoder().decode(original)) as BitfsBuyerQuoteView : selected;
    const plan = createBitfsBuyerDownloadPlan({
      taskId: started.legacyPlan ? undefined : started.taskId, ownerPublicKeyHex: task.identity.owner, seedHashHex: task.seed,
      store: journal, fileSizeBytes: manifest.fileSizeBytes, recommendedFilename: manifest.recommendedFilename,
    });
    const target = (seller: string, seatIndex: number): Link => {
      const link = [...task.links.values()].find(item => !item.closed && item.quote?.sellerPublicKeyHex === seller
        && item.seatIndex === seatIndex);
      if (!link) throw new Error("卖家连接已关闭");
      return link;
    };
    const executor = new SchedulerCommandExecutor({
      openPool: async command => {
        const pool = adapter.snapshot().pools[command.poolId]!;
        const link = target(command.sellerId, pool.seatIndex ?? 0);
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
        const transport = stream(task, link);
        let openingSent = false;
        await protocol.startPurchase({
          sessionId,
          stream: { async send(frame) {
            await transport.send(frame);
            openingSent = parse(frame).kind === 2;
          } },
          openingAmountSatoshis: budget.amount.toString(),
          contentBudgetSatoshis: total.toString(), seedBudgetSatoshis: seed.toString(),
          seedBudgetReserved: seed > 0n, blockBudgetSatoshis: (total - seed).toString(),
          mediaType: selectBitfsMediaType(quotes),
        });
        if (openingSent) {
          await event(task, `opening-sent-${pool.id}`,
            { type: "opening-sent", poolId: pool.id, commandId: command.commandId })
            .catch(error => traceBitfs("buyer.opening.sent-record.failed", () => ({ poolId: pool.id, error })));
        }
      },
      dispatchDownload: async command => {
        const pool = adapter.snapshot().pools[command.poolId]!;
        const link = target(pool.sellerId, pool.seatIndex ?? 0);
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
        const link = target(pool.sellerId, pool.seatIndex ?? 0);
        const sessionId = await readSchedulerPoolSessionId(journalSessions, started.taskId, pool);
        const protocol = await protocolFor(task, link, sessionId);
        await protocol.closeDispatchedPool({ sessionId, stream: stream(task, link) });
      },
    });
    task.purchase = { id: started.taskId, adapter, executor, plan, manifest };
    for (const link of task.links.values()) assignSeat(task, link);
    traceBitfs("buyer.start.ready", () => ({
      seedHashHex: task.seed, runId: task.runId, taskId: started.taskId,
      links: [...task.links.values()].map(link => ({
        sessionId: link.offer.sessionId, sellerId: link.quote?.sellerPublicKeyHex,
        quoteSessionId: link.quote?.sessionId, seatIndex: link.seatIndex, closed: link.closed,
      })),
      scheduler: schedulerOwnership(adapter.snapshot()),
    }));
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
    for (const link of task.links.values()) {
      if (!task.stopped && !link.closed && link.quote && link.seatIndex !== undefined) {
        await quoteFact(task, link.quote);
        await reportSellerLinks(task, link.quote.sellerPublicKeyHex, `open-link-${link.offer.sessionId}`);
      }
    }
    if (task.stopped) return;
    await event(task, `price-${task.runId}`, { type: "slider", slider: "price", value: maxPrice });
    void publishSupplementalDemands(task);
    await event(task, `run-${task.runId}`, { type: previous ? "resume" : "start" });
  }

  async function receive(task: Download, link: Link, input: BitfsBuyerTransportEvent): Promise<void> {
    if (link.closed && input.type !== "closed") return;
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
        await reportSellerLinks(task, link.quote.sellerPublicKeyHex, `closed-link-${link.offer.sessionId}`);
        if (link.seatIndex !== undefined && !task.stopped) {
          for (const pool of Object.values(task.purchase.adapter.snapshot().pools)) {
            if (pool.sellerId === link.quote.sellerPublicKeyHex && (pool.seatIndex ?? 0) === link.seatIndex
              && pool.funding === "opening") await releaseUnfundedOpening(task, pool.id, pool.openCommandId);
          }
        }
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
        assignSeat(task, link);
        if (link.seatIndex === undefined) {
          link.closed = true;
          await link.offer.close();
          return;
        }
        await recoverCommands(task, link);
        if (!task.stopped) {
          await reportSellerLinks(task, link.quote.sellerPublicKeyHex, `open-link-${link.offer.sessionId}`);
          await quoteFact(task, link.quote);
        }
      }
      else if (!task.manualPurchase && task.settings.buyerAutoPurchaseEnabled
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

  async function publishBitfsDemand(seed: string, requireExisting = false, manualPurchase = false): Promise<BitfsBuyerSnapshot> {
    if (!isValidSeed(seed)) throw new Error("BitFS Seed Hash 无效");
    lifecycle.throwIfAborted();
    traceBitfs("buyer.demand.start", () => ({
      seedHashHex: seed, requireExisting, manualPurchase,
      hasLiveTask: downloads.has(seed),
    }));
    const current = downloads.get(seed);
    if (current) {
      if (manualPurchase && !current.purchase && !current.manualPurchase) throw new Error("钱包已在查找这个文件");
      traceBitfs("buyer.demand.reuse", () => ({
        seedHashHex: seed, runId: current.runId, taskId: current.purchase?.id,
        requestMessageId: current.requestId,
      }));
      return snapshot(current);
    }
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
        identity, seed, settings, manualPurchase, release, finished, completeStop, runId: crypto.randomUUID(),
        controller: new AbortController(), links: new Map(), supplementalDemands: new Set(), supplementalPublishing: 0, expiresAtMs: Date.now() + SEARCH_TIMEOUT_MS, tail: Promise.resolve(),
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
      traceBitfs("buyer.demand.published", () => ({
        seedHashHex: seed, runId: currentTask.runId,
        requestMessageId: demand.requestMessageId, expiresAtMs: demand.expiresAtMs,
      }));
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
    async readPlaybackRange(seed: string, taskId: string, offset: number, length: number, signal: AbortSignal): Promise<BitfsPlaybackRange> {
      if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(length) || length < 0 ||
          length > PLAYBACK_BLOCK_BYTES || !Number.isSafeInteger(offset + length)) throw new Error("无效的 BitFS 播放范围");
      lifecycle.throwIfAborted();
      signal.throwIfAborted();
      const identity = options.identity();
      const live = downloads.get(seed);
      const running = live?.purchase?.id === taskId ? live.purchase : undefined;
      const tasks = await createBitfsPurchaseTasks(journal, identity.owner, seed).list();
      const task = tasks.find(row => row.taskId === taskId);
      if (!task) return {};
      const scheduler = running?.adapter.snapshot() ?? (await createSchedulerSnapshotStore({
        store: journal, ownerPublicKeyHex: identity.owner, network: identity.network, taskId,
      }).load())?.snapshot.state;
      if (scheduler?.seedHash !== seed || !scheduler.manifest) return {};
      const seedObject = await content.get(`bitfs-staging/by-seed/${seed}/seed.bin`, { signal });
      if (!seedObject) return {};
      const hashes = await readVerifiedSeed(seed, seedObject.bytes);
      const quoteBytes = task.sessionIds[0] && await journalSessions.getEvidence(task.sessionIds[0], "quote-summary");
      const quote = quoteBytes ? JSON.parse(new TextDecoder().decode(quoteBytes)) as BitfsBuyerQuoteView : undefined;
      const fileSizeBytes = Number(running?.manifest.fileSizeBytes ?? quote?.fileSizeBytes);
      if (!Number.isSafeInteger(fileSizeBytes) || fileSizeBytes < 0 || Math.ceil(fileSizeBytes / PLAYBACK_BLOCK_BYTES) !== hashes.length) {
        throw new Error("BitFS 播放长度与已验收 Seed 不匹配");
      }
      const meta = { fileSizeBytes, mediaType: quote?.mediaType };
      const end = Math.min(offset + length, fileSizeBytes);
      if (offset >= end) return meta;
      const first = Math.floor(offset / PLAYBACK_BLOCK_BYTES);
      const last = Math.floor((end - 1) / PLAYBACK_BLOCK_BYTES);
      const required = hashes.slice(first, last + 1);
      if (required.some(hash => !scheduler.completed[hash])) {
        if (live && running) {
          const preferred = [...new Set(hashes.slice(first, last + 4))];
          const current = live.playbackPriority ?? scheduler.priorityBlocks ?? [];
          if (preferred.length !== current.length || preferred.some((hash, index) => hash !== current[index])) {
            live.playbackPriority = preferred;
            void serial(live, async () => {
              if (signal.aborted || live.playbackPriority !== preferred) return;
              await event(live, `playback-${crypto.randomUUID()}`, { type: "prioritize-blocks", blockHashes: preferred });
            }).catch(error => {
              if (!live.controller.signal.aborted) traceBitfs("buyer.playback.priority.failed", () => ({ error }));
            }).finally(() => {
              if (live.playbackPriority === preferred) live.playbackPriority = undefined;
            });
          }
        }
        return meta;
      }
      const result = new Uint8Array(end - offset);
      let written = 0;
      for (let index = first; index <= last; index++) {
        const hash = hashes[index]!;
        const block = await content.get(`bitfs-staging/by-seed/${seed}/blocks/${hash}.bin`, { signal });
        if (!block) return meta;
        verifyBlock(block.bytes, Digest.fromHex(hash));
        const base = index * PLAYBACK_BLOCK_BYTES;
        if (block.bytes.length !== Math.min(PLAYBACK_BLOCK_BYTES, fileSizeBytes - base)) {
          throw new Error("BitFS 播放 Block 长度无效");
        }
        const part = block.bytes.subarray(Math.max(0, offset - base), Math.min(block.bytes.length, end - base));
        result.set(part, written);
        written += part.length;
      }
      const current = options.identity();
      if (current.generation !== identity.generation || current.owner !== identity.owner || current.network !== identity.network) {
        throw new Error("BitFS 下载身份已变化");
      }
      signal.throwIfAborted();
      return { ...meta, bytes: result };
    },
    registerOffer(offer: BitfsBuyerOffer): AbortSignal | undefined {
      const task = downloads.get(offer.seedHashHex);
      traceBitfs("buyer.offer.received", () => ({
        seedHashHex: offer.seedHashHex, sessionId: offer.sessionId,
        requestMessageId: offer.requestMessageId, sellerPublicKeyHex: offer.publicKeyHex,
        hasTask: Boolean(task), taskRequestId: task?.requestId,
        supplementalPublishing: task?.supplementalPublishing,
      }));
      if (!task || task.controller.signal.aborted || Date.now() >= task.expiresAtMs) {
        traceBitfs("buyer.offer.rejected", () => ({
          seedHashHex: offer.seedHashHex, sessionId: offer.sessionId,
          reason: !task ? "no-task" : task.controller.signal.aborted ? "aborted" : "expired",
        }));
        return undefined;
      }
      if (!task.requestId) {
        if (!options.acceptOfferBeforePublishResult) {
          traceBitfs("buyer.offer.rejected", () => ({
            seedHashHex: offer.seedHashHex, sessionId: offer.sessionId, reason: "early-offer-disabled",
          }));
          return undefined;
        }
        task.requestId = offer.requestMessageId;
      } else if (task.requestId !== offer.requestMessageId && !task.supplementalDemands.has(offer.requestMessageId)) {
        // The wallet Coordinator has already authenticated and bound early offers to
        // this page's publish request; its callback can arrive before publish returns.
        if (!options.acceptOfferBeforePublishResult || task.supplementalPublishing === 0) {
          traceBitfs("buyer.offer.rejected", () => ({
            seedHashHex: offer.seedHashHex, sessionId: offer.sessionId,
            requestMessageId: offer.requestMessageId, taskRequestId: task.requestId,
            reason: "unbound-demand",
          }));
          return undefined;
        }
        task.supplementalDemands.add(offer.requestMessageId);
      }
      const link = { offer, protocols: new Map(), closed: false };
      task.links.set(offer.sessionId, link);
      frames.set(offer.sessionId, { task, link });
      traceBitfs("buyer.offer.accepted", () => ({
        seedHashHex: offer.seedHashHex, sessionId: offer.sessionId,
        requestMessageId: offer.requestMessageId, runId: task.runId,
      }));
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
    download: (seed: string, manualPurchase = false) => publishBitfsDemand(seed, false, manualPurchase),
    resume: (seed: string) => publishBitfsDemand(seed, true),
    async getDownload(seed: string) {
      const task = downloads.get(seed);
      return task ? snapshot(task) : readOther(seed);
    },
    async startPurchase(seed: string, sessionId: string, max?: string, requireNew = false) {
      const task = downloads.get(seed);
      if (!task) throw new Error("BitFS 下载需求不属于当前实例");
      traceBitfs("buyer.purchase.request", () => ({
        seedHashHex: seed, runId: task.runId, quoteSessionId: sessionId,
        max, requireNew, taskId: task.purchase?.id,
      }));
      let alreadyRunning = false;
      try {
        await serial(task, () => {
          alreadyRunning = Boolean(task.purchase);
          return start(task, sessionId, max, requireNew);
        });
      } catch (error) {
        traceBitfs("buyer.purchase.request.failed", () => ({
          seedHashHex: seed, runId: task.runId, quoteSessionId: sessionId,
          taskId: task.purchase?.id, error,
        }));
        if (requireNew && alreadyRunning) throw error;
        if (task.purchase) await drop(task, error);
        else {
          task.releaseSlot?.();
          task.releaseSlot = undefined;
        }
        throw error;
      }
      traceBitfs("buyer.purchase.request.finish", () => ({
        seedHashHex: seed, runId: task.runId, taskId: task.purchase?.id,
        quoteSessionId: sessionId,
      }));
      return snapshot(task);
    },
    async setDownloadPrice(seed: string, taskId: string, max: string, signal: AbortSignal) {
      const task = downloads.get(seed);
      if (!task) throw new Error("下载任务未运行");
      await serial(task, async () => {
        signal.throwIfAborted();
        assertCurrent(task);
        if (task.purchase?.id !== taskId || task.stopped || task.closing) throw new Error("下载任务未运行");
        await event(task, `price-${crypto.randomUUID()}`, { type: "slider", slider: "price", value: max });
      });
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
