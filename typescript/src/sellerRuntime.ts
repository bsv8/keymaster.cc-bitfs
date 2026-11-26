// BitFS 卖方请求匹配核心；不包含 React、Channel provider 或 libp2p host。
// 输入必须是 ChannelProtocol 产出的 VerifiedHashRequest。

import { isVerifiedHashRequest, dedupKey, type VerifiedHashRequest } from "bsv8-channel-protocol/hash-request";
import type { BitfsSellerSettings } from "./ports.js";
import { createSellerQuote, sellerTailPriceSatoshis, type Signer } from "bitfs-protocol";
import type { BitfsJournal } from "./journal.js";
import type { BitfsSessionJournal } from "./sessionJournal.js";
import { bitfsWorkflowFacts, preserveBitfsSignerErrors } from "./sdk.js";

export interface BitfsSellerMatch {
  /** Channel 去重键。 */
  requestKey: string;
  /** ChannelProtocol 哈希请求的真实 message_id。 */
  requestMessageId: string;
  /** 本次销售连接使用的 locator 类型。 */
  transport: "multiaddr" | "webrtc-sdp";
  /** multiaddr 方式下通过白名单与身份 pin 的规范地址；SDP 方式为空。 */
  addresses: string[];
  /** 已持久化、可以交给 BitFS stream 的 exact Kind 1 bytes。 */
  quoteBytes: Uint8Array;
  /** 报价绑定的 Seed Hash。 */
  seedHashHex: string;
  /** 重连时复用的原卖方资金会话；新销售时省略。 */
  resumeSessionId?: string;
}

export interface BitfsSellerRuntimeDeps {
  /** 当前 Key 的受限签名端口；SDK 纯步骤函数不持有该端口。 */
  signer: Signer;
  /** 当前 generation 的可用 Seed 索引。 */
  findContent(seedHashHex: string, signal?: AbortSignal): Promise<{
    fileName: string; mediaType: string; fileSizeBytes: string;
  } | undefined>;
  selectTransport(request: VerifiedHashRequest): Pick<BitfsSellerMatch, "transport" | "addresses"> | undefined;
  /** persist-before-send journal。 */
  journal: BitfsJournal;
  /** 买卖会话证据日志；用于从已开池会话中恢复原报价与池身份。 */
  sessions?: BitfsSessionJournal;
  /** 返回当前卖方设置。 */
  settings(): Readonly<BitfsSellerSettings>;
  /** 显式可信时钟；SDK 与运行单元都不自行拥有时钟。 */
  nowMs(): number;
}

export class BitfsSellerRuntime {
  private readonly seen = new Map<string, number>();
  private readonly signer: Signer;
  constructor(private readonly deps: BitfsSellerRuntimeDeps) {
    this.signer = preserveBitfsSignerErrors(deps.signer);
  }

  /**
   * 命中完整 Seed 且存在兼容 locator 时生成确定报价；未命中保持静默。
   * `webrtc-sdp` 单独走 ChannelProtocol SDP/ICE，不会伪装成 WebRTC Direct。
   */
  async match(request: VerifiedHashRequest, signal?: AbortSignal): Promise<BitfsSellerMatch | null> {
    if (!isVerifiedHashRequest(request)) throw new TypeError("卖方只接受 VerifiedHashRequest");
    const nowMs = this.deps.nowMs();
    this.prune(nowMs);
    if (request.expires_at_ms <= nowMs) return null;
    signal?.throwIfAborted();
    const settings = this.deps.settings();
    const dedup = dedupKey(request);
    const key = `${dedup.from_public_key}\0${dedup.message_id}`;
    if (this.seen.has(key)) return null;
    this.seen.set(key, request.expires_at_ms);
    const route = this.deps.selectTransport(request);
    if (!route) return null;
    const seed = await this.deps.findContent(request.body.hash, signal);
    if (!seed) return null;
    signal?.throwIfAborted();
    // 每个新 HashRequest 都生成当前设置下的新报价；旧池仅凭原 Kind 2 精确恢复。
    // 尾价由卖方按本地文件用既有公式计算并签名，买方直接采用。
    const quote = await createSellerQuote(bitfsWorkflowFacts(nowMs), this.signer, {
      seedHash: hexToBytes(request.body.hash),
      buyerPublicKey: hexToBytes(request.from_public_key),
      seedPriceSatoshis: BigInt(settings.seedPriceSatoshis),
      maxBlocksPerBatch: settings.maxBlocksPerBatch,
      fullBlockPriceSatoshis: BigInt(settings.fullBlockPriceSatoshis),
      tailPriceSatoshis: sellerTailPriceSatoshis(BigInt(settings.fullBlockPriceSatoshis), BigInt(seed.fileSizeBytes)),
      fileSizeBytes: BigInt(seed.fileSizeBytes),
      quoteExpiresAtUnixSeconds: BigInt(Math.floor((nowMs + settings.quoteLifetimeSeconds * 1_000) / 1_000)),
      supportedArbiterPublicKeys: settings.supportedArbiterPublicKeys.map(hexToBytes),
      recommendedFilename: seed.fileName,
      mediaType: seed.mediaType,
    });
    const journalId = await requestJournalId(request);
    const quoteBytes = await this.deps.journal.prepareOutbound(journalId, "seller", quote.outbound, nowMs);
    return {
      requestKey: key,
      requestMessageId: request.message_id,
      ...route,
      quoteBytes,
      seedHashHex: request.body.hash,
    };
  }

  clear(): void { this.seen.clear(); }
  private prune(nowMs: number): void { for (const [key, expiresAt] of this.seen) if (expiresAt <= nowMs) this.seen.delete(key); }
}

async function requestJournalId(request: VerifiedHashRequest): Promise<string> {
  const bytes = new TextEncoder().encode(`${request.from_public_key}\0${request.message_id}`);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(digest, (value) => value.toString(16).padStart(2, "0")).join("");
}

function hexToBytes(value: string): Uint8Array {
  if (!/^[0-9a-f]+$/u.test(value) || value.length % 2 !== 0) throw new TypeError("hex 字段不合法");
  const bytes = new Uint8Array(value.length / 2);
  for (let index = 0; index < bytes.length; index += 1) bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  return bytes;
}
