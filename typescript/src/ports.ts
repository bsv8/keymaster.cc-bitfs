import type { BitfsBroadcastOutcome } from "./broadcast.js";

export interface BitfsStore {
  get(path: string, options?: { signal?: AbortSignal }): Promise<{ bytes: Uint8Array; revision: string } | undefined>;
  put(path: string, bytes: Uint8Array, options?: {
    ifRevision?: string;
    ifNoneMatch?: true;
    signal?: AbortSignal;
  }): Promise<unknown>;
  delete(path: string): Promise<void>;
  list(input: { prefix: string; limit: number; cursor?: string }): Promise<{
    files: readonly { path: string }[];
    nextCursor?: string;
  }>;
}

export interface BitfsSellerContent {
  readSeed(seedHashHex: string): Promise<Uint8Array>;
  readBlocks(seedHashHex: string, blockHashes: readonly string[]): Promise<ReadonlyMap<string, Uint8Array>>;
}

export interface BitfsBuyerFunding {
  prepareOpening(input: {
    sessionId: string;
    openingAmountSatoshis: string;
    expiryLockTime: number;
    minerFeeRateSatoshisPerKilobyte: bigint;
    arbiterPublicKeyHex: string;
  }): Promise<Uint8Array>;
  acceptOpeningResponse(input: { sessionId: string; rawKind3: Uint8Array }): Promise<unknown>;
  cancelUnfundedOpening(sessionId: string): Promise<void>;
  submitOrReconcileFunding(sessionId: string): Promise<{ outcome: BitfsBroadcastOutcome; kind4?: Uint8Array }>;
  submitOrReconcilePoolRecovery(sessionId: string): Promise<{ outcome: BitfsBroadcastOutcome; closed: boolean }>;
}

export type BitfsBuyerPurchasePhase =
  | "discovering"
  | "opening"
  | "cancelling-opening"
  | "funding"
  | "funding-unknown"
  | "requesting-seed"
  | "requesting-blocks"
  | "payment-unknown"
  | "content-committing"
  | "closing-pool"
  | "close-unknown"
  | "cancelling-pool"
  | "cancel-unknown"
  | "cancelled"
  | "refund-ready"
  | "refund-unknown"
  | "refunded"
  | "completed"
  | "failed"
  | "connection-closed";

export type BsvNetwork = "main" | "test";

export type BitfsPoolSpendChain =
  | { kind: "unspent" }
  | { kind: "unknown"; reason: "spender_raw_unavailable" }
  | { kind: "unchanged"; txid: string }
  | { kind: "spender"; txid: string; rawTransaction: Uint8Array; status: "confirmed" | "unconfirmed" };

export interface ProtocolSpendInput {
  txid: string;
  vout: number;
  value: number;
  address: string;
  lockingScriptHex?: string;
}

export interface ProtocolFundingPlan {
  allocationId: string;
  submissionId: string;
  selected: ProtocolSpendInput[];
}

export interface ProtocolSpendOutput {
  value: number;
  scriptHex: string;
  label?: string;
}

export interface ProtocolSpendPreview {
  ownerPublicKeyHex: string;
  network: BsvNetwork;
  inputs: ProtocolSpendInput[];
  outputs: ProtocolSpendOutput[];
  changeAddress?: string;
  changeSatoshis: number;
  estimatedFeeSatoshis: number;
  txid: string;
  rawTxHex: string;
  submissionId?: string;
  managerAllocationId?: string;
}

export interface ProtocolSpendService {
  presign(input: {
    managerAllocationId: string;
    fundingSessionId: string;
    inputs: ProtocolSpendInput[];
    outputs: ProtocolSpendOutput[];
    feeRateSatoshisPerKb: number;
    maxFeeSatoshis: number;
    protocolKind: "fund";
    changeAddress: string;
  }): Promise<ProtocolSpendPreview>;
}

export interface BitfsSellerSettings {
  sellerEnabled: boolean;
  seedPriceSatoshis: string;
  fullBlockPriceSatoshis: string;
  maxBlocksPerBatch: number;
  quoteLifetimeSeconds: number;
  supportedArbiterPublicKeys: string[];
}
