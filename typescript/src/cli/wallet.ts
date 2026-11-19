import { createHash, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { BigNumber, ECDSA, LockingScript, P2PKH, PrivateKey, Transaction } from "@bsv/sdk";
import { FundingKernel, parseTx, type AllocationMaterial } from "keymaster-utxo-manager";
import type { Signer } from "bitfs-protocol";
import type { BitfsChainPort } from "../broadcast.js";
import type { BitfsFundingTransactionView } from "../funding.js";
import type { BsvNetwork, ProtocolFundingPlan, ProtocolSpendService } from "../ports.js";

export function parseFundingTransaction(raw: string, expected?: string): BitfsFundingTransactionView {
  const parsed = parseTx(raw);
  if (expected && expected !== parsed.canonicalTxid) throw new Error("BitFS transaction identity mismatch");
  return {
    canonicalTxid: parsed.canonicalTxid,
    inputs: parsed.inputs.map(input => `${input.txid}:${input.vout}`),
    outputs: parsed.outputs.map((output, vout) => ({ vout, valueSatoshis: Number(output.value), scriptHex: output.scriptHex })),
  };
}

export function openWallet(keyHex: string, network: BsvNetwork, chainURL: string) {
  const privateKey = PrivateKey.fromHex(keyHex);
  const address = privateKey.toAddress(network === "main" ? "mainnet" : "testnet");
  const template = new P2PKH();
  const script = template.lock(address);
  const scriptHex = script.toHex();
  const pubkey = Uint8Array.from(Buffer.from(privateKey.toPublicKey().toDER("hex") as string, "hex"));
  const owner = Buffer.from(pubkey).toString("hex");
  const signer: Signer = {
    publicKey: () => pubkey.slice(),
    async sign(request, signal) {
      signal?.throwIfAborted();
      return Uint8Array.from(ECDSA.sign(new BigNumber(Array.from(request.digest)), privateKey, true).toDER() as number[]);
    },
  };
  const manager = new FundingKernel({ owner, network }, { dustThresholdSats: "546" });
  const allocations = new Map<string, { plan: AllocationMaterial; submissionId: string; txid?: string; dispatched: boolean }>();
  let revision = 0;
  const base = chainURL.replace(/\/+$/u, "");
  let nextRequest = 0;
  async function request(path: string, init: RequestInit = {}): Promise<Response> {
    const headers = new Headers(init.headers);
    if (process.env.BITFS_CHAIN_API_KEY) headers.set("Authorization", process.env.BITFS_CHAIN_API_KEY);
    const signal = init.signal ?? AbortSignal.timeout(40_000);
    for (let attempt = 0; ; attempt++) {
      const slot = Math.max(Date.now(), nextRequest);
      nextRequest = slot + 800;
      if (slot > Date.now()) await delay(slot - Date.now(), undefined, { signal });
      const response = await fetch(base + path, { ...init, headers, signal });
      if (response.status !== 429 || attempt === 3) return response;
      const retryAfter = response.headers.get("Retry-After");
      const seconds = retryAfter === null ? NaN : Number(retryAfter);
      const date = retryAfter === null ? NaN : Date.parse(retryAfter);
      const retryMs = Number.isFinite(seconds) ? seconds * 1000
        : Number.isFinite(date) ? date - Date.now() : 1500 * (attempt + 1);
      if (retryMs > 12_000) return response;
      await response.body?.cancel();
      const waitUntil = Date.now() + Math.max(800, retryMs);
      nextRequest = Math.max(nextRequest, waitUntil);
    }
  }
  let height: { value: number; expiresAt: number } | undefined;
  let heightRequest: Promise<number> | undefined;
  async function blockHeight(): Promise<number> {
    if (height && height.expiresAt > Date.now()) return height.value;
    heightRequest ??= (async () => {
      const response = await request("/chain/info");
      if (!response.ok) throw new Error(`BitFS chain height HTTP ${response.status}`);
      const info = await response.json() as { blocks?: unknown };
      if (typeof info.blocks !== "number" || !Number.isSafeInteger(info.blocks) || info.blocks < 0) throw new Error("Invalid chain height");
      height = { value: info.blocks, expiresAt: Date.now() + 15_000 };
      return height.value;
    })().finally(() => { heightRequest = undefined; });
    return heightRequest;
  }
  async function refresh(): Promise<void> {
    const snapshotRevision = ++revision;
    const rows = new Map<string, { txid: string; vout: number; value: string; height: number; status: "confirmed" | "unconfirmed" }>();
    const spent = new Set<string>();
    for (const status of ["confirmed", "unconfirmed"] as const) {
      const response = await request(`/address/${address}/${status}/unspent`);
      if (response.status === 404) continue;
      if (!response.ok) throw new Error(`BitFS UTXO refresh HTTP ${response.status}`);
      const page = await response.json() as { result?: Array<{ tx_hash: string; tx_pos: number; value: number; height?: number; isSpentInMempoolTx?: boolean }> };
      if (!Array.isArray(page.result)) throw new Error("Invalid BitFS UTXO list");
      for (const item of page.result) {
        const key = `${item.tx_hash}:${item.tx_pos}`;
        if (item.isSpentInMempoolTx === true) {
          spent.add(key);
          rows.delete(key);
        } else if (!spent.has(key) && item.value > 0) {
          rows.set(key, { txid: item.tx_hash, vout: item.tx_pos, value: String(item.value),
            height: item.height ?? 0, status });
        }
      }
    }
    manager.applySnapshot({ generation: 1, revision: snapshotRevision, utxos: [...rows.values()] });
  }

  let allocationTail = Promise.resolve();
  const allocationRequest = async (input: { amountSatoshis: number; feeRateSatoshisPerKb: number; outputs: Array<{scriptHex: string}> }): Promise<ProtocolFundingPlan> => {
    await refresh();
    const submissionId = randomUUID();
    const plan = manager.request({ requestId: submissionId, businessAmountSats: String(input.amountSatoshis),
      feeRateSatPerKb: String(input.feeRateSatoshisPerKb), outputScriptLengths: input.outputs.map(output => output.scriptHex.length / 2) });
    allocations.set(plan.allocationId, { plan, submissionId, dispatched: false });
    return { allocationId: plan.allocationId, submissionId,
      selected: plan.inputs.map(item => ({ txid: item.txid, vout: item.vout, value: Number(item.value), address })) };
  };
  const planFunding = (input: Parameters<typeof allocationRequest>[0]) => {
    const pending = allocationTail.then(() => allocationRequest(input));
    allocationTail = pending.then(() => undefined, () => undefined);
    return pending;
  };
  async function releaseFundingPlan(input: { allocationId: string; submissionId: string }): Promise<boolean> {
    const record = allocations.get(input.allocationId);
    if (!record) return false;
    if (record.dispatched || record.submissionId !== input.submissionId) return false;
    manager.release(input.allocationId);
    allocations.delete(input.allocationId);
    return true;
  }
  async function releasePreparedSubmission(input: { txid: string; submissionId: string }): Promise<void> {
    for (const [allocationId, record] of allocations) {
      if (record.submissionId === input.submissionId && record.txid === input.txid) {
        await releaseFundingPlan({ allocationId, submissionId: record.submissionId });
        return;
      }
    }
  }
  const protocolSpend: ProtocolSpendService = {
    async presign(input) {
      const record = allocations.get(input.managerAllocationId);
      if (!record || record.dispatched) throw new Error("BitFS wallet allocation is unavailable");
      // With final input sequences the locktime is inactive. Bind the raw
      // funding transaction ID to this pool even when a prior canceled pool
      // and this pool happen to choose the same wallet inputs and amounts.
      const lockTime = createHash("sha256").update(input.fundingSessionId).digest().readUInt32LE();
      const transaction = new Transaction(1, [], [], lockTime);
      for (const coin of input.inputs) {
        transaction.addInput({ sourceTXID: coin.txid, sourceOutputIndex: coin.vout, sequence: 0xffffffff,
          unlockingScriptTemplate: template.unlock(privateKey, "all", false, coin.value, script) });
      }
      for (const output of input.outputs) transaction.addOutput({ satoshis: output.value, lockingScript: LockingScript.fromHex(output.scriptHex) });
      const fee = Number(record.plan.feeSats);
      const change = Number(record.plan.changeSats);
      if (fee > input.maxFeeSatoshis) throw new Error("BitFS funding fee exceeds maximum");
      if (change > 0) transaction.addOutput({ satoshis: change, lockingScript: script });
      await transaction.sign();
      const rawTxHex = transaction.toHex();
      const txid = parseFundingTransaction(rawTxHex).canonicalTxid;
      record.txid = txid;
      return { ownerPublicKeyHex: owner, network, inputs: input.inputs, outputs: input.outputs, changeAddress: address,
        changeSatoshis: change, estimatedFeeSatoshis: fee, txid, rawTxHex,
        submissionId: record.submissionId, managerAllocationId: record.plan.allocationId };
    },
  };
  const chain: BitfsChainPort = {
    async broadcast({ txid, rawTxHex }) {
      const record = [...allocations.values()].find(item => item.txid === txid);
      if (record) record.dispatched = true;
      const response = await request("/tx/raw", { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ txhex: rawTxHex }) });
      const receipt = await response.text();
      const returned = receipt.trim().replace(/^"|"$/gu, "");
      if (!response.ok || returned !== txid) {
        const observation = await chain.lookupTransaction(txid);
        if (observation !== "mempool" && observation !== "confirmed") {
          throw new Error(`BitFS broadcast outcome unknown (HTTP ${response.status})`);
        }
      }
      const outputs = parseFundingTransaction(rawTxHex, txid).outputs.filter(output => output.scriptHex === scriptHex && output.valueSatoshis > 0);
      manager.injectP2pkhUtxos(outputs.map(output => ({ txid, vout: output.vout, value: String(output.valueSatoshis) })));
      return { outcome: response.ok && returned === txid ? "accepted" : "already-known" };
    },
    async lookupTransaction(txid) {
      try {
        const response = await request(`/tx/${txid}/hex`);
        if (response.status === 404) return "absent";
        if (!response.ok) return "unknown";
        const raw = (await response.text()).trim().replace(/^"|"$/gu, "");
        return parseFundingTransaction(raw).canonicalTxid === txid ? "mempool" : "unknown";
      } catch {
        return "unknown";
      }
    },
  };
  return { owner, address, scriptHex, signer, chain, blockHeight, planFunding, protocolSpend, releaseFundingPlan, releasePreparedSubmission };
}
