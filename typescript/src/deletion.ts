import { createBitfsFundingLedger } from "./funding.js";
import type { BitfsStore, BsvNetwork } from "./ports.js";
import { createBitfsPurchaseTasks } from "./purchaseTasks.js";
import { createBitfsSessionJournal } from "./sessionJournal.js";

export interface DeleteDownloadOptions {
  seedHashHex: string;
  ownerPublicKeyHex: string;
  network: BsvNetwork;
  content: BitfsStore;
  journal: BitfsStore;
  tryLock(key: string): Promise<(() => void) | undefined>;
}

export function downloadLockKey(owner: string, network: BsvNetwork, seed: string): string {
  return `bitfs:download:${owner}:${network}:${seed}`;
}

export function downloadIntentPath(seed: string): string {
  return `download-intents/${seed}.json`;
}

export function downloadDeletionPath(seed: string): string {
  return `download-deleting/${seed}.json`;
}

async function deletePrefix(store: BitfsStore, prefix: string): Promise<void> {
  for (;;) {
    const page = await store.list({ prefix, limit: 200 });
    if (!page.files.length) return;
    for (const file of page.files) await store.delete(file.path);
  }
}

/** Delete one stopped workspace; preserve published content and recovery evidence. */
export async function deleteBitfsDownload(input: DeleteDownloadOptions): Promise<{ recoveryPending: boolean }> {
  const { ownerPublicKeyHex: owner, network, seedHashHex: seed, content, journal } = input;
  if (!/^(02|03)[0-9a-f]{64}$/u.test(owner) || !/^[0-9a-f]{64}$/u.test(seed)) {
    throw new Error("BitFS 下载身份无效");
  }
  const release = await input.tryLock(downloadLockKey(owner, network, seed));
  if (!release) throw new Error("BitFS 下载仍在运行");
  try {
    const path = downloadDeletionPath(seed);
    await journal.put(path, new TextEncoder().encode(seed));
    const tasks = createBitfsPurchaseTasks(journal, owner, seed);
    const rows = await tasks.list();
    const sessions = (await createBitfsSessionJournal(journal).list())
      .filter(item => item.role === "buyer" && item.seedHashHex === seed && item.ownerPublicKeyHex === owner);
    await deletePrefix(content, `bitfs-staging/by-seed/${seed}/`);
    for (const item of sessions) await deletePrefix(content, `bitfs-staging/${item.sessionId}/`);
    const account = await createBitfsFundingLedger(journal).getAccount({
      ownerPublicKeyHex: owner, network, seedHashHex: seed, nowMs: Date.now(),
    });
    const recoveryPending = account.pools.some(pool => pool.state !== "closed" && pool.state !== "funding-failed")
      || account.transactions.some(tx => tx.state === "prepared" || tx.state === "result-unknown")
      || sessions.some(item => item.pendingAuthorizationId && !["cancelled", "completed", "refunded"].includes(item.phase));
    if (recoveryPending) return { recoveryPending };
    for (const row of rows) {
      await journal.delete(`scheduler/tasks/${row.taskId}.json`);
      const planName = row.legacyPlan ? `${seed}.json` : `${seed}/${row.taskId}.json`;
      await journal.delete(`download-plans/${planName}`);
    }
    await tasks.forgetSessions(rows.flatMap(row => row.sessionIds));
    await journal.delete(downloadIntentPath(seed));
    await journal.delete(path);
    return { recoveryPending };
  } finally {
    release();
  }
}
