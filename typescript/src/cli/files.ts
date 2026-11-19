import { createHash, randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import { link, mkdir, open, readFile, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { BLOCK_SIZE, createSeed, Digest, verifyBlock } from "masterseed";
import type { BitfsVerifiedDownload } from "../buyerProtocol.js";
import type { BitfsSellerContent, BitfsStore } from "../ports.js";

function isMissing(error: unknown): boolean {
  return (error as NodeJS.ErrnoException)?.code === "ENOENT";
}

function conflict(): Error {
  return Object.assign(new Error("BitFS storage revision conflict"), { code: "storage_conflict" });
}

export function directoryStore(root: string): BitfsStore {
  const base = resolve(root);
  const writes = new Map<string, Promise<unknown>>();
  function path(name: string): string {
    if (name.split("/").some(part => !part || part === "." || part === ".." || part.includes("\\"))) {
      throw new Error("Invalid BitFS storage path");
    }
    return join(base, name);
  }
  async function get(name: string) {
    try {
      const bytes = await readFile(path(name));
      return { bytes, revision: createHash("sha256").update(bytes).digest("hex") };
    } catch (error) {
      if (isMissing(error)) return undefined;
      throw error;
    }
  }
  async function scan(directory: string, prefix = ""): Promise<string[]> {
    const entries = await readdir(directory, { withFileTypes: true }).catch(error => {
      if (isMissing(error)) return [];
      throw error;
    });
    const names: string[] = [];
    for (const entry of entries) {
      if (entry.name.startsWith(".write-")) continue;
      const name = prefix + entry.name;
      if (entry.isDirectory()) names.push(...await scan(join(directory, entry.name), name + "/"));
      if (entry.isFile()) names.push(name);
    }
    return names;
  }
  return {
    get,
    put(name, bytes, options) {
      const prior = writes.get(name);
      const pending = Promise.resolve(prior).catch(() => undefined).then(async () => {
        options?.signal?.throwIfAborted();
        const target = path(name);
        if (options?.ifRevision && (await get(name))?.revision !== options.ifRevision) throw conflict();
        await mkdir(dirname(target), { recursive: true, mode: 0o700 });
        const temporary = join(dirname(target), ".write-" + randomUUID());
        try {
          await writeFile(temporary, bytes, { mode: 0o600, flag: "wx", signal: options?.signal });
          options?.signal?.throwIfAborted();
          if (options?.ifNoneMatch) {
            await link(temporary, target).catch(error => {
              if (error.code === "EEXIST") throw conflict();
              throw error;
            });
          } else {
            await rename(temporary, target);
          }
        } finally {
          await rm(temporary, { force: true });
        }
      });
      writes.set(name, pending);
      return pending.finally(() => { if (writes.get(name) === pending) writes.delete(name); });
    },
    async delete(name) {
      await rm(path(name), { force: true });
    },
    async list({ prefix, cursor, limit }) {
      const names = (await scan(base)).filter(name => name.startsWith(prefix) && (cursor === undefined || name > cursor)).sort();
      const files = names.slice(0, limit).map(path => ({ path }));
      return { files, nextCursor: names.length > limit ? files.at(-1)?.path : undefined };
    },
  };
}

export async function lockDirectory(root: string, key: string): Promise<(() => void) | undefined> {
  await mkdir(root, { recursive: true, mode: 0o700 });
  const file = join(root, createHash("sha256").update(key).digest("hex"));
  return new Promise((resolve, reject) => {
    const process = spawn("flock", ["-n", file, "sh", "-c", "printf 'locked\\n'; cat >/dev/null"], {
      stdio: ["pipe", "pipe", "inherit"],
    });
    process.stdin.on("error", reject);
    process.once("error", reject);
    process.once("exit", code => {
      if (code === 1) resolve(undefined);
      else if (code !== 0) reject(new Error(`flock exited with status ${code}`));
    });
    process.stdout.once("data", () => resolve(() => { process.stdin.end(); }));
  });
}

interface CatalogEntry {
  fileName: string;
  fileSizeBytes: string;
  mediaType: string;
  path: string;
  seed: Buffer;
  offsets: Map<string, number>;
  size: number;
}

export async function indexDirectory(directory: string) {
  const catalog = new Map<string, CatalogEntry>();
  async function scan(path: string): Promise<void> {
    for (const entry of await readdir(path, { withFileTypes: true })) {
      const file = join(path, entry.name);
      if (entry.isDirectory()) {
        await scan(file);
        continue;
      }
      if (!entry.isFile()) continue;
      const before = await stat(file);
      const pieces: Buffer[] = [];
      const info = await createSeed(createReadStream(file), { write: bytes => { pieces.push(Buffer.from(bytes)); } });
      const after = await stat(file);
      if (after.size !== before.size || after.mtimeMs !== before.mtimeMs || info.sourceSize !== BigInt(before.size)) {
        throw new Error(`Seller content changed while indexing ${entry.name}`);
      }
      const bytes = Buffer.concat(pieces);
      const offsets = new Map<string, number>();
      for (let index = 0; index < bytes.length / 32; index++) {
        const hash = bytes.subarray(index * 32, index * 32 + 32).toString("hex");
        // Prefer a full block when its digest also occurs at another offset.
        if (!offsets.has(hash)) offsets.set(hash, index * BLOCK_SIZE);
      }
      catalog.set(info.seedHashHex, {
        fileName: entry.name, fileSizeBytes: before.size.toString(), mediaType: "application/octet-stream",
        path: file, seed: bytes, size: before.size, offsets,
      });
    }
  }
  await scan(directory);
  const content: BitfsSellerContent = {
    async readSeed(seed) {
      const entry = catalog.get(seed);
      if (!entry) throw new Error("Seller Seed not available");
      return entry.seed.slice();
    },
    async readBlocks(seed, hashes) {
      const entry = catalog.get(seed);
      if (!entry) throw new Error("Seller Seed not available");
      const file = await open(entry.path, "r");
      try {
        const blocks = new Map<string, Uint8Array>();
        for (const hash of hashes) {
          const offset = entry.offsets.get(hash);
          if (offset === undefined) throw new Error("Seller Block does not belong to Seed");
          const block = Buffer.alloc(Math.min(BLOCK_SIZE, entry.size - offset));
          const { bytesRead } = await file.read(block, 0, block.length, offset);
          if (bytesRead !== block.length) throw new Error("Seller file changed after indexing");
          verifyBlock(block, Digest.fromHex(hash));
          blocks.set(hash, block);
        }
        return blocks;
      } finally {
        await file.close();
      }
    },
  };
  return { find: (hash: string) => catalog.get(hash), content, size: catalog.size };
}

export async function publishDownload(directory: string, content: BitfsVerifiedDownload): Promise<string> {
  const raw = content.manifest.recommendedFilename.replace(/\\/gu, "/");
  const candidate = basename(raw).trim().replace(/[\x00-\x1f\x7f]/gu, "");
  const file = !candidate || candidate === "." || candidate === ".." ? content.seedHashHex : candidate;
  const output = join(directory, file);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = join(directory, ".write-" + randomUUID());
  const handle = await open(temporary, "wx", 0o600);
  try {
    try {
      for (const hash of content.blockHashes) {
        const block = await content.readBlock(hash);
        verifyBlock(block, Digest.fromHex(hash));
        await handle.writeFile(block);
      }
    } finally {
      await handle.close();
    }
    await link(temporary, output).catch(async error => {
      if (error.code !== "EEXIST") throw error;
      const digest = async (path: string) => {
        const hash = createHash("sha256");
        for await (const bytes of createReadStream(path)) hash.update(bytes);
        return hash.digest("hex");
      };
      if (await digest(temporary) !== await digest(output)) throw error;
    });
    return output;
  } finally {
    await rm(temporary, { force: true });
  }
}
