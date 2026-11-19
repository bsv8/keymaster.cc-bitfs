import { privateKeyFromProtobuf } from "@libp2p/crypto/keys";
import { identify } from "@libp2p/identify";
import type { Libp2p, Stream } from "@libp2p/interface";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { multiaddr } from "@multiformats/multiaddr";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { createLibp2p } from "libp2p";
import { authenticateConnection } from "bitcoin-libp2p/libp2p";
import { bytesToHex, hexToBytes, peerIdFromPublicKeyBytes } from "bitcoin-libp2p/identity";
import { readUvarintFrames, writeUvarintFrame } from "bitcoin-libp2p/stream";
import { Kind, MAX_WIRE_BYTES } from "sat-subscription-protocol/protocol";
import { parse as parseWire } from "sat-subscription-protocol/wire";
import { newPublish, newRequestId, newSubscribe, parseActionResult, parsePublish } from "sat-subscription-protocol/client";
import { BITFS_PROTOCOL_ID, MAX_WIRE_FRAME_BYTES, readArtifacts, writeArtifact } from "bitfs-protocol/transport";
import { parse as parseArtifact } from "bitfs-protocol";
import { decode } from "cborg";
import type { BitfsArbiter } from "../buyerRuntime.js";

export { BITFS_PROTOCOL_ID, readArtifacts, writeArtifact };

export async function openHost(key: Uint8Array, listen: string) {
  return createLibp2p({
    privateKey: privateKeyFromProtobuf(Uint8Array.from([8, 2, 18, 32, ...key])),
    addresses: { listen: [listen] },
    transports: [tcp(), webSockets()],
    streamMuxers: [yamux()],
    connectionEncrypters: [noise()],
    services: { identify: identify() },
  });
}

export function publicKeyHex(connection: Parameters<typeof authenticateConnection>[0]): string {
  return bytesToHex(authenticateConnection(connection).publicKey);
}

export function peerIdForKey(key: string): string {
  return peerIdFromPublicKeyBytes(hexToBytes(key)).toString();
}

export async function sendFrame(stream: Stream, payload: Uint8Array, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  if (!writeArtifact(stream, parseArtifact(payload))) await stream.onDrain({ signal });
}

export async function openGateway(host: Libp2p, address: string, signal: AbortSignal) {
  const stream = await host.dialProtocol(multiaddr(address), "/ssp/1.0.0", { signal });
  const pending = new Map<string, { resolve(value: Uint8Array): void; reject(reason: unknown): void }>();
  let deliver: ((channel: string, body: Uint8Array) => void) | undefined;
  const reader = async () => {
    try {
      for await (const frame of readUvarintFrames(stream, { maxInboundFrameBytes: MAX_WIRE_BYTES, maxBufferedFrames: 16 })) {
        const envelope = parseWire(frame);
        if (envelope.kind === Kind.ActionResult) {
          const response = parseActionResult(frame);
          pending.get(bytesToHex(response.requestId))?.resolve(frame);
        } else if (envelope.kind === Kind.Publish) {
          const input = parsePublish(frame);
          deliver?.(input.channel, input.contentJson);
        }
      }
    } finally {
      for (const waiter of pending.values()) waiter.reject(new Error("BitFS supplier connection closed"));
    }
  };
  void reader().catch(error => { if (!signal.aborted) console.error("BitFS supplier:", error); });
  async function action(build: (id: ReturnType<typeof newRequestId>) => Uint8Array, parent: AbortSignal): Promise<void> {
    const callSignal = AbortSignal.any([parent, AbortSignal.timeout(20_000)]);
    callSignal.throwIfAborted();
    const id = newRequestId();
    const idKey = bytesToHex(id);
    let abort!: () => void;
    const reply = new Promise<Uint8Array>((resolve, reject) => {
      pending.set(idKey, { resolve, reject });
      abort = () => reject(callSignal.reason);
      callSignal.addEventListener("abort", abort, { once: true });
    });
    // The reader/timeout can settle while the stream is draining.
    void reply.catch(() => undefined);
    try {
      if (!writeUvarintFrame(stream, build(id))) await stream.onDrain({ signal: callSignal });
      const result = parseActionResult(await reply);
      if (!result.success) throw new Error(`BitFS supplier rejected request (${result.errorCode})`);
    } finally {
      pending.delete(idKey);
      callSignal.removeEventListener("abort", abort);
    }
  }
  return {
    publish: (channel: string, body: Uint8Array, signal: AbortSignal) => action(id => newPublish(id, channel, body), signal),
    subscribe: async (channel: string, handler: NonNullable<typeof deliver>) => {
      deliver = handler;
      await action(id => newSubscribe(id, channel), signal);
    },
    close: async () => stream.close().catch(() => undefined),
  };
}

export async function arbitrationPrices(host: Libp2p, endpoint: BitfsArbiter, signal: AbortSignal) {
  const bounded = AbortSignal.any([signal, AbortSignal.timeout(20_000)]);
  const connection = await host.dial(multiaddr(endpoint.address), { signal: bounded });
  authenticateConnection(connection, { publicKey: hexToBytes(endpoint.publicKeyHex) });
  const stream = await connection.newStream("/bitfs/arbitration/info/1.0.0", { signal: bounded });
  const frames = readUvarintFrames(stream, { signal: bounded, maxInboundFrameBytes: MAX_WIRE_FRAME_BYTES });
  try {
    const reply = frames.next();
    void reply.catch(() => undefined);
    if (!writeUvarintFrame(stream, Uint8Array.of(1))) await stream.onDrain({ signal: bounded });
    const frame = await reply;
    if (frame.done) throw new Error("BitFS arbiter did not respond");
    const outer: unknown = decode(frame.value);
    if (!Array.isArray(outer) || outer[0] !== 1 || outer[1] !== 0 || outer[2] !== 0 || !(outer[3] instanceof Uint8Array)) {
      throw new Error("BitFS invalid arbiter fee envelope");
    }
    const data: unknown = decode(outer[3]);
    if (!Array.isArray(data) || data.length !== 9 || data[0] !== 2 || data[1] !== peerIdForKey(endpoint.publicKeyHex)
      || data[8] !== "content-item" || !Number.isSafeInteger(data[3]) || data[3] < 1
      || !Number.isSafeInteger(data[4]) || data[4] < 0) {
      throw new Error("BitFS invalid arbiter fee schedule");
    }
    return { base: String(data[3]), perItem: String(data[4]) };
  } finally {
    await frames.return?.();
    await stream.close().catch(() => undefined);
  }
}
