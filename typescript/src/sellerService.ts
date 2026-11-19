import type { VerifiedHashRequest } from "bsv8-channel-protocol/hash-request";
import { BitfsSellerRuntime, type BitfsSellerRuntimeDeps } from "./sellerRuntime.js";
import { BitfsSellerSessionManager, type BitfsSellerSessionManagerDeps } from "./sellerSession.js";

export interface BitfsSellerOptions extends BitfsSellerSessionManagerDeps {
  discovery: BitfsSellerRuntimeDeps;
  peerIdForKey(publicKeyHex: string): string;
  createSignalingSessionId?: () => string;
}

/** Owns quote admission and the lifecycle of an embedded seller. */
export function createBitfsSeller(options: BitfsSellerOptions) {
  const matcher = new BitfsSellerRuntime(options.discovery);
  const admission = new AbortController();
  const starting = new Set<Promise<boolean>>();
  let notifyStopped: (() => void) | undefined;
  let stopped: Promise<void> | undefined;

  function updateStopped(): void {
    if (admission.signal.aborted && !starting.size && !manager.activeCount()) notifyStopped?.();
  }

  const manager = new BitfsSellerSessionManager({
    ...options,
    onActiveSessionsChanged(count) {
      updateStopped();
      options.onActiveSessionsChanged(count);
    },
  });

  async function accept(request: VerifiedHashRequest): Promise<boolean> {
    if (admission.signal.aborted || !options.protocol.ready) return false;
    if (starting.size + manager.activeCount() >= options.maxSessions()) return false;
    const opening = (async () => {
      const match = await matcher.match(request, admission.signal);
      if (!match || admission.signal.aborted) return false;
      return manager.start({
        ...match,
        sessionId: "d-" + request.digest,
        publicKeyHex: request.from_public_key,
        expectedPeerId: options.peerIdForKey(request.from_public_key),
        webrtcSessionId: match.transport === "webrtc-sdp" ? options.createSignalingSessionId?.() : undefined,
      });
    })();
    starting.add(opening);
    try { return await opening; }
    finally { starting.delete(opening); updateStopped(); }
  }

  async function waitForStop(graceMs: number): Promise<void> {
    admission.abort();
    const drained = new Promise<void>(resolve => { notifyStopped = resolve; });
    updateStopped();
    const timer = setTimeout(() => {
      manager.clear();
      notifyStopped?.();
    }, graceMs);
    try {
      await drained;
      await Promise.allSettled(starting);
    } finally {
      clearTimeout(timer);
      notifyStopped = undefined;
    }
  }

  return {
    accept,
    activeCount: () => manager.activeCount(),
    handleFrame: (input: { sessionId: string; frame: Uint8Array }) => manager.handleFrame(input),
    closeSession: (sessionId: string, reason: string) => manager.close(sessionId, reason),
    stop(graceMs = 30_000) { return stopped ??= waitForStop(graceMs); },
  };
}
