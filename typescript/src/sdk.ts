// BitFSProtocol TypeScript 角色工作流的 Keymaster 适配层。
// SDK 只负责协议计算与验证；存储、时钟、网络、广播均由上层显式注入。

import { hexToBytes } from "bitcoin-libp2p/identity";
import {
  MultisigPoolEngine,
  WireError,
  type Signer,
  type SigningRequest,
  type PureFunctionFacts,
} from "bitfs-protocol";

/** 由 Coordinator 提供的显式 UTC 秒事实；SDK 不读取系统时钟。 */
export function bitfsWorkflowFacts(nowMs: number, blockHeight?: number): PureFunctionFacts {
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) throw new TypeError("BitFS 当前时间必须是非负毫秒整数");
  if (blockHeight !== undefined && (!Number.isSafeInteger(blockHeight) || blockHeight < 0)) {
    throw new TypeError("BitFS 当前区块高度必须是非负整数");
  }
  return {
    nowUnixSeconds: BigInt(Math.floor(nowMs / 1_000)),
    ...(blockHeight === undefined ? {} : { blockHeight }),
  };
}

/** SDK 会保留 WireError；在应用签名边界保留钱包的具体失败原因和稳定错误码。 */
export function preserveBitfsSignerErrors(signer: Signer): Signer {
  return {
    publicKey: () => signer.publicKey().slice(),
    async sign(request, signal) {
      try {
        return await signer.sign(request, signal);
      } catch (error) {
        if (error instanceof WireError || (error instanceof DOMException && error.name === "AbortError")) throw error;
        const message = error instanceof Error ? error.message
          : typeof error === "string" ? error
          : error && typeof error === "object" && "message" in error && typeof error.message === "string" ? error.message
          : "钱包签名未返回具体失败原因";
        const code = error && typeof error === "object" && "code" in error && typeof error.code === "string"
          ? ` [${error.code}]` : "";
        throw new WireError("signer_unavailable", request.wireKind, "signature", `BitFS 签名失败${code}：${message}`);
      }
    },
  };
}

export function deriveBitfsPoolLockingScript(input: {
  /** 当前买方的压缩公钥。 */
  buyerPublicKeyHex: string;
  /** 报价绑定的卖方压缩公钥。 */
  sellerPublicKeyHex: string;
  /** 报价允许且本次选中的仲裁方压缩公钥。 */
  arbiterPublicKeyHex: string;
}): Uint8Array {
  const engine = new MultisigPoolEngine({
    buyerPublicKey: hexToBytes(input.buyerPublicKeyHex),
    sellerPublicKey: hexToBytes(input.sellerPublicKeyHex),
    arbiterPublicKey: hexToBytes(input.arbiterPublicKeyHex),
  });
  return engine.lockingScript().slice();
}

