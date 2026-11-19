import { bitfsDebug } from "bitfs-protocol";

export function traceBitfs(event: string, details: () => unknown = () => ({})): void {
  bitfsDebug("keymaster.cc-bitfs", event, details);
}
