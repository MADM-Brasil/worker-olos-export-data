/** SQL INT as documented; unconfirmed zero/negative sentinel values never join. */
export function agentId(value: unknown): string | null {
  const s=typeof value==='number' && Number.isSafeInteger(value) ? String(value) : value;
  return typeof s==='string' && /^[1-9]\d{0,9}$/.test(s) && BigInt(s)<=2147483647n ? s : null;
}
