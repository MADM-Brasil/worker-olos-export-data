import { OlosError } from './types.js';

// Protótipos offline de FRONTEIRA DE VALOR, sem conexão, framing TDS, patch ou hook no parser instalado.
// Bytes já devem ter sido separados por um futuro transporte certificado, antes de readValue do Tedious.
// Não são conectados ao coletor, não certificam exact-text/v1 e não aprovam persistência.
export function decimalFromTdsBody(body: Uint8Array | null, precision: number, scale: number): string | null {
  if (!Number.isInteger(precision) || precision < 1 || precision > 38 || !Number.isInteger(scale) || scale < 0 || scale > precision) throw new OlosError('PRECISION_UNSAFE');
  if (body === null) return null;
  const length = precision <= 9 ? 5 : precision <= 19 ? 9 : precision <= 28 ? 13 : 17;
  if (!(body instanceof Uint8Array) || body.length !== length || body[0] !== 0 && body[0] !== 1) throw new OlosError('PRECISION_UNSAFE');
  let magnitude = 0n;
  for (let i = body.length - 1; i >= 1; i--) magnitude = magnitude * 256n + BigInt(body[i]!);
  if (magnitude >= 10n ** BigInt(precision)) throw new OlosError('PRECISION_UNSAFE');
  const digits = magnitude.toString().padStart(scale + 1,'0');
  const signed = body[0] === 0 && magnitude !== 0n ? '-' : '';
  return signed + (scale === 0 ? digits : `${digits.slice(0,-scale)}.${digits.slice(-scale)}`);
}
/** SQL datetime possui ticks de 1/300s: não arredondar para Date nem fingir fração decimal finita exata. */
export function legacyDatetimeFromTdsBody(body: Uint8Array | null): Readonly<{
  representation: 'sql-datetime-ticks/v1'; daysSince1900: string; ticks300: string; rawHex: string;
}> | null {
  if (body === null) return null;
  if (!(body instanceof Uint8Array) || body.length !== 8) throw new OlosError('PRECISION_UNSAFE');
  const bytes = Buffer.from(body), days = bytes.readInt32LE(0), ticks = bytes.readUInt32LE(4);
  const epoch = Date.UTC(1900,0,1);
  const minDays = (Date.UTC(1753,0,1) - epoch) / 86_400_000, maxDays = (Date.UTC(9999,11,31) - epoch) / 86_400_000;
  if (days < minDays || days > maxDays || ticks >= 86_400 * 300) throw new OlosError('PRECISION_UNSAFE');
  return Object.freeze({ representation: 'sql-datetime-ticks/v1', daysSince1900: String(days), ticks300: String(ticks), rawHex: bytes.toString('hex') });
}
