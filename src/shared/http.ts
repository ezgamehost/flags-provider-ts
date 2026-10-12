// Small helpers both providers share: the default endpoint, Retry-After, backoff and abortable sleep.

export const DEFAULT_ENDPOINT = "https://flags.ezghcloud.com"

export function trimEndpoint(endpoint: string | undefined): string {
  return (endpoint ?? DEFAULT_ENDPOINT).replace(/\/+$/, "")
}

/** A Retry-After header (seconds or an HTTP date) in milliseconds, or undefined. */
export function retryAfterMs(res: Response, now = Date.now()): number | undefined {
  const h = res.headers.get("retry-after")
  if (h === null) return undefined
  const trimmed = h.trim()
  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1000
  const at = Date.parse(trimmed)
  return Number.isNaN(at) ? undefined : Math.max(0, at - now)
}

/**
 * Exponential backoff with jitter: attempt 1 waits between min/2 and min, doubling up to max, and
 * never less than the server's Retry-After, itself capped at max (a broken or hostile Retry-After
 * can't park a provider for hours).
 */
export function backoffMs(attempt: number, minMs: number, maxMs: number, retryAfter?: number): number {
  const base = Math.min(maxMs, minMs * 2 ** Math.max(0, attempt - 1))
  const jittered = base / 2 + (Math.random() * base) / 2
  return Math.max(jittered, Math.min(retryAfter ?? 0, maxMs))
}

/** Resolves after ms, or at once when the signal aborts. */
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve()
    const done = () => {
      clearTimeout(t)
      signal.removeEventListener("abort", done)
      resolve()
    }
    const t = setTimeout(done, ms)
    signal.addEventListener("abort", done, { once: true })
  })
}

/** An AbortController that also aborts when the parent signal does. */
export function childController(parent: AbortSignal): AbortController {
  const c = new AbortController()
  if (parent.aborted) c.abort()
  else parent.addEventListener("abort", () => c.abort(), { once: true, signal: c.signal })
  return c
}

/** Whether a status means the key itself is refused (unknown, revoked, or the wrong kind). */
export function isFatalStatus(status: number): boolean {
  return status === 401 || status === 403
}
