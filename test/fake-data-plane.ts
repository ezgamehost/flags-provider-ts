// A fake flags-data for the provider tests: Bun.serve speaking the data plane's wire shapes
// (flags-stack plan D2/D3): GET /v1/sync/snapshot with ETag "<version>", POST /v1/sync/stream
// (SSE `snapshot` events), POST /ofrep/v1/evaluate/flags (bulk, ETag), POST /v1/client/stream
// (SSE `changed` events). Tests drive it: set snapshots (pushed to open streams), end or drop
// streams, force a status, and read the recorded requests.
import { createHash } from "node:crypto"

export interface Recorded {
  method: string
  path: string
  headers: Record<string, string>
  body: string
  at: number
}

export interface FlagDef {
  variants: Record<string, unknown>
  defaultVariant: string
  flagVersion: number
  enabled?: boolean
  targeting?: unknown
}

/** A server snapshot in the compiler's shape (PD13). */
export function snapshot(version: number, flags: Record<string, FlagDef>, evaluators?: Record<string, unknown>): string {
  const out: Record<string, unknown> = {
    $schema: "https://flagd.dev/schema/v0/flags.json",
    metadata: { environmentId: "0199a3c2-8d31-7f02-a4b5-6c7d8e9f0a1b", snapshotVersion: version, compiledAt: "2026-10-12T09:00:00Z" },
  }
  if (evaluators) out.$evaluators = evaluators
  out.flags = Object.fromEntries(
    Object.entries(flags).map(([k, f]) => {
      const def: Record<string, unknown> = {
        state: "ENABLED",
        variants: f.variants,
        defaultVariant: f.defaultVariant,
        metadata: { flagVersion: f.flagVersion, enabled: f.enabled ?? true },
      }
      if (f.targeting !== undefined) def.targeting = f.targeting
      return [k, def]
    }),
  )
  return JSON.stringify(out)
}

export const bool = (on: boolean, flagVersion: number, extra: Partial<FlagDef> = {}): FlagDef => ({
  variants: { false: false, true: true },
  defaultVariant: on ? "true" : "false",
  flagVersion,
  ...extra,
})

export type BulkFlag =
  | { key: string; value: unknown; variant?: string; reason?: string; metadata?: Record<string, unknown> }
  | { key: string; errorCode: string; errorDetails?: string }

interface Stream {
  kind: "server" | "client"
  key: string
  ctl: ReadableStreamDefaultController<Uint8Array>
  closed: boolean
}

const enc = new TextEncoder()

export class FakeDataPlane {
  readonly requests: Recorded[] = []
  /** The current server snapshot (compact flagd JSON); null answers 503 snapshot_pending. */
  snapshot: string | null = null
  /** When set, every request gets this status (with Retry-After when given) and no stream opens. */
  force: { status: number; retryAfter?: string; paths?: string[] } | null = null
  /** Keys answered 401 (revoked or unknown). */
  readonly revoked = new Set<string>()
  /** Bulk OFREP: what the client key sees for a context. */
  bulk: (context: Record<string, unknown>) => BulkFlag[] = () => []
  clientVersion = 1
  /** The streams' Content-Type (a wrong one must not count as connected). */
  streamContentType = "text/event-stream; charset=utf-8"
  /** Streams open with a 200 but send nothing (no opening comment, no snapshot). */
  silentOpen = false
  readonly #streams = new Set<Stream>()
  readonly #server: ReturnType<typeof Bun.serve>

  constructor() {
    this.#server = Bun.serve({ port: 0, hostname: "127.0.0.1", idleTimeout: 0, fetch: (req) => this.#handle(req) })
  }

  get url(): string {
    return `http://127.0.0.1:${this.#server.port}`
  }

  get version(): number {
    return this.snapshot ? JSON.parse(this.snapshot).metadata.snapshotVersion : 0
  }

  /** Sets the server snapshot and pushes it to every open server stream. */
  setSnapshot(json: string): void {
    this.snapshot = json
    for (const s of this.#streams) if (s.kind === "server") this.#send(s, `event: snapshot\nid: ${this.version}\ndata: ${json}\n\n`)
  }

  /** Bumps the client artifact's version and sends `changed` to every open client stream. */
  clientChanged(version?: number): void {
    this.clientVersion = version ?? this.clientVersion + 1
    for (const s of this.#streams) if (s.kind === "client") this.#send(s, `event: changed\nid: ${this.clientVersion}\ndata: {"version":${this.clientVersion}}\n\n`)
  }

  /** Ends every open stream (of one key, when given) with `end` and a reason. */
  endStreams(reason: string, key?: string): void {
    for (const s of this.#streams) {
      if (key !== undefined && s.key !== key) continue
      this.#send(s, `event: end\ndata: ${JSON.stringify({ reason })}\n\n`)
      this.#close(s)
    }
  }

  /** Closes every open stream without an `end` event (a lost connection). */
  dropStreams(): void {
    for (const s of this.#streams) this.#close(s)
  }

  heartbeat(): void {
    for (const s of this.#streams) this.#send(s, ": heartbeat\n\n")
  }

  get openStreams(): number {
    return this.#streams.size
  }

  of(path: string, method?: string): Recorded[] {
    return this.requests.filter((r) => r.path === path && (method === undefined || r.method === method))
  }

  async waitFor(cond: () => boolean, timeoutMs = 3000, what = "condition"): Promise<void> {
    const end = Date.now() + timeoutMs
    while (!cond()) {
      if (Date.now() > end) throw new Error(`timed out waiting for ${what}`)
      await Bun.sleep(5)
    }
  }

  stop(): void {
    this.dropStreams()
    this.#server.stop(true)
  }

  #send(s: Stream, text: string): void {
    if (s.closed) return
    try {
      s.ctl.enqueue(enc.encode(text))
    } catch {
      this.#close(s)
    }
  }

  #close(s: Stream): void {
    if (s.closed) return
    s.closed = true
    this.#streams.delete(s)
    try {
      s.ctl.close()
    } catch {}
  }

  async #handle(req: Request): Promise<Response> {
    const url = new URL(req.url)
    const body = req.method === "GET" ? "" : await req.text()
    this.requests.push({ method: req.method, path: url.pathname, headers: Object.fromEntries(req.headers), body, at: Date.now() })
    if (this.force && (!this.force.paths || this.force.paths.includes(url.pathname))) {
      const headers: Record<string, string> = { "Content-Type": "application/json" }
      if (this.force.retryAfter !== undefined) headers["Retry-After"] = this.force.retryAfter
      return new Response(JSON.stringify({ error: { code: "forced", message: "forced by the test" } }), { status: this.force.status, headers })
    }
    const m = /^Bearer (ezgh_f([sc])_[A-Za-z0-9]{43})$/.exec(req.headers.get("authorization") ?? "")
    if (!m || this.revoked.has(m[1]!)) return json(401, { error: { code: "unauthenticated", message: "a valid SDK key is required" } })
    const key = m[1]!
    const kind = m[2] === "s" ? "server" : "client"
    const route = `${req.method} ${url.pathname}`
    if (route === "GET /v1/sync/snapshot") {
      if (kind !== "server") return json(403, { error: { code: "client_key_not_allowed", message: "server keys only" } })
      if (this.snapshot === null) return json(503, { error: { code: "snapshot_pending", message: "pending" } }, { "Retry-After": "1" })
      const etag = `"${this.version}"`
      if (req.headers.get("if-none-match") === etag) return new Response(null, { status: 304, headers: { ETag: etag } })
      return new Response(this.snapshot, { headers: { "Content-Type": "application/json", ETag: etag } })
    }
    if (route === "POST /v1/sync/stream" || route === "POST /v1/client/stream") {
      const want = route === "POST /v1/sync/stream" ? "server" : "client"
      if (kind !== want) return json(403, { error: { code: `${kind}_key_not_allowed`, message: "wrong key kind" } })
      if (want === "server" && this.snapshot === null) return json(503, { error: { code: "snapshot_pending", message: "pending" } }, { "Retry-After": "1" })
      const known = body ? (JSON.parse(body) as { version?: number }).version : undefined
      let stream!: Stream
      const rs = new ReadableStream<Uint8Array>({
        start: (ctl) => {
          stream = { kind: want, key, ctl, closed: false }
          this.#streams.add(stream)
          if (this.silentOpen) return
          ctl.enqueue(enc.encode(": open\n\n"))
          if (want === "server" && known !== this.version) ctl.enqueue(enc.encode(`event: snapshot\nid: ${this.version}\ndata: ${this.snapshot}\n\n`))
        },
        cancel: () => {
          stream.closed = true
          this.#streams.delete(stream)
        },
      })
      return new Response(rs, { headers: { "Content-Type": this.streamContentType, "Cache-Control": "no-store" } })
    }
    if (route === "POST /ofrep/v1/evaluate/flags") {
      let context: unknown
      try {
        context = (JSON.parse(body) as { context?: unknown }).context
      } catch {
        return json(400, { errorCode: "PARSE_ERROR", errorDetails: "bad JSON" })
      }
      if (typeof context !== "object" || context === null || Array.isArray(context)) return json(400, { errorCode: "INVALID_CONTEXT", errorDetails: "context" })
      const out = JSON.stringify({ flags: this.bulk(context as Record<string, unknown>) })
      const etag = `"${createHash("sha256").update(out).digest("hex")}"`
      if (req.headers.get("if-none-match") === etag) return new Response(null, { status: 304, headers: { ETag: etag } })
      return new Response(out, { headers: { "Content-Type": "application/json", ETag: etag } })
    }
    return json(404, { error: { code: "not_found", message: route } })
  }
}

function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json", ...headers } })
}

/** A syntactically valid server key (43 base62 characters). */
export const serverKey = (c = "A") => `ezgh_fs_${c.repeat(43)}`
export const clientKey = (c = "A") => `ezgh_fc_${c.repeat(43)}`
