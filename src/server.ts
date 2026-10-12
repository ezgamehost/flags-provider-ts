// EzghProvider: the OpenFeature server provider (Node, Bun, Deno). It holds the environment's
// current snapshot and evaluates every flag locally through flagd's core; nothing is sent per
// evaluation. It loads the snapshot (GET /v1/sync/snapshot), then follows POST /v1/sync/stream
// for new versions. If the stream keeps failing it polls the snapshot with If-None-Match while it
// retries the stream with backoff. The only state it keeps is the current snapshot.
import {
  ErrorCode,
  GeneralError,
  OpenFeatureEventEmitter,
  ProviderEvents,
  ProviderFatalError,
  type EvaluationContext,
  type JsonValue,
  type Logger,
  type Provider,
  type ResolutionDetails,
} from "@openfeature/server-sdk"
import { changedKeys, Evaluator, type FlagType } from "./shared/evaluator.ts"
import { backoffMs, childController, isFatalStatus, retryAfterMs, sleep, trimEndpoint } from "./shared/http.ts"
import { endReason, isEventStream, watchedSSE } from "./shared/sse.ts"

export { DEFAULT_ENDPOINT } from "./shared/http.ts"

export interface EzghProviderOptions {
  /** A server SDK key (`ezgh_fs_…`). Required unless `offline`. */
  sdkKey?: string
  /** The data plane; defaults to https://flags.ezghcloud.com. */
  endpoint?: string
  /** A snapshot file (`ezgh flags export`): the provider is ready from it without the network. */
  bootstrapPath?: string
  /** Use only `bootstrapPath`; never call the network (tests, CI, air-gapped runs). */
  offline?: boolean
  /** The fetch to use; defaults to the global one. */
  fetch?: typeof fetch
  /** Logs snapshot and connection problems; defaults to console warnings and errors. */
  logger?: Logger
  /** How long `initialize` waits for the first snapshot (default 10 s). Syncing continues after. */
  timeoutMs?: number
  /** The snapshot poll interval while the stream is down (default 30 s). */
  pollIntervalMs?: number
  /** Stale after this long without reaching the data plane (default 30 s). */
  staleAfterMs?: number
  /** Stream reconnect backoff bounds (default 1 s to 60 s, with jitter). */
  minBackoffMs?: number
  maxBackoffMs?: number
  /** A stream that sends nothing (not even a heartbeat) for this long is reconnected (default 45 s). */
  idleTimeoutMs?: number
}

const STREAM_FAILURES_BEFORE_POLLING = 3

const defaultLogger: Logger = {
  error: (...a: unknown[]) => console.error("[ezgh-flags]", ...a),
  warn: (...a: unknown[]) => console.warn("[ezgh-flags]", ...a),
  info() {},
  debug() {},
}

type StreamOutcome = { kind: "end"; reason: string } | { kind: "failed"; retryAfter?: number; established: boolean } | { kind: "fatal" }

export class EzghProvider implements Provider {
  readonly metadata = { name: "ezgh-flags" } as const
  readonly runsOn = "server" as const
  readonly events = new OpenFeatureEventEmitter()

  readonly #key: string
  readonly #endpoint: string
  readonly #bootstrapPath: string | undefined
  readonly #offline: boolean
  readonly #fetch: typeof fetch
  readonly #log: Logger
  readonly #timeoutMs: number
  readonly #pollMs: number
  readonly #staleMs: number
  readonly #minBackoff: number
  readonly #maxBackoff: number
  readonly #idleMs: number

  /** Made by each initialize, so the provider can be initialized again after onClose. */
  #abort = new AbortController()
  #ev: Evaluator | null = null
  /** The snapshot came from the data plane (not the bootstrap file), so its version can be named to it. */
  #fromNetwork = false
  #init: "pending" | "done" | "failed" = "pending"
  #fatal = false
  #connected = false
  #stale = false
  #lastContact = Date.now()
  #staleTimer: ReturnType<typeof setTimeout> | undefined
  #polling: AbortController | null = null
  #running = false

  constructor(options: EzghProviderOptions) {
    this.#offline = options.offline ?? false
    this.#bootstrapPath = options.bootstrapPath
    if (this.#offline && !this.#bootstrapPath) throw new TypeError("EzghProvider: offline needs a bootstrapPath")
    const key = options.sdkKey ?? ""
    if (!this.#offline && !key.startsWith("ezgh_fs_")) {
      throw new TypeError("EzghProvider needs a server key (ezgh_fs_…); client keys (ezgh_fc_…) are for EzghWebProvider")
    }
    this.#key = key
    this.#endpoint = trimEndpoint(options.endpoint)
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis)
    this.#log = options.logger ?? defaultLogger
    this.#timeoutMs = options.timeoutMs ?? 10_000
    this.#pollMs = options.pollIntervalMs ?? 30_000
    this.#staleMs = options.staleAfterMs ?? 30_000
    this.#minBackoff = options.minBackoffMs ?? 1_000
    this.#maxBackoff = options.maxBackoffMs ?? 60_000
    this.#idleMs = options.idleTimeoutMs ?? 45_000
  }

  async initialize(): Promise<void> {
    this.#abort.abort()
    this.#abort = new AbortController()
    clearTimeout(this.#staleTimer)
    this.#ev = null
    this.#fromNetwork = false
    this.#init = "pending"
    this.#fatal = false
    this.#connected = false
    this.#stale = false
    this.#lastContact = Date.now()
    this.#polling = null
    this.#running = false
    if (this.#bootstrapPath) {
      const { readFile } = await import("node:fs/promises")
      this.#apply(await readFile(this.#bootstrapPath, "utf8"), { throwOnInvalid: true, bootstrap: true })
    }
    if (this.#offline) {
      this.#init = "done"
      return
    }
    if (!this.#ev) {
      try {
        await this.#initialFetch()
      } catch (e) {
        this.#init = "failed"
        if (!this.#fatal) this.#startSync()
        throw e
      }
    } else {
      // Ready from the bootstrap file without reaching the data plane: STALE if it stays unreachable.
      this.#scheduleStale()
    }
    this.#init = "done"
    this.#startSync()
  }

  async onClose(): Promise<void> {
    this.#abort.abort()
    clearTimeout(this.#staleTimer)
  }

  async resolveBooleanEvaluation(flagKey: string, defaultValue: boolean, context: EvaluationContext): Promise<ResolutionDetails<boolean>> {
    return this.#resolve("boolean", flagKey, defaultValue, context)
  }

  async resolveStringEvaluation(flagKey: string, defaultValue: string, context: EvaluationContext): Promise<ResolutionDetails<string>> {
    return this.#resolve("string", flagKey, defaultValue, context)
  }

  async resolveNumberEvaluation(flagKey: string, defaultValue: number, context: EvaluationContext): Promise<ResolutionDetails<number>> {
    return this.#resolve("number", flagKey, defaultValue, context)
  }

  async resolveObjectEvaluation<T extends JsonValue>(flagKey: string, defaultValue: T, context: EvaluationContext): Promise<ResolutionDetails<T>> {
    return this.#resolve("object", flagKey, defaultValue, context)
  }

  #resolve<T extends JsonValue>(type: FlagType, key: string, defaultValue: T, context: EvaluationContext): ResolutionDetails<T> {
    const ev = this.#ev
    if (!ev) return { value: defaultValue, reason: "ERROR", errorCode: ErrorCode.PROVIDER_NOT_READY }
    return ev.resolve(type, key, defaultValue, context)
  }

  // ---- snapshots

  /**
   * Swaps in a newer snapshot and announces it: PROVIDER_CONFIGURATION_CHANGED on every new
   * version, with the flags whose answers may have changed (possibly none) and the snapshot
   * version. A snapshot from the data plane always replaces a bootstrap file's, whatever their
   * versions; between data-plane snapshots only a newer version counts.
   */
  #apply(json: string, opts: { throwOnInvalid?: boolean; bootstrap?: boolean } = {}): void {
    let next: Evaluator
    try {
      next = new Evaluator(json)
    } catch (e) {
      if (opts.throwOnInvalid) throw new GeneralError(`EzghProvider: ${(e as Error).message}`)
      this.#log.warn(`ignored an invalid snapshot: ${(e as Error).message}`)
      return
    }
    const prev = this.#ev
    if (prev && this.#fromNetwork && next.version <= prev.version) return
    this.#ev = next
    this.#fromNetwork = !opts.bootstrap
    if (!prev) {
      if (this.#init === "failed") this.events.emit(ProviderEvents.Ready)
      return
    }
    const flagsChanged = changedKeys(prev.fingerprints(), next.fingerprints())
    this.events.emit(ProviderEvents.ConfigurationChanged, { flagsChanged, metadata: { snapshotVersion: next.version } })
  }

  #headers(extra: Record<string, string> = {}): Record<string, string> {
    return { Authorization: `Bearer ${this.#key}`, ...extra }
  }

  /** GET /v1/sync/snapshot until it answers, the key is refused (fatal) or the timeout passes. */
  async #initialFetch(): Promise<void> {
    const deadline = Date.now() + this.#timeoutMs
    const ctl = childController(this.#abort.signal)
    const timer = setTimeout(() => ctl.abort(), this.#timeoutMs)
    try {
      for (let attempt = 1; ; attempt++) {
        let wait: number | undefined
        try {
          const res = await this.#fetch(`${this.#endpoint}/v1/sync/snapshot`, { headers: this.#headers(), signal: ctl.signal })
          if (res.ok) {
            this.#apply(await res.text(), { throwOnInvalid: true })
            this.#contact()
            return
          }
          await res.body?.cancel()
          if (isFatalStatus(res.status)) throw this.#fatalize(`the data plane refused the SDK key (${res.status})`)
          wait = retryAfterMs(res)
        } catch (e) {
          if (e instanceof ProviderFatalError || e instanceof GeneralError) throw e
          if (ctl.signal.aborted) break
        }
        await sleep(Math.min(backoffMs(attempt, this.#minBackoff, this.#maxBackoff, wait), Math.max(0, deadline - Date.now())), ctl.signal)
        if (ctl.signal.aborted || Date.now() >= deadline) break
      }
    } finally {
      clearTimeout(timer)
      ctl.abort()
    }
    throw new GeneralError(`EzghProvider: no snapshot from ${this.#endpoint} within ${this.#timeoutMs} ms`)
  }

  // ---- the stream, polling and staleness

  #startSync(): void {
    if (this.#running || this.#abort.signal.aborted) return
    this.#running = true
    void this.#syncLoop(this.#abort.signal)
  }

  /** Follows the stream until `signal` (this run's) aborts or the key is refused. */
  async #syncLoop(signal: AbortSignal): Promise<void> {
    let failures = 0
    while (!signal.aborted && !this.#fatal) {
      let out: StreamOutcome
      try {
        out = await this.#streamOnce(signal)
      } catch (e) {
        // Anything unexpected is a transient failure, never a silent stop.
        this.#log.warn(`sync stream: ${(e as Error).message}`)
        out = { kind: "failed", established: false }
      }
      if (out.kind === "fatal" || signal.aborted) return
      if (out.kind === "end") {
        if (out.reason === "revoked" || out.reason === "environmentDeleted") {
          this.#fatalize(out.reason === "revoked" ? "the SDK key was revoked" : "the SDK key's environment was deleted")
          return
        }
        if (out.reason === "rotate") {
          failures = 0
          continue // the only reason to reconnect at once
        }
        if (out.reason === "shutdown") {
          // The pod is draining: reconnect (to another one) after a little jitter.
          failures = 0
          await sleep(Math.random() * this.#minBackoff, signal)
          continue
        }
        // An unknown reason is a failed attempt.
      } else if (out.kind === "failed" && out.established) {
        // A stream that was up and dropped: reconnect soon; it isn't a failed attempt.
        failures = 0
        await sleep(backoffMs(1, this.#minBackoff, this.#maxBackoff, out.retryAfter), signal)
        continue
      }
      failures++
      if (failures >= STREAM_FAILURES_BEFORE_POLLING) this.#startPolling()
      await sleep(backoffMs(failures, this.#minBackoff, this.#maxBackoff, out.kind === "failed" ? out.retryAfter : undefined), signal)
    }
  }

  /** One stream connection, from the request until it ends. */
  async #streamOnce(signal: AbortSignal): Promise<StreamOutcome> {
    const ctl = childController(signal)
    try {
      return await this.#streamWith(ctl)
    } finally {
      ctl.abort()
    }
  }

  async #streamWith(ctl: AbortController): Promise<StreamOutcome> {
    let res: Response
    try {
      const body = this.#ev && this.#fromNetwork ? JSON.stringify({ version: this.#ev.version }) : "{}"
      res = await this.#fetch(`${this.#endpoint}/v1/sync/stream`, {
        method: "POST",
        headers: this.#headers({ "Content-Type": "application/json", Accept: "text/event-stream" }),
        body,
        signal: ctl.signal,
      })
    } catch {
      return { kind: "failed", established: false }
    }
    if (!res.ok || !res.body || !isEventStream(res)) {
      await res.body?.cancel()
      if (isFatalStatus(res.status)) {
        this.#fatalize(`the data plane refused the SDK key (${res.status})`)
        return { kind: "fatal" }
      }
      const retryAfter = retryAfterMs(res)
      return retryAfter === undefined ? { kind: "failed", established: false } : { kind: "failed", established: false, retryAfter }
    }

    // Connected only once the stream sends its first byte (the opening comment).
    let established = false
    const onActivity = () => {
      if (established) {
        this.#lastContact = Date.now()
        return
      }
      established = true
      this.#connected = true
      this.#stopPolling()
      this.#contact()
    }
    try {
      for await (const ev of watchedSSE(res.body, ctl, this.#idleMs, onActivity)) {
        if (ev.event === "snapshot") this.#apply(ev.data)
        else if (ev.event === "end") return { kind: "end", reason: endReason(ev.data) }
      }
      return { kind: "failed", established }
    } catch {
      return { kind: "failed", established }
    } finally {
      if (established) {
        this.#connected = false
        if (!this.#fatal) this.#scheduleStale()
      }
    }
  }

  #startPolling(): void {
    if (this.#polling || this.#abort.signal.aborted) return
    const ctl = childController(this.#abort.signal)
    this.#polling = ctl
    void (async () => {
      while (!ctl.signal.aborted && !this.#fatal) {
        const wait = await this.#pollOnce(ctl.signal).catch(() => undefined)
        await sleep(Math.max(this.#pollMs, Math.min(wait ?? 0, this.#maxBackoff)), ctl.signal)
      }
    })()
  }

  #stopPolling(): void {
    this.#polling?.abort()
    this.#polling = null
  }

  /** One GET /v1/sync/snapshot with If-None-Match; answers a Retry-After to honour, if any. */
  async #pollOnce(signal: AbortSignal): Promise<number | undefined> {
    try {
      const headers = this.#ev && this.#fromNetwork ? this.#headers({ "If-None-Match": `"${this.#ev.version}"` }) : this.#headers()
      const res = await this.#fetch(`${this.#endpoint}/v1/sync/snapshot`, { headers, signal })
      if (res.status === 200) {
        this.#apply(await res.text())
        this.#contact()
        return undefined
      }
      await res.body?.cancel()
      if (res.status === 304) {
        this.#contact()
        return undefined
      }
      if (isFatalStatus(res.status)) this.#fatalize(`the data plane refused the SDK key (${res.status})`)
      return retryAfterMs(res)
    } catch {
      return undefined
    }
  }

  /** The data plane answered: not stale (any more). */
  #contact(): void {
    this.#lastContact = Date.now()
    if (this.#stale) {
      this.#stale = false
      if (!this.#fatal) this.events.emit(ProviderEvents.Ready)
    }
    if (this.#connected) clearTimeout(this.#staleTimer)
    else this.#scheduleStale()
  }

  /** While disconnected: stale once the last contact is older than the threshold. */
  #scheduleStale(): void {
    clearTimeout(this.#staleTimer)
    if (this.#stale || this.#connected || this.#abort.signal.aborted) return
    // While polling, a poll is due every pollIntervalMs; allow two before calling the data stale.
    const threshold = this.#polling ? Math.max(this.#staleMs, 2 * this.#pollMs) : this.#staleMs
    const due = this.#lastContact + threshold - Date.now()
    this.#staleTimer = setTimeout(() => {
      if (this.#connected || this.#fatal || this.#abort.signal.aborted) return
      if (Date.now() - this.#lastContact < threshold) return this.#scheduleStale()
      this.#stale = true
      this.events.emit(ProviderEvents.Stale, { message: `no contact with ${this.#endpoint} for ${threshold} ms` })
    }, Math.max(0, due))
  }

  /** The key was refused: stop everything. Answers the error for initialize to throw. */
  #fatalize(message: string): ProviderFatalError {
    const err = new ProviderFatalError(`EzghProvider: ${message}`)
    if (this.#fatal) return err
    this.#fatal = true
    clearTimeout(this.#staleTimer)
    this.#stopPolling()
    this.#abort.abort()
    this.#log.error(err.message)
    if (this.#init !== "pending") this.events.emit(ProviderEvents.Error, { errorCode: ErrorCode.PROVIDER_FATAL, message: err.message })
    return err
  }
}
