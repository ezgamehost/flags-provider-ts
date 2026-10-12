// EzghWebProvider: the OpenFeature web provider. Following OpenFeature's static-context paradigm,
// it runs one bulk OFREP evaluation (POST /ofrep/v1/evaluate/flags) at initialize and on every
// context change, then answers evaluations synchronously from those results. It refreshes them
// when the page becomes visible again and every 5 minutes (If-None-Match), and with `live: true`
// it follows POST /v1/client/stream and refetches on `changed`. A client key only ever sees
// `clientSide` flags, and rules never reach the browser. The only state it keeps is the current
// results, in memory: nothing goes to localStorage or sessionStorage.
import {
  ErrorCode,
  GeneralError,
  InvalidContextError,
  OpenFeatureEventEmitter,
  ProviderEvents,
  ProviderFatalError,
  type EvaluationContext,
  type FlagMetadata,
  type JsonValue,
  type Logger,
  type Provider,
  type ResolutionDetails,
} from "@openfeature/web-sdk"
import type { FlagType } from "./shared/evaluator.ts"
import { backoffMs, childController, isFatalStatus, retryAfterMs, sleep, trimEndpoint } from "./shared/http.ts"
import { endReason, isEventStream, watchedSSE } from "./shared/sse.ts"

export { DEFAULT_ENDPOINT } from "./shared/http.ts"

export interface EzghWebProviderOptions {
  /** A client SDK key (`ezgh_fc_…`). It's publishable: it sees only `clientSide` flags. */
  clientKey: string
  /** The data plane; defaults to https://flags.ezghcloud.com. */
  endpoint?: string
  /** Follow the client change stream and refetch on every change (default false: one connection per open tab). */
  live?: boolean
  /** The periodic refresh interval (default 5 minutes). */
  refreshMs?: number
  /** The fetch to use; defaults to the global one. */
  fetch?: typeof fetch
  /** Logs connection problems; defaults to console warnings and errors. */
  logger?: Logger
  /** How long `initialize` retries the first evaluation (default 10 s). Retrying continues after. */
  timeoutMs?: number
  /** Retry backoff bounds (default 1 s to 60 s, with jitter). */
  minBackoffMs?: number
  maxBackoffMs?: number
  /** A live stream silent this long (heartbeats come every 15 s) is reconnected (default 45 s). */
  idleTimeoutMs?: number
  /**
   * After a `changed` whose refetch answers 304 (the pod answering may not have the new version
   * yet), refetch once more after this long (default 1 s).
   */
  changedRetryMs?: number
}

/** One flag of a bulk OFREP answer. */
interface Evaluated {
  key: string
  value?: unknown
  variant?: string
  reason?: string
  metadata?: Record<string, unknown>
  errorCode?: string
  errorDetails?: string
}

const defaultLogger: Logger = {
  error: (...a: unknown[]) => console.error("[ezgh-flags]", ...a),
  warn: (...a: unknown[]) => console.warn("[ezgh-flags]", ...a),
  info() {},
  debug() {},
}

class Retryable extends Error {
  constructor(
    message: string,
    readonly retryAfter?: number,
  ) {
    super(message)
  }
}

/**
 * Errors for a 400: the context itself is refused, so retrying it can't help (final until it
 * changes). A set, not a subclass: OpenFeature's error classes reset their own prototypes, so a
 * subclass wouldn't survive `instanceof`.
 */
const finalErrors = new WeakSet<Error>()

function final<E extends Error>(e: E): E {
  finalErrors.add(e)
  return e
}

function isFinal(e: unknown): boolean {
  return e instanceof Error && finalErrors.has(e)
}

type BulkOutcome = "ok" | "not-modified" | "dropped"

const errorCodes = new Set<string>(Object.values(ErrorCode))

export class EzghWebProvider implements Provider {
  readonly metadata = { name: "ezgh-flags" } as const
  readonly runsOn = "client" as const
  readonly events = new OpenFeatureEventEmitter()

  readonly #key: string
  readonly #endpoint: string
  readonly #live: boolean
  readonly #refreshMs: number
  readonly #fetch: typeof fetch
  readonly #log: Logger
  readonly #timeoutMs: number
  readonly #minBackoff: number
  readonly #maxBackoff: number
  readonly #idleMs: number
  readonly #changedRetryMs: number

  /** Made by each initialize, so the provider can be initialized again after onClose. */
  #abort = new AbortController()
  #results: Map<string, Evaluated> | null = null
  #etag: string | null = null
  #context: EvaluationContext = {}
  /** Bumped on every context change (and initialize): an answer for an older context is dropped. */
  #gen = 0
  #fatal = false
  #init: "pending" | "done" | "failed" = "pending"
  #recovering = false
  #inContextChange = false
  /** The highest client artifact version a `changed` event has named. */
  #lastChanged = 0
  #refreshing: Promise<BulkOutcome | "skipped"> | null = null
  #refreshAgain = false
  #timer: ReturnType<typeof setInterval> | undefined
  #onVisibility = () => {
    if (visibleDocument()) void this.#refresh()
  }

  constructor(options: EzghWebProviderOptions) {
    if (!options.clientKey?.startsWith("ezgh_fc_")) {
      throw new TypeError("EzghWebProvider needs a client key (ezgh_fc_…); server keys (ezgh_fs_…) are secret and belong in EzghProvider")
    }
    this.#key = options.clientKey
    this.#endpoint = trimEndpoint(options.endpoint)
    this.#live = options.live ?? false
    this.#refreshMs = options.refreshMs ?? 5 * 60_000
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis)
    this.#log = options.logger ?? defaultLogger
    this.#timeoutMs = options.timeoutMs ?? 10_000
    this.#minBackoff = options.minBackoffMs ?? 1_000
    this.#maxBackoff = options.maxBackoffMs ?? 60_000
    this.#idleMs = options.idleTimeoutMs ?? 45_000
    this.#changedRetryMs = options.changedRetryMs ?? 1_000
  }

  async initialize(context?: EvaluationContext): Promise<void> {
    this.#stop()
    this.#abort = new AbortController()
    this.#results = null
    this.#etag = null
    this.#gen++
    this.#fatal = false
    this.#init = "pending"
    this.#recovering = false
    this.#lastChanged = 0
    this.#refreshing = null
    this.#refreshAgain = false
    this.#context = context ?? {}
    try {
      await this.#evaluateWithin(this.#timeoutMs)
      this.#init = "done"
    } catch (e) {
      this.#init = "failed"
      if (!this.#fatal && !isFinal(e)) this.#recover()
      throw e
    } finally {
      if (!this.#fatal) this.#startBackground()
    }
  }

  async onContextChange(_old: EvaluationContext, next: EvaluationContext): Promise<void> {
    this.#context = next
    this.#gen++
    this.#etag = null
    this.#inContextChange = true
    try {
      // The SDK announces PROVIDER_CONTEXT_CHANGED itself: no CONFIGURATION_CHANGED for this.
      await this.#bulk({ conditional: false, announce: false })
    } catch (e) {
      // The old results describe someone else: answer defaults until this context's arrive.
      this.#results = null
      if (e instanceof ProviderFatalError) {
        // The SDK marks a failed context change ERROR; FATAL has to follow it.
        setTimeout(() => this.events.emit(ProviderEvents.Error, { errorCode: ErrorCode.PROVIDER_FATAL, message: e.message }), 0)
      } else if (!isFinal(e)) {
        this.#recover()
      }
      throw e
    } finally {
      this.#inContextChange = false
    }
  }

  async onClose(): Promise<void> {
    this.#stop()
  }

  #stop(): void {
    this.#abort.abort()
    clearInterval(this.#timer)
    this.#timer = undefined
    const doc = (globalThis as { document?: EventTarget }).document
    doc?.removeEventListener?.("visibilitychange", this.#onVisibility)
  }

  resolveBooleanEvaluation(flagKey: string, defaultValue: boolean): ResolutionDetails<boolean> {
    return this.#resolve("boolean", flagKey, defaultValue)
  }

  resolveStringEvaluation(flagKey: string, defaultValue: string): ResolutionDetails<string> {
    return this.#resolve("string", flagKey, defaultValue)
  }

  resolveNumberEvaluation(flagKey: string, defaultValue: number): ResolutionDetails<number> {
    return this.#resolve("number", flagKey, defaultValue)
  }

  resolveObjectEvaluation<T extends JsonValue>(flagKey: string, defaultValue: T): ResolutionDetails<T> {
    return this.#resolve("object", flagKey, defaultValue)
  }

  #resolve<T extends JsonValue>(type: FlagType, key: string, defaultValue: T): ResolutionDetails<T> {
    if (!this.#results) return { value: defaultValue, reason: "ERROR", errorCode: ErrorCode.PROVIDER_NOT_READY }
    const r = this.#results.get(key)
    if (!r) return { value: defaultValue, reason: "ERROR", errorCode: ErrorCode.FLAG_NOT_FOUND, errorMessage: `flag '${key}' not found` }
    if (r.errorCode !== undefined) {
      const out: ResolutionDetails<T> = { value: defaultValue, reason: "ERROR", errorCode: errorCodes.has(r.errorCode) ? (r.errorCode as ErrorCode) : ErrorCode.GENERAL }
      if (r.errorDetails !== undefined) out.errorMessage = r.errorDetails
      return out
    }
    const v = r.value
    const ok = type === "object" ? typeof v === "object" && v !== null : typeof v === type
    if (!ok) {
      return { value: defaultValue, reason: "ERROR", errorCode: ErrorCode.TYPE_MISMATCH, errorMessage: `flag '${key}' is not a ${type}` }
    }
    const out: ResolutionDetails<T> = { value: v as T, flagMetadata: flagMetadata(r.metadata) }
    if (r.variant !== undefined) out.variant = r.variant
    if (r.reason !== undefined) out.reason = r.reason
    return out
  }

  // ---- bulk evaluation

  /**
   * One bulk evaluation of the current context. With `conditional`, sends If-None-Match and keeps
   * the results on 304. With `announce`, emits CONFIGURATION_CHANGED for the flags that changed
   * when it replaces results it already had. Throws ProviderFatalError for a refused key, a final
   * error for a refused context (400), and Retryable otherwise.
   */
  async #bulk(opts: { conditional: boolean; announce: boolean; signal?: AbortSignal }): Promise<BulkOutcome> {
    const gen = this.#gen
    const abort = this.#abort
    const headers: Record<string, string> = { Authorization: `Bearer ${this.#key}`, "Content-Type": "application/json" }
    if (opts.conditional && this.#etag) headers["If-None-Match"] = this.#etag
    let res: Response
    try {
      res = await this.#fetch(`${this.#endpoint}/ofrep/v1/evaluate/flags`, {
        method: "POST",
        headers,
        body: JSON.stringify({ context: this.#context }),
        signal: opts.signal ?? abort.signal,
      })
    } catch (e) {
      throw new Retryable(`bulk evaluation failed: ${(e as Error).message}`)
    }
    if (res.status === 304) {
      await res.body?.cancel()
      return "not-modified"
    }
    if (res.status === 400) {
      const body = (await res.json().catch(() => ({}))) as { errorCode?: unknown; errorDetails?: unknown }
      const detail = typeof body.errorDetails === "string" ? `: ${body.errorDetails}` : ""
      if (body.errorCode === "INVALID_CONTEXT") throw final(new InvalidContextError(`EzghWebProvider: the data plane refused the context${detail}`))
      throw final(new GeneralError(`EzghWebProvider: bulk evaluation answered 400 (${String(body.errorCode ?? "bad request")})${detail}`))
    }
    if (!res.ok) {
      await res.body?.cancel()
      if (isFatalStatus(res.status)) throw this.#fatalize(`the data plane refused the client key (${res.status})`)
      throw new Retryable(`bulk evaluation answered ${res.status}`, retryAfterMs(res))
    }
    let flags: Evaluated[]
    try {
      const body = (await res.json()) as { flags?: unknown }
      if (!Array.isArray(body.flags)) throw new Error("no flags array")
      flags = body.flags.filter((f): f is Evaluated => typeof f === "object" && f !== null && typeof (f as Evaluated).key === "string")
    } catch (e) {
      throw new Retryable(`bulk evaluation answered an invalid body: ${(e as Error).message}`)
    }
    if (gen !== this.#gen || abort.signal.aborted) return "dropped" // answered for an older context, or closed
    const next = new Map(flags.map((f) => [f.key, f]))
    const prev = this.#results
    this.#results = next
    this.#etag = res.headers.get("etag")
    if (prev && opts.announce) {
      const flagsChanged = changed(prev, next)
      if (flagsChanged.length > 0) this.events.emit(ProviderEvents.ConfigurationChanged, { flagsChanged })
    }
    return "ok"
  }

  /** #bulk, retrying retryable failures with backoff until it answers or `ms` pass. */
  async #evaluateWithin(ms: number): Promise<void> {
    const deadline = Date.now() + ms
    const ctl = childController(this.#abort.signal)
    const timer = setTimeout(() => ctl.abort(), ms)
    try {
      for (let attempt = 1; ; attempt++) {
        try {
          await this.#bulk({ conditional: false, announce: false, signal: ctl.signal })
          return
        } catch (e) {
          if (!(e instanceof Retryable)) throw e
          if (ctl.signal.aborted || Date.now() >= deadline) throw new GeneralError(`EzghWebProvider: ${e.message}`)
          await sleep(Math.min(backoffMs(attempt, this.#minBackoff, this.#maxBackoff, e.retryAfter), Math.max(0, deadline - Date.now())), ctl.signal)
        }
      }
    } finally {
      clearTimeout(timer)
      ctl.abort()
    }
  }

  /** After a failed initialize or context change: retry in the background, then announce READY. */
  #recover(): void {
    if (this.#recovering) return
    this.#recovering = true
    const signal = this.#abort.signal
    void (async () => {
      try {
        for (let attempt = 1; !signal.aborted && !this.#fatal; attempt++) {
          const gen = this.#gen
          try {
            await this.#bulk({ conditional: false, announce: false })
            if (gen === this.#gen && this.#results) {
              this.events.emit(ProviderEvents.Ready)
              return
            }
            attempt = 0
          } catch (e) {
            if (!(e instanceof Retryable)) return
            await sleep(backoffMs(attempt, this.#minBackoff, this.#maxBackoff, e.retryAfter), signal)
          }
        }
      } finally {
        this.#recovering = false
      }
    })()
  }

  /**
   * A conditional refresh; concurrent calls coalesce into one more after the current one. Answers
   * the last attempt's outcome ("skipped" when there's nothing to refresh, or it failed).
   */
  #refresh(): Promise<BulkOutcome | "skipped"> {
    if (this.#refreshing) {
      this.#refreshAgain = true
      return this.#refreshing
    }
    const abort = this.#abort
    this.#refreshing = (async () => {
      let outcome: BulkOutcome | "skipped" = "skipped"
      try {
        do {
          this.#refreshAgain = false
          if (abort.signal.aborted || this.#fatal || !this.#results) return "skipped"
          try {
            outcome = await this.#bulk({ conditional: true, announce: true })
          } catch (e) {
            if (e instanceof Retryable || isFinal(e)) this.#log.warn((e as Error).message)
            return "skipped"
          }
        } while (this.#refreshAgain)
        return outcome
      } finally {
        if (this.#abort === abort) this.#refreshing = null
      }
    })()
    return this.#refreshing
  }

  /** A `changed` event: refetch; if that's a 304, the answering pod may lag, so once more later. */
  async #onChanged(signal: AbortSignal): Promise<void> {
    if ((await this.#refresh()) !== "not-modified") return
    await sleep(this.#changedRetryMs, signal)
    if (!signal.aborted) await this.#refresh()
  }

  // ---- refresh triggers and the live stream

  #startBackground(): void {
    if (this.#timer !== undefined || this.#abort.signal.aborted) return
    this.#timer = setInterval(() => void this.#refresh(), this.#refreshMs)
    const doc = (globalThis as { document?: EventTarget }).document
    doc?.addEventListener?.("visibilitychange", this.#onVisibility)
    if (this.#live) void this.#streamLoop(this.#abort.signal)
  }

  async #streamLoop(signal: AbortSignal): Promise<void> {
    let failures = 0
    while (!signal.aborted && !this.#fatal) {
      let out: { established: boolean; ended?: string; retryAfter?: number }
      try {
        out = await this.#streamOnce(signal)
      } catch (e) {
        // Anything unexpected is a transient failure, never a silent stop.
        this.#log.warn(`client stream: ${(e as Error).message}`)
        out = { established: false }
      }
      if (signal.aborted || this.#fatal) return
      if (out.ended === "revoked" || out.ended === "environmentDeleted") {
        this.#fatalize(out.ended === "revoked" ? "the client key was revoked" : "the client key's environment was deleted")
        return
      }
      if (out.ended === "rotate") {
        failures = 0
        continue // the only reason to reconnect at once
      }
      if (out.ended === "shutdown") {
        // The pod is draining: reconnect (to another one) after a little jitter.
        failures = 0
        await sleep(Math.random() * this.#minBackoff, signal)
        continue
      }
      if (out.ended === undefined && out.established) {
        // Up, then dropped: reconnect soon; it isn't a failed attempt.
        failures = 0
        await sleep(backoffMs(1, this.#minBackoff, this.#maxBackoff), signal)
        continue
      }
      // A failed attempt, or an end with an unknown reason.
      failures++
      await sleep(backoffMs(failures, this.#minBackoff, this.#maxBackoff, out.retryAfter), signal)
    }
  }

  /** One client stream connection, from the request until it ends. */
  async #streamOnce(signal: AbortSignal): Promise<{ established: boolean; ended?: string; retryAfter?: number }> {
    const ctl = childController(signal)
    try {
      let res: Response
      try {
        res = await this.#fetch(`${this.#endpoint}/v1/client/stream`, {
          method: "POST",
          headers: { Authorization: `Bearer ${this.#key}`, "Content-Type": "application/json", Accept: "text/event-stream" },
          body: "{}",
          signal: ctl.signal,
        })
      } catch {
        return { established: false }
      }
      if (!res.ok || !res.body || !isEventStream(res)) {
        await res.body?.cancel()
        if (isFatalStatus(res.status)) this.#fatalize(`the data plane refused the client key (${res.status})`)
        const retryAfter = retryAfterMs(res)
        return retryAfter === undefined ? { established: false } : { established: false, retryAfter }
      }
      // Connected only once the stream sends its first byte (the opening comment). Every
      // connection, the first included, refreshes (conditionally): a change may have happened
      // while there was no stream, or between the first evaluation and this connection.
      let established = false
      const onActivity = () => {
        if (established) return
        established = true
        void this.#refresh()
      }
      try {
        for await (const ev of watchedSSE(res.body, ctl, this.#idleMs, onActivity)) {
          if (ev.event === "changed") {
            const v = changedVersion(ev.data)
            if (v !== undefined && v < this.#lastChanged) continue // an older version, from a lagging pod
            if (v !== undefined) this.#lastChanged = v
            void this.#onChanged(signal)
          } else if (ev.event === "end") return { established, ended: endReason(ev.data) }
        }
      } catch {
        // dropped: the caller reconnects
      }
      return { established }
    } finally {
      ctl.abort()
    }
  }

  /** The key was refused: stop everything. Answers the error to throw. */
  #fatalize(message: string): ProviderFatalError {
    const err = new ProviderFatalError(`EzghWebProvider: ${message}`)
    if (this.#fatal) return err
    this.#fatal = true
    this.#log.error(err.message)
    void this.onClose()
    if (this.#init !== "pending" && !this.#inContextChange) this.events.emit(ProviderEvents.Error, { errorCode: ErrorCode.PROVIDER_FATAL, message: err.message })
    return err
  }
}

function changedVersion(data: string): number | undefined {
  try {
    const v = (JSON.parse(data) as { version?: unknown }).version
    return typeof v === "number" ? v : undefined
  } catch {
    return undefined
  }
}

function visibleDocument(): boolean {
  const doc = (globalThis as { document?: { visibilityState?: string } }).document
  return doc?.visibilityState === "visible"
}

function flagMetadata(m: Record<string, unknown> | undefined): FlagMetadata {
  const out: Record<string, string | number | boolean> = {}
  for (const [k, v] of Object.entries(m ?? {})) if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") out[k] = v
  return out
}

/** Keys whose value, variant or error changed, appeared or disappeared, sorted. */
function changed(before: Map<string, Evaluated>, after: Map<string, Evaluated>): string[] {
  const sig = (e: Evaluated) => JSON.stringify([e.value, e.variant, e.errorCode])
  const out: string[] = []
  for (const [k, v] of after) {
    const b = before.get(k)
    if (!b || sig(b) !== sig(v)) out.push(k)
  }
  for (const k of before.keys()) if (!after.has(k)) out.push(k)
  return out.sort()
}
