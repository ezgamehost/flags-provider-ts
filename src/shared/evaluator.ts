// The shared evaluator: flagd's TypeScript core (@openfeature/flagd-core, pinned) over one EZGH
// snapshot, with EZGH's one reason rule (plan PD7): a flag whose metadata says `enabled: false`
// is an off config, which the core answers with the off value and reason STATIC; EZGH reports
// DISABLED. Nothing else rewrites what the core answers.
import { FlagdCore } from "@openfeature/flagd-core"
import type { EvaluationContext, FlagMetadata, JsonValue, Logger, ResolutionDetails } from "@openfeature/core"

export type FlagType = "boolean" | "string" | "number" | "object"

/** A no-op logger: the core's default logs targeting errors to the console. */
const quiet: Logger = { error() {}, warn() {}, info() {}, debug() {} }

export class Evaluator {
  /** The snapshot's `metadata.snapshotVersion`. */
  readonly version: number
  readonly #core: FlagdCore
  readonly #fingerprints: Map<string, string>

  /**
   * Loads a snapshot (flagd JSON as flags-data serves it). Throws on an invalid snapshot. The
   * core runs without dynamic code generation (it works in every runtime, edge ones included);
   * `dynamicCodeGeneration: true` uses the core's compiled mode, which the conformance suite also
   * runs.
   */
  constructor(snapshotJson: string, logger: Logger = quiet, options: { dynamicCodeGeneration?: boolean } = {}) {
    let parsed: unknown
    try {
      parsed = JSON.parse(snapshotJson)
    } catch (e) {
      throw new Error(`invalid snapshot: ${(e as Error).message}`)
    }
    if (!isObject(parsed) || !isObject(parsed.flags)) throw new Error("invalid snapshot: no flags object")
    const meta = isObject(parsed.metadata) ? parsed.metadata : {}
    const version = meta.snapshotVersion
    if (typeof version !== "number" || !Number.isSafeInteger(version) || version < 0) {
      throw new Error("invalid snapshot: metadata.snapshotVersion isn't a non-negative integer")
    }
    this.version = version
    this.#core = new FlagdCore(undefined, logger, { disableDynamicCodeGeneration: !options.dynamicCodeGeneration })
    this.#core.setConfigurations(snapshotJson)
    this.#fingerprints = fingerprints(parsed.flags, isObject(parsed.$evaluators) ? parsed.$evaluators : {})
  }

  /**
   * A fingerprint of each flag's definition (variants, default, targeting with the segments it
   * references resolved, version, enabled), by key. Two snapshots' fingerprints differ for a flag
   * exactly when its answers may differ, so a segment edit, which changes no flagVersion, still
   * shows up in `changedKeys`.
   */
  fingerprints(): Map<string, string> {
    return this.#fingerprints
  }

  /** Each flag's `metadata.flagVersion` (its config version), by key. */
  flagVersions(): Map<string, number> {
    const out = new Map<string, number>()
    for (const [key, flag] of this.#core.getFlags()) {
      const v = flag.metadata.flagVersion
      out.set(key, typeof v === "number" ? v : 0)
    }
    return out
  }

  /** The snapshot's flag keys, sorted. */
  keys(): string[] {
    return [...this.#core.getFlags().keys()].sort()
  }

  resolve<T extends JsonValue>(type: FlagType, key: string, defaultValue: T, ctx: EvaluationContext): ResolutionDetails<T> {
    const r = this.#core.resolve<T>(type, key, defaultValue, ctx)
    if (r.errorCode !== undefined) {
      const out: ResolutionDetails<T> = { value: defaultValue, reason: r.reason ?? "ERROR", errorCode: r.errorCode }
      if (r.errorMessage !== undefined) out.errorMessage = r.errorMessage
      return out
    }
    const flagMetadata: FlagMetadata = {}
    if (typeof r.flagMetadata.flagVersion === "number") flagMetadata.flagVersion = r.flagMetadata.flagVersion
    const out: ResolutionDetails<T> = { value: r.value, reason: r.reason, flagMetadata }
    if (r.variant !== undefined) out.variant = r.variant
    if (r.flagMetadata.enabled === false) out.reason = "DISABLED"
    return out
  }
}

/**
 * The keys whose value changed, appeared or disappeared between two maps (`flagVersions()` or
 * `fingerprints()`), sorted.
 */
export function changedKeys<V>(before: Map<string, V>, after: Map<string, V>): string[] {
  const out: string[] = []
  for (const [k, v] of after) if (before.get(k) !== v) out.push(k)
  for (const k of before.keys()) if (!after.has(k)) out.push(k)
  return out.sort()
}

function fingerprints(flags: Record<string, unknown>, evaluators: Record<string, unknown>): Map<string, string> {
  const refs = new Map(Object.entries(evaluators).map(([k, v]) => [JSON.stringify({ $ref: k }), JSON.stringify(v)]))
  const out = new Map<string, string>()
  for (const [key, def] of Object.entries(flags)) {
    if (!isObject(def)) continue
    const meta = isObject(def.metadata) ? def.metadata : {}
    let fp = JSON.stringify([def.state, def.variants, def.defaultVariant, def.targeting ?? null, meta.flagVersion ?? null, meta.enabled ?? null])
    // flagd substitutes {"$ref": "<segment>"} textually; so does the fingerprint.
    for (const [ref, body] of refs) fp = fp.replaceAll(ref, body)
    out.set(key, fp)
  }
  return out
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v)
}
