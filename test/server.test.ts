import { afterEach, beforeEach, expect, test } from "bun:test"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { ErrorCode, OpenFeature, ProviderEvents, ProviderStatus, type EventDetails } from "@openfeature/server-sdk"
import { EzghProvider, type EzghProviderOptions } from "../src/server.ts"
import { bool, FakeDataPlane, serverKey, snapshot } from "./fake-data-plane.ts"

let fake: FakeDataPlane
let domain = 0
const providers: EzghProvider[] = []

beforeEach(() => {
  fake = new FakeDataPlane()
  fake.setSnapshot(snapshot(1, { a: bool(true, 1), b: bool(false, 1), n: { variants: { one: 1, two: 2 }, defaultVariant: "two", flagVersion: 1 } }))
})

afterEach(async () => {
  await Promise.all(providers.splice(0).map((p) => p.onClose()))
  fake.stop()
})

function provider(opts: Partial<EzghProviderOptions> = {}): EzghProvider {
  const p = new EzghProvider({ sdkKey: serverKey(), endpoint: fake.url, minBackoffMs: 10, maxBackoffMs: 40, ...opts })
  providers.push(p)
  return p
}

async function ready(p: EzghProvider) {
  const d = `t${++domain}`
  await OpenFeature.setProviderAndWait(d, p)
  return OpenFeature.getClient(d)
}

function nextEvent(p: EzghProvider, type: ProviderEvents, timeoutMs = 3000): Promise<EventDetails | undefined> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`no ${type} within ${timeoutMs} ms`)), timeoutMs)
    const h = (d?: EventDetails) => {
      clearTimeout(t)
      p.events.removeHandler(type, h)
      resolve(d)
    }
    p.events.addHandler(type, h)
  })
}

const streamOpens = () => fake.of("/v1/sync/stream", "POST")

test("ready after initialize, evaluating locally", async () => {
  const p = provider()
  const client = await ready(p)
  const gets = fake.of("/v1/sync/snapshot")
  expect(gets.length).toBe(1)
  expect(gets[0]!.headers.authorization).toBe(`Bearer ${serverKey()}`)
  await fake.waitFor(() => fake.openStreams === 1, 3000, "the stream")
  expect(JSON.parse(streamOpens()[0]!.body)).toEqual({ version: 1 })
  expect(streamOpens()[0]!.headers.authorization).toBe(`Bearer ${serverKey()}`)
  const before = fake.requests.length
  for (let i = 0; i < 50; i++) expect(await client.getBooleanValue("a", false, { targetingKey: `u${i}` })).toBe(true)
  expect(await client.getNumberValue("n", 0)).toBe(2)
  const d = await client.getBooleanDetails("a", false)
  expect(d.reason).toBe("STATIC")
  expect(d.variant).toBe("true")
  expect(d.flagMetadata.flagVersion).toBe(1)
  expect(fake.requests.length).toBe(before)
  expect(p.metadata.name).toBe("ezgh-flags")
  expect(p.runsOn).toBe("server")
})

test("an off config reports DISABLED with the off value", async () => {
  fake.setSnapshot(snapshot(1, { off: bool(true, 3, { enabled: false }) }))
  const client = await ready(provider())
  const d = await client.getBooleanDetails("off", false)
  expect(d.value).toBe(true)
  expect(d.reason).toBe("DISABLED")
  expect(d.flagMetadata.flagVersion).toBe(3)
})

test("an unknown flag is FLAG_NOT_FOUND with the default", async () => {
  const client = await ready(provider())
  const d = await client.getBooleanDetails("nope", true)
  expect(d.value).toBe(true)
  expect(d.errorCode).toBe(ErrorCode.FLAG_NOT_FOUND)
})

test("configuration changed: the keys whose flagVersion changed", async () => {
  const p = provider()
  const client = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  fake.setSnapshot(snapshot(2, { a: bool(true, 1), b: bool(true, 2), n: { variants: { one: 1, two: 2 }, defaultVariant: "two", flagVersion: 1 } }))
  expect((await changed)?.flagsChanged).toEqual(["b"])
  expect(await client.getBooleanValue("b", false)).toBe(true)
})

test("configuration changed on every new version, carrying snapshotVersion, flagsChanged possibly empty", async () => {
  const p = provider()
  await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  fake.setSnapshot(snapshot(2, { a: bool(true, 1), b: bool(false, 1), n: { variants: { one: 1, two: 2 }, defaultVariant: "two", flagVersion: 1 } }))
  const d = await changed
  expect(d?.flagsChanged).toEqual([])
  expect(d?.metadata?.snapshotVersion).toBe(2)
  // The same version again (a duplicate delivery) announces nothing.
  let again = false
  p.events.addHandler(ProviderEvents.ConfigurationChanged, () => (again = true))
  fake.setSnapshot(snapshot(2, { a: bool(false, 9) }))
  await Bun.sleep(50)
  expect(again).toBe(false)
})

test("configuration changed: added and removed flags, and a segment edit", async () => {
  const seg = (keys: string[]) => ({ "beta-testers": { in: [{ var: "targetingKey" }, keys] } })
  const targeted = bool(false, 1, { targeting: { if: [{ $ref: "beta-testers" }, "true"] } })
  fake.setSnapshot(snapshot(1, { a: bool(true, 1), t: targeted }, seg(["u1"])))
  const p = provider()
  const client = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  let changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  fake.setSnapshot(snapshot(2, { c: bool(true, 1), t: targeted }, seg(["u1"])))
  expect((await changed)?.flagsChanged).toEqual(["a", "c"])
  expect(await client.getBooleanValue("t", false, { targetingKey: "u2" })).toBe(false)
  changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  fake.setSnapshot(snapshot(3, { c: bool(true, 1), t: targeted }, seg(["u1", "u2"])))
  expect((await changed)?.flagsChanged).toEqual(["t"])
  expect(await client.getBooleanValue("t", false, { targetingKey: "u2" })).toBe(true)
})

test("an invalid snapshot on the stream keeps the current one", async () => {
  const p = provider()
  const client = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  fake.setSnapshot('{"flags": 7, "metadata": {"snapshotVersion": 2}}')
  await Bun.sleep(50)
  expect(await client.getBooleanValue("a", false)).toBe(true)
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  fake.setSnapshot(snapshot(3, { a: bool(false, 2) }))
  expect((await changed)?.flagsChanged).toEqual(["a", "b", "n"])
  expect(await client.getBooleanValue("a", true)).toBe(false)
})

test("reconnects after end rotate, naming the version it has", async () => {
  await ready(provider())
  await fake.waitFor(() => fake.openStreams === 1)
  fake.endStreams("rotate")
  await fake.waitFor(() => streamOpens().length === 2 && fake.openStreams === 1, 3000, "a second stream")
  expect(JSON.parse(streamOpens()[1]!.body)).toEqual({ version: 1 })
})

test("stale when down, ready again when it recovers", async () => {
  const p = provider({ staleAfterMs: 100 })
  const client = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  const stale = nextEvent(p, ProviderEvents.Stale)
  fake.force = { status: 503 }
  fake.dropStreams()
  await stale
  expect(client.providerStatus).toBe(ProviderStatus.STALE)
  expect((await client.getBooleanDetails("a", false)).value).toBe(true)
  const back = nextEvent(p, ProviderEvents.Ready)
  fake.force = null
  await back
  expect(client.providerStatus).toBe(ProviderStatus.READY)
  await fake.waitFor(() => fake.openStreams === 1)
})

test("falls back to polling with If-None-Match after three stream failures", async () => {
  const p = provider({ pollIntervalMs: 50, minBackoffMs: 200, maxBackoffMs: 400 })
  const client = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  fake.force = { status: 503, paths: ["/v1/sync/stream"] }
  fake.dropStreams()
  await fake.waitFor(() => fake.of("/v1/sync/snapshot").length >= 3, 5000, "polls")
  const failuresBeforePolling = streamOpens().filter((r) => r.at <= fake.of("/v1/sync/snapshot")[1]!.at).length - 1
  expect(failuresBeforePolling).toBe(3)
  for (const poll of fake.of("/v1/sync/snapshot").slice(1)) expect(poll.headers["if-none-match"]).toBe('"1"')
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  fake.snapshot = snapshot(2, { a: bool(false, 2) })
  expect((await changed)?.flagsChanged).toEqual(["a", "b", "n"])
  expect(await client.getBooleanValue("a", true)).toBe(false)
  // The stream comes back: polling stops.
  fake.force = null
  await fake.waitFor(() => fake.openStreams === 1, 3000, "the stream again")
  const polls = fake.of("/v1/sync/snapshot").length
  await Bun.sleep(200)
  expect(fake.of("/v1/sync/snapshot").length).toBe(polls)
})

test("fatal on 401 at initialize", async () => {
  fake.revoked.add(serverKey())
  const p = provider()
  const d = `t${++domain}`
  await expect(OpenFeature.setProviderAndWait(d, p)).rejects.toThrow()
  const client = OpenFeature.getClient(d)
  expect(client.providerStatus).toBe(ProviderStatus.FATAL)
  expect((await client.getBooleanDetails("a", false)).errorCode).toBe(ErrorCode.PROVIDER_FATAL)
  await Bun.sleep(100)
  expect(fake.requests.length).toBe(1)
})

test("fatal at once on end revoked, with no reconnect", async () => {
  const p = provider()
  const client = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  const err = nextEvent(p, ProviderEvents.Error)
  fake.endStreams("revoked")
  expect((await err)?.errorCode).toBe(ErrorCode.PROVIDER_FATAL)
  expect(client.providerStatus).toBe(ProviderStatus.FATAL)
  await Bun.sleep(150)
  expect(streamOpens().length).toBe(1)
  expect(fake.of("/v1/sync/snapshot").length).toBe(1)
})

test("fatal on 401 when reconnecting", async () => {
  const p = provider()
  const client = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  const err = nextEvent(p, ProviderEvents.Error)
  fake.revoked.add(serverKey())
  fake.dropStreams()
  expect((await err)?.errorCode).toBe(ErrorCode.PROVIDER_FATAL)
  expect(client.providerStatus).toBe(ProviderStatus.FATAL)
  const n = fake.requests.length
  await Bun.sleep(150)
  expect(fake.requests.length).toBe(n)
})

test("honours Retry-After on 429", async () => {
  await ready(provider())
  await fake.waitFor(() => fake.openStreams === 1)
  fake.force = { status: 429, retryAfter: "1", paths: ["/v1/sync/stream"] }
  fake.dropStreams()
  await fake.waitFor(() => streamOpens().length >= 3, 5000, "two retries")
  const [, first, second] = streamOpens()
  expect(second!.at - first!.at).toBeGreaterThanOrEqual(950)
  fake.force = null
})

test("initialize waits out a pending snapshot (503 Retry-After)", async () => {
  fake.snapshot = null
  setTimeout(() => fake.setSnapshot(snapshot(1, { a: bool(true, 1) })), 300)
  const client = await ready(provider())
  expect(await client.getBooleanValue("a", false)).toBe(true)
  expect(fake.of("/v1/sync/snapshot").length).toBe(2)
})

test("initialize gives up after its timeout, then recovers in the background", async () => {
  fake.force = { status: 503 }
  const p = provider({ timeoutMs: 200 })
  const d = `t${++domain}`
  await expect(OpenFeature.setProviderAndWait(d, p)).rejects.toThrow()
  const client = OpenFeature.getClient(d)
  expect(client.providerStatus).toBe(ProviderStatus.ERROR)
  const back = nextEvent(p, ProviderEvents.Ready)
  fake.force = null
  await back
  expect(client.providerStatus).toBe(ProviderStatus.READY)
  expect(await client.getBooleanValue("a", false)).toBe(true)
})

test("bootstrap and offline", async () => {
  const dir = mkdtempSync(join(tmpdir(), "ezgh-flags-"))
  const file = join(dir, "production.json")
  // A bootstrap version above the data plane's: the data plane's snapshot still replaces it.
  writeFileSync(file, snapshot(50, { a: bool(false, 9) }))

  // offline: only the file, never the network.
  const off = new EzghProvider({ offline: true, bootstrapPath: file, endpoint: fake.url })
  providers.push(off)
  const offClient = await ready(off)
  expect(await offClient.getBooleanValue("a", true)).toBe(false)
  await Bun.sleep(50)
  expect(fake.requests.length).toBe(0)

  // bootstrap: ready from the file with the data plane down, then syncs when it's back.
  fake.force = { status: 503 }
  const p = provider({ bootstrapPath: file })
  const client = await ready(p)
  expect(await client.getBooleanValue("a", true)).toBe(false)
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  fake.setSnapshot(snapshot(6, { a: bool(true, 10) }))
  fake.force = null
  expect((await changed)?.flagsChanged).toEqual(["a"])
  expect(await client.getBooleanValue("a", false)).toBe(true)
  // The bootstrap file's version is never named to the data plane.
  for (const r of fake.of("/v1/sync/snapshot")) expect(r.headers["if-none-match"]).not.toBe('"50"')
  for (const r of streamOpens().filter((r) => JSON.parse(r.body).version !== undefined)) expect(JSON.parse(r.body).version).toBe(6)
  expect(JSON.parse(streamOpens()[0]!.body)).toEqual({})

  expect(() => new EzghProvider({ offline: true, sdkKey: serverKey() })).toThrow(/bootstrapPath/)
})

test("type mismatch gives TYPE_MISMATCH and the default", async () => {
  const client = await ready(provider())
  const d = await client.getStringDetails("a", "x")
  expect(d.value).toBe("x")
  expect(d.errorCode).toBe(ErrorCode.TYPE_MISMATCH)
})

test("a client key is refused at construction", () => {
  expect(() => new EzghProvider({ sdkKey: "ezgh_fc_" + "A".repeat(43), endpoint: fake.url })).toThrow(/server key/)
})

test("onClose ends the stream and stops everything", async () => {
  const p = provider()
  await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  await p.onClose()
  await fake.waitFor(() => fake.openStreams === 0, 3000, "the stream to close")
  const n = fake.requests.length
  await Bun.sleep(100)
  expect(fake.requests.length).toBe(n)
})

test("a silent connection is abandoned after the idle timeout", async () => {
  await ready(provider({ idleTimeoutMs: 150 }))
  await fake.waitFor(() => fake.openStreams === 1)
  await fake.waitFor(() => streamOpens().length >= 2, 3000, "a reconnect after silence")
})
