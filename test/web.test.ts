import { afterEach, beforeEach, expect, test } from "bun:test"
import { ClientProviderStatus, ErrorCode, OpenFeature, ProviderEvents, type EventDetails } from "@openfeature/web-sdk"
import { EzghWebProvider, type EzghWebProviderOptions } from "../src/web.ts"
import { clientKey, FakeDataPlane, type BulkFlag } from "./fake-data-plane.ts"

class DocumentStub extends EventTarget {
  visibilityState: "visible" | "hidden" = "visible"
  set(state: "visible" | "hidden") {
    this.visibilityState = state
    this.dispatchEvent(new Event("visibilitychange"))
  }
}

let fake: FakeDataPlane
let doc: DocumentStub
let domain = 0
const providers: EzghWebProvider[] = []
const storageTouches: string[] = []
const g = globalThis as Record<string, unknown>

function storageSpy(name: string) {
  return new Proxy({}, { get: (_t, p) => (storageTouches.push(`${name}.${String(p)}`), () => null), set: (_t, p) => (storageTouches.push(`${name}.${String(p)}=`), true) })
}

beforeEach(() => {
  fake = new FakeDataPlane()
  fake.bulk = (ctx) => flagsFor(ctx)
  doc = new DocumentStub()
  g.document = doc
  g.localStorage = storageSpy("localStorage")
  g.sessionStorage = storageSpy("sessionStorage")
  storageTouches.length = 0
})

afterEach(async () => {
  await Promise.all(providers.splice(0).map((p) => p.onClose()))
  fake.stop()
  delete g.document
  delete g.localStorage
  delete g.sessionStorage
})

let on = false
function flagsFor(ctx: Record<string, unknown>): BulkFlag[] {
  return [
    { key: "new-checkout", value: on, variant: String(on), reason: "TARGETING_MATCH", metadata: { flagVersion: on ? 2 : 1 } },
    { key: "greeting", value: ctx.targetingKey === "bob" ? "hi bob" : "hello", variant: ctx.targetingKey === "bob" ? "bob" : "plain", reason: "TARGETING_MATCH", metadata: { flagVersion: 1 } },
    { key: "limits", value: { max: 3 }, variant: "three", reason: "STATIC", metadata: { flagVersion: 4 } },
    { key: "off", value: 7, variant: "seven", reason: "DISABLED", metadata: { flagVersion: 2 } },
    { key: "broken", errorCode: "PARSE_ERROR", errorDetails: "bad targeting" },
  ]
}

beforeEach(() => {
  on = false
})

function provider(opts: Partial<EzghWebProviderOptions> = {}): EzghWebProvider {
  const p = new EzghWebProvider({ clientKey: clientKey(), endpoint: fake.url, minBackoffMs: 10, maxBackoffMs: 40, ...opts })
  providers.push(p)
  return p
}

async function ready(p: EzghWebProvider, context: Record<string, string> = { targetingKey: "alice" }) {
  const d = `w${++domain}`
  await OpenFeature.setContext(d, context)
  await OpenFeature.setProviderAndWait(d, p)
  return { client: OpenFeature.getClient(d), domain: d }
}

function nextEvent(p: EzghWebProvider, type: ProviderEvents, timeoutMs = 3000): Promise<EventDetails | undefined> {
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

const bulks = () => fake.of("/ofrep/v1/evaluate/flags", "POST")

test("initialize: one bulk evaluation, then synchronous answers", async () => {
  const p = provider()
  const { client } = await ready(p)
  expect(bulks().length).toBe(1)
  expect(bulks()[0]!.headers.authorization).toBe(`Bearer ${clientKey()}`)
  expect(JSON.parse(bulks()[0]!.body)).toEqual({ context: { targetingKey: "alice" } })
  expect(client.getBooleanValue("new-checkout", true)).toBe(false)
  expect(client.getStringValue("greeting", "")).toBe("hello")
  expect(client.getObjectValue("limits", {})).toEqual({ max: 3 })
  const off = client.getNumberDetails("off", 0)
  expect(off.value).toBe(7)
  expect(off.reason).toBe("DISABLED")
  expect(off.variant).toBe("seven")
  expect(off.flagMetadata.flagVersion).toBe(2)
  const missing = client.getBooleanDetails("nope", true)
  expect(missing.value).toBe(true)
  expect(missing.errorCode).toBe(ErrorCode.FLAG_NOT_FOUND)
  const broken = client.getBooleanDetails("broken", true)
  expect(broken.value).toBe(true)
  expect(broken.errorCode).toBe(ErrorCode.PARSE_ERROR)
  const mismatch = client.getStringDetails("new-checkout", "x")
  expect(mismatch.value).toBe("x")
  expect(mismatch.errorCode).toBe(ErrorCode.TYPE_MISMATCH)
  expect(client.getObjectDetails("greeting", {}).errorCode).toBe(ErrorCode.TYPE_MISMATCH)
  expect(bulks().length).toBe(1)
  expect(p.metadata.name).toBe("ezgh-flags")
  expect(p.runsOn).toBe("client")
})

test("onContextChange: one new bulk request, results replaced", async () => {
  const { client, domain } = await ready(provider())
  expect(client.getStringValue("greeting", "")).toBe("hello")
  await OpenFeature.setContext(domain, { targetingKey: "bob" })
  expect(bulks().length).toBe(2)
  expect(JSON.parse(bulks()[1]!.body)).toEqual({ context: { targetingKey: "bob" } })
  expect(client.getStringValue("greeting", "")).toBe("hi bob")
})

test("refresh on visibility: If-None-Match, and a 304 keeps the results", async () => {
  const p = provider()
  const { client } = await ready(p)
  const etag = fake.requests.length
  doc.set("hidden")
  await Bun.sleep(20)
  expect(bulks().length).toBe(1)
  doc.set("visible")
  await fake.waitFor(() => bulks().length === 2, 3000, "a refresh")
  expect(bulks()[1]!.headers["if-none-match"]).toMatch(/^"[0-9a-f]{64}"$/)
  await Bun.sleep(20)
  expect(client.getStringValue("greeting", "")).toBe("hello")
  expect(fake.requests.length).toBe(etag + 1)
  // A change seen on the next visibility refresh.
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  on = true
  doc.set("hidden")
  doc.set("visible")
  expect((await changed)?.flagsChanged).toEqual(["new-checkout"])
  expect(client.getBooleanValue("new-checkout", false)).toBe(true)
})

test("refresh every refreshMs", async () => {
  const p = provider({ refreshMs: 60 })
  const { client } = await ready(p)
  await fake.waitFor(() => bulks().length >= 3, 3000, "two periodic refreshes")
  for (const r of bulks().slice(1)) expect(r.headers["if-none-match"]).toBeDefined()
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  on = true
  await changed
  expect(client.getBooleanValue("new-checkout", false)).toBe(true)
})

test("live: a changed event triggers a bulk refetch and CONFIGURATION_CHANGED", async () => {
  const p = provider({ live: true })
  const { client } = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1, 3000, "the client stream")
  const stream = fake.of("/v1/client/stream", "POST")
  expect(stream.length).toBe(1)
  expect(stream[0]!.headers.authorization).toBe(`Bearer ${clientKey()}`)
  const changed = nextEvent(p, ProviderEvents.ConfigurationChanged)
  on = true
  fake.clientChanged()
  expect((await changed)?.flagsChanged).toEqual(["new-checkout"])
  expect(client.getBooleanValue("new-checkout", false)).toBe(true)
  expect(bulks().length).toBe(2)
  // Rotation reconnects.
  fake.endStreams("rotate")
  await fake.waitFor(() => fake.of("/v1/client/stream").length === 2 && fake.openStreams === 1, 3000, "a reconnect")
})

test("not live: no stream", async () => {
  await ready(provider())
  await Bun.sleep(50)
  expect(fake.of("/v1/client/stream").length).toBe(0)
})

test("no localStorage or sessionStorage", async () => {
  const p = provider({ live: true, refreshMs: 30 })
  const { domain } = await ready(p)
  await OpenFeature.setContext(domain, { targetingKey: "bob" })
  on = true
  fake.clientChanged()
  doc.set("hidden")
  doc.set("visible")
  await Bun.sleep(100)
  expect(storageTouches).toEqual([])
})

test("401: PROVIDER_FATAL", async () => {
  fake.revoked.add(clientKey())
  const p = provider()
  const d = `w${++domain}`
  await expect(OpenFeature.setProviderAndWait(d, p)).rejects.toThrow()
  expect(OpenFeature.getClient(d).providerStatus).toBe(ClientProviderStatus.FATAL)
})

test("end revoked on the live stream: PROVIDER_FATAL at once, no reconnect", async () => {
  const p = provider({ live: true })
  const { client } = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  const err = nextEvent(p, ProviderEvents.Error)
  fake.endStreams("revoked")
  expect((await err)?.errorCode).toBe(ErrorCode.PROVIDER_FATAL)
  expect(client.providerStatus).toBe(ClientProviderStatus.FATAL)
  await Bun.sleep(100)
  expect(fake.of("/v1/client/stream").length).toBe(1)
})

test("a key revoked later: the stream's reconnect gets 401, PROVIDER_FATAL", async () => {
  const p = provider({ live: true })
  const { client } = await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  const err = nextEvent(p, ProviderEvents.Error)
  fake.revoked.add(clientKey())
  fake.dropStreams()
  expect((await err)?.errorCode).toBe(ErrorCode.PROVIDER_FATAL)
  expect(client.providerStatus).toBe(ClientProviderStatus.FATAL)
})

test("initialize retries a 503, then is ready", async () => {
  fake.force = { status: 503, retryAfter: "0" }
  setTimeout(() => (fake.force = null), 100)
  const { client } = await ready(provider())
  expect(client.getStringValue("greeting", "")).toBe("hello")
  expect(bulks().length).toBeGreaterThan(1)
})

test("a server key is refused at construction", () => {
  expect(() => new EzghWebProvider({ clientKey: "ezgh_fs_" + "A".repeat(43) })).toThrow(/client key/)
})

test("onClose stops refreshes, the stream and the visibility listener", async () => {
  const p = provider({ live: true, refreshMs: 30 })
  await ready(p)
  await fake.waitFor(() => fake.openStreams === 1)
  await p.onClose()
  await fake.waitFor(() => fake.openStreams === 0)
  const n = fake.requests.length
  doc.set("hidden")
  doc.set("visible")
  await Bun.sleep(100)
  expect(fake.requests.length).toBe(n)
})

test("401 on a context change: FATAL, not ERROR", async () => {
  const p = provider()
  const { client, domain } = await ready(p)
  fake.revoked.add(clientKey())
  await OpenFeature.setContext(domain, { targetingKey: "bob" })
  await Bun.sleep(20)
  expect(client.providerStatus).toBe(ClientProviderStatus.FATAL)
})

test("a context change that fails answers defaults, then recovers with READY", async () => {
  const p = provider()
  const { client, domain } = await ready(p)
  fake.force = { status: 503 }
  await OpenFeature.setContext(domain, { targetingKey: "bob" })
  expect(client.providerStatus).toBe(ClientProviderStatus.ERROR)
  const back = nextEvent(p, ProviderEvents.Ready)
  fake.force = null
  await back
  expect(client.providerStatus).toBe(ClientProviderStatus.READY)
  expect(client.getStringValue("greeting", "")).toBe("hi bob")
})
