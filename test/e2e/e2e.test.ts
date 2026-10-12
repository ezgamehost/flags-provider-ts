// End to end: both providers against the real flags-stack (`flags-stack all`: flags-api, the
// compiler worker, the change relay and flags-data in one process) with org-api faked by
// flags-stack's cmd/fake-orgapi. scripts/e2e.sh starts everything and runs this file; without
// E2E_API_URL (a plain `bun test`) it's skipped.
import { afterAll, describe, expect, test } from "bun:test"
import { OpenFeature as ServerOpenFeature, ProviderEvents as ServerEvents, ProviderStatus, type EventDetails } from "@openfeature/server-sdk"
import { OpenFeature as WebOpenFeature, ProviderEvents as WebEvents } from "@openfeature/web-sdk"
import { EzghProvider } from "../../src/server.ts"
import { EzghWebProvider } from "../../src/web.ts"

const api = process.env.E2E_API_URL ?? ""
const data = process.env.E2E_DATA_URL ?? ""
const apiKey = process.env.E2E_API_KEY ?? ""
const base = `${api}/v1/organizations/${process.env.E2E_ORG}/projects/${process.env.E2E_PROJECT}`

async function call<T = Record<string, unknown>>(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<T> {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json", ...(method === "POST" ? { "Idempotency-Key": crypto.randomUUID() } : {}), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${text}`)
  return (text ? JSON.parse(text) : {}) as T
}

function once(emitter: { addHandler: Function; removeHandler: Function }, type: string, timeoutMs: number): Promise<EventDetails | undefined> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`no ${type} within ${timeoutMs} ms`)), timeoutMs)
    const h = (d?: EventDetails) => {
      clearTimeout(t)
      emitter.removeHandler(type, h)
      resolve(d)
    }
    emitter.addHandler(type, h)
  })
}

describe.skipIf(!api)("e2e against flags-stack", () => {
  const closers: (() => Promise<void>)[] = []
  afterAll(async () => {
    for (const c of closers) await c()
  })

  test(
    "server and web providers follow a flag from off to on, and a revoked key is fatal",
    async () => {
      // 1. An environment, a client-side boolean flag, a server key and a client key.
      const env = await call<{ id: string }>("POST", "/environments", { key: "production", name: "Production" })
      const flag = await call<{ id: string }>("POST", "/flags", { key: "e2e-flag", name: "E2E flag", type: "boolean", clientSide: true })
      const server = await call<{ id: string; key: string }>("POST", `/environments/${env.id}/sdk-keys`, { kind: "server", name: "e2e server" })
      const client = await call<{ id: string; key: string }>("POST", `/environments/${env.id}/sdk-keys`, { kind: "client", name: "e2e client" })
      expect(server.key).toStartWith("ezgh_fs_")
      expect(client.key).toStartWith("ezgh_fc_")

      // The compiler works asynchronously: wait until the environment's snapshot has the flag.
      const deadline = Date.now() + 30_000
      for (;;) {
        const res = await fetch(`${data}/v1/sync/snapshot`, { headers: { Authorization: `Bearer ${server.key}` } })
        const text = await res.text()
        if (res.ok && (JSON.parse(text) as { flags?: Record<string, unknown> }).flags?.["e2e-flag"]) break
        if (Date.now() > deadline) throw new Error(`no snapshot with e2e-flag within 30 s (last: ${res.status} ${text.slice(0, 200)})`)
        await Bun.sleep(200)
      }

      // 2. The server provider is ready and the flag is off.
      const sp = new EzghProvider({ sdkKey: server.key, endpoint: data, timeoutMs: 30_000 })
      closers.push(() => sp.onClose())
      await ServerOpenFeature.setProviderAndWait("e2e", sp)
      const sc = ServerOpenFeature.getClient("e2e")
      const off = await sc.getBooleanDetails("e2e-flag", true, { targetingKey: "user-1" })
      expect(off.value).toBe(false)
      expect(off.reason).toBe("DISABLED")

      // 3. The web provider, live, sees the same.
      const wp = new EzghWebProvider({ clientKey: client.key, endpoint: data, live: true, timeoutMs: 30_000 })
      closers.push(() => wp.onClose())
      await WebOpenFeature.setContext("e2e", { targetingKey: "user-1" })
      await WebOpenFeature.setProviderAndWait("e2e", wp)
      const wc = WebOpenFeature.getClient("e2e")
      expect(wc.getBooleanValue("e2e-flag", true)).toBe(false)

      // 4. Turn it on: within 3 s the server provider announces it and answers true, and the web
      //    provider refetches on its stream's `changed`.
      const serverChanged = once(sp.events, ServerEvents.ConfigurationChanged, 3_000)
      const webChanged = once(wp.events, WebEvents.ConfigurationChanged, 5_000)
      await call("PUT", `/flags/${flag.id}/environments/${env.id}`, { enabled: true, offVariation: "false", targets: [], rules: [], defaultServe: { variation: "true" } })
      expect((await serverChanged)?.flagsChanged).toContain("e2e-flag")
      expect(await sc.getBooleanValue("e2e-flag", false, { targetingKey: "user-1" })).toBe(true)
      expect((await webChanged)?.flagsChanged).toEqual(["e2e-flag"])
      expect(wc.getBooleanValue("e2e-flag", false)).toBe(true)

      // 5. Revoke the server key: its stream ends and the provider is fatal.
      const fatal = once(sp.events, ServerEvents.Error, 5_000)
      await call("DELETE", `/environments/${env.id}/sdk-keys/${server.id}`)
      expect((await fatal)?.errorCode).toBe("PROVIDER_FATAL")
      expect(sc.providerStatus).toBe(ProviderStatus.FATAL)
    },
    60_000,
  )
})
