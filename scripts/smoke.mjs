// Imports the built package under plain Node (no TypeScript, no bundler), so a missing file
// extension or a Bun-only API in dist fails here rather than for users.
import assert from "node:assert/strict"
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { OpenFeature } from "@openfeature/server-sdk"
import * as server from "../dist/server.js"
import * as web from "../dist/web.js"

assert.equal(typeof server.EzghProvider, "function")
assert.equal(server.DEFAULT_ENDPOINT, "https://flags.ezghcloud.com")
assert.equal(typeof web.EzghWebProvider, "function")
assert.throws(() => new web.EzghWebProvider({ clientKey: "ezgh_fs_x" }), /client key/)

// Offline from a bootstrap file (the basics vector's snapshot): ready with no network.
const v = JSON.parse(readFileSync(new URL("../test/vectors/basics.json", import.meta.url), "utf8"))
const file = join(mkdtempSync(join(tmpdir(), "ezgh-flags-smoke-")), "snapshot.json")
writeFileSync(file, JSON.stringify(v.snapshot))
await OpenFeature.setProviderAndWait("smoke", new server.EzghProvider({ offline: true, bootstrapPath: file }))
const client = OpenFeature.getClient("smoke")
for (const c of v.cases.filter((c) => c.type === "boolean")) {
  const d = await client.getBooleanDetails(c.flag, c.default, c.context)
  assert.equal(d.value, c.expect.value, `${c.flag} ${JSON.stringify(c.context)}`)
  if (!c.expect.errorCode) assert.equal(d.reason, c.expect.reason)
}
await OpenFeature.close()
console.log("node smoke: ok")
