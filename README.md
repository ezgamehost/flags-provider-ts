# @ezgh/flags-openfeature

[OpenFeature](https://openfeature.dev) providers for **EZGH Feature Flags**, in TypeScript:

- `@ezgh/flags-openfeature/server`: `EzghProvider`, for backends on Node (20+), Bun and Deno.
  It keeps the environment's compiled flags in memory and evaluates locally with flagd's
  open-source core, so an evaluation never makes a network call.

You write against the OpenFeature API; the provider connects it to EZGH. Licensed Apache-2.0.

## Install

The package installs from this repository's release tags, which carry the built `dist/`
(it isn't published to npm):

```bash
npm i github:ezgamehost/flags-provider-ts#v0.1.0
bun add github:ezgamehost/flags-provider-ts#v0.1.0
pnpm add github:ezgamehost/flags-provider-ts#v0.1.0
```

Install the OpenFeature SDK you use alongside it: `@openfeature/server-sdk` for the server
provider.

## Server provider

Create a **server key** (`ezgh_fs_…`) for an environment in the console or with
`ezgh flags sdk-keys create`, and keep it secret: it can read every flag's rules.

```ts
import { OpenFeature } from "@openfeature/server-sdk"
import { EzghProvider } from "@ezgh/flags-openfeature/server"

await OpenFeature.setProviderAndWait(new EzghProvider({ sdkKey: process.env.EZGH_FLAGS_KEY! }))
const flags = OpenFeature.getClient()

if (await flags.getBooleanValue("new-checkout", false, { targetingKey: user.id, plan: user.plan })) {
  // …
}
```

- **Startup:** the provider fetches the environment's snapshot and is ready once it has it, then
  follows the data plane's sync stream. If the stream fails three times in a row, it polls the
  snapshot every 30 seconds while it keeps retrying the stream (exponential backoff with jitter,
  honouring `Retry-After`).
- **Changes** emit `PROVIDER_CONFIGURATION_CHANGED` with `flagsChanged`: the flags whose
  configuration changed, appeared or disappeared (a segment edit counts for the flags using it).
- **Disconnected** for 30 seconds: it keeps evaluating the last snapshot and emits
  `PROVIDER_STALE`, then `PROVIDER_READY` once it reaches the data plane again.
- **A revoked or unknown key** is fatal: `PROVIDER_ERROR` with `PROVIDER_FATAL`, and the client
  status becomes `FATAL` (OpenFeature then answers defaults).
- **Reasons:** `TARGETING_MATCH`, `STATIC`, `DEFAULT`, and `DISABLED` for a flag that's off in
  the environment (with its off value). `flagMetadata.flagVersion` is the flag's config version.

### Options

| Option | Default | |
| --- | --- | --- |
| `sdkKey` | | A server key. Required unless `offline`. |
| `endpoint` | `https://flags.ezghcloud.com` | The data plane. |
| `bootstrapPath` | | A snapshot file from `ezgh flags export`. The provider is ready from it without the network, then syncs. |
| `offline` | `false` | Use only `bootstrapPath` and never call the network (tests, CI, air-gapped runs). |
| `timeoutMs` | `10000` | How long `initialize` waits for the first snapshot. On a timeout it fails (status `ERROR`) and keeps trying; it emits `PROVIDER_READY` when the snapshot arrives. |
| `pollIntervalMs` | `30000` | The snapshot poll interval while the stream is down. |
| `staleAfterMs` | `30000` | How long without reaching the data plane before `PROVIDER_STALE`. |
| `minBackoffMs`, `maxBackoffMs` | `1000`, `60000` | Stream reconnect backoff. |
| `idleTimeoutMs` | `45000` | A stream silent this long (the data plane sends a heartbeat every 15 s) is reconnected. |
| `fetch`, `logger` | global `fetch`, console | |

### Offline and tests

```ts
// A file from `ezgh flags export --env production > flags.json`; no network at all.
await OpenFeature.setProviderAndWait(new EzghProvider({ offline: true, bootstrapPath: "flags.json" }))
```

## Conformance

`test/vectors/` holds flags-stack's conformance vectors (`protocol/vectors/eval`) verbatim, at the
commit in `.flags-stack-rev`. Every case is evaluated through this package's evaluator and must
match the value, variant, reason, flag version and error code that flags-stack, the Go provider
and the Python provider answer. CI checks the copy against flags-stack with a read-only deploy key.

To update them: `scripts/sync-vectors.sh ~/code/flags-stack <commit>`, then `bun run check`. Don't
edit the vectors here; a failing vector is a cross-core disagreement to fix in flags-stack.

## Development

```bash
bun install
bun run check   # typecheck, tests (vectors included), build, a Node import smoke test
```
