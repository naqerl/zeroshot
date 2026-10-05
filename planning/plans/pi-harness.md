# Pi harness lane

Status: proposed. Open questions are resolved inline below; no `BLOCKING-SPEC` review yet.

Authority: maintainer request to add support of the Pi harness, "as a provider along with
codex/cc". Read as: add `pi` as a fourth `RuntimePlan` harness alongside `codex` and `claude`
(Claude Code), with its own provider enum, on the same admission, capability and durability
contracts. Not read as: replace or re-scope the existing `codex`, `claude`, or `copilot`
lanes, and not read as: GitHub Copilot becoming a Pi provider.

Upstream target: `earendil-works/pi` (`@earendil-works/pi-coding-agent`, MIT, Node >= 22.19),
documented as "a minimal, extensible agent harness". The integration surface used here is the
documented CLI only: `--mode json`, `--model`, `--provider`, `--thinking`, `--session-id`,
`--session-dir`, `PI_CODING_AGENT_DIR`, and per-provider API-key environment variables.

## Acceptance

| Clause | Required behavior |
| --- | --- |
| Protocol | `RuntimePlan::Pi { provider, size, nodes }` is a first-class tagged variant. `pi/<provider>` pairs decode and re-encode byte-identically; `pi` with a non-Pi provider is rejected by shape, not by a table. |
| Local run | `zeroshot run --uniform-runtime-config` accepts `{"harness":"pi",...}` for every admitted provider lane, executes real Pi turns in the shared workspace, and returns the admitted outcome. |
| Hosted run | The same plan executes inside the disposable capsule with a private Pi config/session home and declared connections only. |
| Sessions | `node_instance` scope reuses one Pi session across loop revisits; `execution` scope gets a fresh one; a provider-reported identity change is a provider failure. |
| Structured output | Final assistant text is validated locally against the node response contract with at most two in-session corrections, exactly like the existing correction invariant. |
| Isolation | Pi exposes no permission control. Containment and the shared prompt boundary remain the isolation story, and AGENTS.md says so explicitly. |
| Contracts | No new registry, model catalog, provider-availability check, or protocol detection. Model strings stay opaque and caller-owned. |

Explicitly out of scope: Pi as a *provider* behind the `copilot` harness; Pi SDK/TypeScript
embedding; Pi sub-agents, plan mode, MCP, `codemode`, `tool_search`; a managed Pi copy of the
product skill (see Escalations); any change to the retired Node product paths.

## Key findings that drive the design

1. **Pi has no native structured-output flag, and an extension cannot supply one.**
   `constrainedSampling` is a *tool* field: it constrains a tool's input schema, never the final
   assistant message, and its strict support skips `openrouter` (`packages/ai/README.md`). Use
   the harness-neutral mechanism that already exists: `render_agent_prompt`
   (`native_v2_runner/response.rs:468`) states the contract, `resolve_agent_response` (:262)
   validates locally, and `AgentResponseState` (:215) allows two correction turns.
2. **`--mode json` is a closer fit than `--mode rpc`.** It resolves to the one-shot print path and
   emits LF-framed JSONL (`packages/coding-agent/src/main.ts`), matching the Claude adapter's
   `ProviderJsonLines`/`ProviderExecution`/`turn_process` skeleton. RPC mode is a persistent
   bidirectional protocol and would duplicate Copilot's framing budget, request-id correlation
   and 64 MiB message bound. Keep RPC as an optimization for long node sessions, not a
   prerequisite.
3. **The prompt belongs on stdin, not argv.** A node prompt carries its full input JSON and can
   exceed `ARG_MAX`, and AGENTS.md requires provider stdin/stdout to be concurrent and bounded.
   Pi prepends piped stdin to the first prompt (`cli/initial-message.ts`).
4. **Effort maps without translation.** `provider_process::effort_token`
   (`native_v2_capsule/provider_process.rs:490`) yields `low|medium|high|xhigh|max`, all of which
   Pi accepts as `--thinking` levels, which it then clamps to model capability. Zeroshot passes
   the token through and records the effective level from `thinking_level_changed` instead of
   validating provider capability.
5. **Pi has no permission system** (`README.md`, "Permissions & Containerization", which points
   at containerization instead). Copilot is the existing precedent for a harness with no
   `apply_permission_default` and no configuration probe.
6. **Session identity needs a charset adapter.** Pi requires session IDs to start and end
   alphanumeric over `[A-Za-z0-9._-]` (`docs/cli.md`); Zeroshot's `NodeInstanceId`/`ExecutionId`
   are bounded opaque strings with no such guarantee.
7. **The `gateway` lane needs one synthesized config file, not a synthesized catalog.**
   `<agent-dir>/models.json` overrides only `baseUrl`/`headers`/`apiKey`/`api` while leaving the
   built-in models registered, and `--provider` with an unmatched `--model` resolves the caller's
   own ID as a custom model id. No catalog, no protocol detection.

## Provider lane matrix

| `pi` provider | Pi provider name | Credentials | `native_local` | Notes |
| --- | --- | --- | --- | --- |
| `anthropic` | `anthropic` | `ANTHROPIC_API_KEY` | yes | Local reuses `~/.pi/agent/auth.json`; declared key suppresses ambient. |
| `openai` | `openai` | `OPENAI_API_KEY` | yes | Local reuses stored login; declared key suppresses ambient. |
| `openrouter` | `openrouter` | `OPENROUTER_API_KEY` | no | Private Pi config home so ambient `auth.json` cannot leak in. |
| `bedrock` | `amazon-bedrock` | `AWS_BEARER_TOKEN_BEDROCK`, `AWS_REGION` | no | Same contract as `codex/bedrock` and `claude/bedrock`. Pi's provider name is `amazon-bedrock`. |
| `gateway` | `openai` (override) | `GATEWAY_BASE_URL`, `GATEWAY_API_KEY` | no | Adapter-owned: pinned to `openai-responses`, same lane semantics as `codex/gateway`. Never detected. |

`native_local` mirrors `provider_access.rs`: `true` reuses native login state and needs no
connection; `false` always materializes the canonical missing requirement. Pi stores login in
`<agent-dir>/auth.json` and resolves credentials in the order `--api-key`, `auth.json`, a
`models.json` `apiKey`, then provider environment variables (`docs/models.md#authenticate`), so
native reuse means exactly `auth.json` reuse and `native_local` is `true` for `anthropic` and
`openai` only, and a contained run's private `PI_CODING_AGENT_DIR` leaves the declared
environment variable as the only credential source. `accepted_field_sets` for `anthropic` accepts
`ANTHROPIC_API_KEY` and `ANTHROPIC_OAUTH_TOKEN` as API credentials plus `ANTHROPIC_AUTH_TOKEN` as
bearer authentication (`docs/providers.md`); the Claude-only `CLAUDE_CODE_OAUTH_TOKEN` pair does
not apply. A present-but-empty or malformed declared credential still fails closed before Pi
starts, as it does for Copilot.

The `gateway` lane writes exactly one file into the private agent directory and nothing else:

```json
{
  "providers": {
    "openai": {
      "baseUrl": "https://gateway.example/api/v1",
      "api": "openai-responses",
      "apiKey": "$GATEWAY_API_KEY"
    }
  }
}
```

`api` is the provider-level discriminant `models.json` accepts (`ProviderConfigSchema`,
`core/model-config.ts`) and `openai-responses` is what built-in `openai` already uses
(`packages/ai/src/providers/openai.ts`), so the pin needs no protocol detection. A provider entry
with `baseUrl` and no `models` keeps every built-in OpenAI model registered (`applyModelsJson`,
`core/provider-composer.ts`), and an unmatched caller model ID becomes a custom model id on that
provider (`buildFallbackModel`, `core/model-resolver.ts`), so model strings stay opaque. Pi
appends nothing to `baseUrl`: the built-in value is `https://api.openai.com/v1` and the request
goes to `{baseUrl}/responses`, so a caller-owned base path is preserved verbatim as
`codex/gateway` does. `baseUrl` takes a literal while `apiKey` accepts `$NAME` interpolation,
keeping the secret in the declared connection and out of the file.

## Runtime contract

### Command line (per turn)

```
pi --mode json
   --model <caller model, unchanged>
   --provider <pi provider name>          # always passed together with --model
   [--thinking <effort_token>]            # only when the binding sets effort
   --session-dir <private session dir>
   [--session-id <derived pi session id>]
   [--no-extensions --tools read,bash,edit,write --no-approve]   # contained placement only
```

Prompt text is written to stdin and stdin is then closed; Pi resolves the prompt only at EOF, so
the existing `send_process_input` close-stdin step at `native_v2_capsule/provider_process.rs:100`
is what starts the run, exactly as for Claude. Stdout is drained concurrently as JSONL.
`--print` is not needed, and neither is `--`: the adapter supplies no positional prompt. Never
pass `--api-key`, `--system-prompt`, or `@file` arguments. `--provider` is always passed so
model resolution stays provider-scoped, and because Pi reads a trailing `:level` on a model
pattern as a thinking level, a caller model ID ending in `:<level>` is only unambiguous when
`--thinking` is passed with it.

Contained placement names Pi's own documented default tool set rather than a Zeroshot
preference, adds `--no-extensions` so no discovered, configured, or built-in extension code
loads, and passes `--no-approve` so a project `.pi` directory or `.agents/skills` tree in the
workspace cannot load settings, MCP servers, extensions, skills, or system prompts. That
overrides `defaultProjectTrust` instead of relying on its `"ask"` default, which only skips those
resources in non-interactive modes (`docs/security.md`). Context files load after the trust
decision regardless (`docs/how-pi-works.md`), so repository-declared setup stays visible and
`-nc` is never passed. A local user's own extensions, skills, and MCP servers stay active, which
is the local/contained split Codex and Claude already have; local runs get no `--tools` override.

### Session identity

Derive the Pi session ID from the node's `PositiveIdentity` (`NodeInstanceId` for
`node_instance`, execution identity otherwise) through one bounded function: lowercase hex of a
SHA-256 over the identity, prefixed `zs-`, 67 characters total. Pi bounds only the charset and
requires the first and last characters to be alphanumeric, which `zs-` plus hex satisfies, and
documents no length bound. Reject an empty or oversized identity before hashing. Correction turns
reuse the same ID; never `--fork`.

Pass `--session-dir` in every placement and every turn, pointing at `<private home>/sessions`. It
outranks `PI_CODING_AGENT_SESSION_DIR` and the `sessionDir` setting, which matters because Pi
reads a project `sessionDir` before resolving project trust and declining trust cannot undo that
lookup (`docs/security.md`). `ProviderExecutionFiles::home()` is already keyed by session scope
(`process_scope`, `native_v2_capsule/provider_process.rs:275`; `home()`,
`provider_process/filesystem.rs:173`) and released only after confirmed
cleanup, so one directory serves node-instance revisits, gives `execution` scope a fresh session,
and keeps local runs out of `~/.pi/agent/sessions`.

The `{"type":"session","version":3,...}` header (`docs/json.md`) is the identity record. When the
ID was supplied, a header reporting a different `id` is a provider failure, mirroring
`observe_session` in `native_v2_claude/transcript/session_id.rs`.

### Response extraction

`message_update` records are delta-only. The response value comes from the last assistant
`message_end.message`, with `text_end.content` replacing reconstructed blocks, per Pi's own
`reduceAssistantMessageFrames()` rules. Deltas feed `LiveOutputStream::Stdout` only.
`agent_settled` closes the run, but success comes from that final message's `stopReason`: a failed
or aborted response still appears in the stream without a nonzero exit status
(`docs/cli-integration.md`). `message_update.usage` is provisional usage and the terminal usage
comes from the final assistant message, following `native_v2_claude/transcript/usage.rs`. Ignore
unknown future event types before validating provider-owned fields, which absorbs a versioned
event vocabulary; a renamed terminal event fails the turn rather than hanging. Keep the shared
64 MiB unfinished-record guard, accept a complete final record without a newline, and keep
draining after the first valid terminal event.

### Environment and redaction

Add `PI_LOCAL_ENVIRONMENT` next to the Codex/Claude/Copilot allowlists:

- provider keys: `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_OAUTH_TOKEN`,
  `OPENAI_API_KEY`, `OPENROUTER_API_KEY`, `AWS_BEARER_TOKEN_BEDROCK`, `AWS_REGION`
- endpoint settings: `ANTHROPIC_BASE_URL`, `ANTHROPIC_BEDROCK_BASE_URL`, `OPENAI_BASE_URL`
- Pi process controls: `PI_CODING_AGENT_DIR`, `PI_CODING_AGENT_SESSION_DIR`, `PI_OFFLINE`,
  `PI_SKIP_VERSION_CHECK`, `PI_TELEMETRY`
- proxy/CA names already arrive through `LOCAL_TRANSPORT_ENVIRONMENT`

Contained placement sets `PI_OFFLINE=1` and `PI_TELEMETRY=0`; Pi expands `PI_OFFLINE=1` into
`PI_SKIP_VERSION_CHECK=1` itself, so the latter needs no separate assignment. `PI_OFFLINE` gates
only Pi's automatic network activity, the `pi.dev` catalog refresh and the latest-version request;
print mode streams straight to the provider, so provider traffic is untouched and the bundled
catalog still serves an offline start. Suppressing it makes a contained run deterministic.
`PI_CODING_AGENT_DIR` points at `<private home>` in contained placement and in every local lane
whose `native_local` is `false`; a local `native_local` lane leaves it unset so Pi reads the
user's `~/.pi/agent`. It is a new home override, so it joins the relative-path resolution list at
`native_v2_local.rs:147`. It, `PI_OFFLINE`, and `PI_TELEMETRY` are adapter-reserved, so a declared
connection colliding with one is a provider failure, the Copilot rule at
`native_v2_copilot/command.rs:288`. No redaction change: `provider_redactions` already treats
`*_BASE_URL`, `*_API_KEY`, `*_AUTH_TOKEN`, and `*_OAUTH_TOKEN` as private local values and redacts
every declared connection value, so neither `ANTHROPIC_OAUTH_TOKEN` nor the literal
`GATEWAY_BASE_URL` written into `models.json` can reach a diagnostic
(`native_v2_capsule/provider_process/environment.rs:199`).

### Permission policy

No `apply_permission_default`, no configuration probe, no argv bypass, exactly as for Copilot.
Keep Pi out of the hosted bypass matrix, assert in `native_v2_pi/tests.rs` that no permission
argument is ever appended, and state the exception in AGENTS.md so the "each harness owns its
native policy parser" invariant does not read as a universal obligation.

## Phases

Each phase is one PR titled with a Conventional Commit header, targets `main`, keeps its own
focused tests, and passes `CI / required` on its own. The split follows the precedent of
`054ad3fd feat(providers): add user-backed GitHub Copilot CLI (#1119)`, which landed the
protocol variant, the CLI lane, the adapter, the image pin, and the docs together.

### Phase A — `feat(providers): add the Pi harness lane`

**The protocol variant and the adapter cannot ship in separate PRs.** Eight exhaustive matches
break the moment `RuntimePlan::Pi` exists, and every one of them is a composition root:

| Site | What it needs |
| --- | --- |
| `crates/.../native_v2_run/wire.rs:18,40,49` | `RuntimePlan::Pi`, then `Self::Pi` in `size()` and `nodes()`. |
| `zeroshot/src/native_v2_local.rs:347` | `local_harness` arm constructing a Pi config. |
| `zeroshot/src/native_v2_hosting/allocator.rs:674` | `harness` arm constructing a contained Pi config. |
| `zeroshot/src/native_v2_candidate.rs:186` | `validate_config` arm (workspace + provider match). |
| `zeroshot/src/native_v2_candidate/provider_access.rs:53,131` | `pi_contract` and `runtime_nodes_mut`. |
| `zeroshot/src/native_v2_cli/execution/submission.rs:520,595` | `into_runtime_plan` and `insert_template_binding`. |

Adding placeholder arms instead would be throwaway code, which AGENTS.md forbids. `acp.rs`
uses `matches!` and stays Codex/Claude-only without a compile change.

| File | Change |
| --- | --- |
| `crates/openengine-cluster-protocol/src/native_v2_run.rs` | Add `PiProvider` beside the existing provider enums (:363, :381, :392) with `CodexProvider`'s derives. |
| `zeroshot/src/native_v2_contract.rs` | Re-export `PiProvider` beside the other provider re-exports (:18). |
| `zeroshot/src/native_v2_cli/execution/submission.rs` | `UniformHarness::Pi`, `UniformProvider::pi()`. The incompatible-pair table lives here, not in `parser.rs`. |
| `zeroshot/src/native_v2_cli/parser.rs` | Add the `pi/*` pairs and the native-local note to the `--runtime-config` help prose (:439, :461). |
| `zeroshot/src/lib.rs` | `pub mod native_v2_pi;`. |
| `zeroshot/src/native_v2_pi.rs` and `native_v2_pi/` | `command.rs` (argv, reserved environment, home derivation), `transcript.rs` (+ `transcript/` submodules), `session.rs`, `turn_process.rs`, `tests.rs`. Mirror the Claude module split and Copilot's `provider_homes`/`insert_reserved_environment`. |
| `zeroshot/src/native_v2_capsule/provider_process/environment.rs` | `PI_LOCAL_ENVIRONMENT`. |
| `zeroshot/src/native_v2_candidate.rs` | `NativeV2HarnessConfig::Pi`, the `build_candidate` arm, `NativeV2CandidateError::Pi`. |
| `zeroshot/src/native_v2_local.rs` | The `local_harness` arm with `executable: "pi"`, plus `PI_CODING_AGENT_DIR` in `capture_local_native_environment`. |
| `zeroshot/src/native_v2_hosting.rs`, `native_v2_hosting/allocator.rs` | `pi_executable` on `ProductionHostingConfig` and the allocator config, the empty-executable validation, the default `/usr/local/bin/pi`. |
| `zeroshot/src/native_v2_capsule/permission_tests.rs` | No change, as decided above. |
| `protocol/openengine-cluster/v1/schema.json` | Regenerate: `cargo run -p openengine-cluster-testkit --bin generate-cluster-protocol -- --write`, verify with `npm run protocol:check`. |
| `docs/zeroshot-cli.md`, `docs/zeroshot-cli.html` | `cargo run -p zeroshot --example generate_cli_docs -- --write`, then `--check`. |

Tests beside the owning module:

- `native_v2_pi/tests.rs` — argv construction per provider lane, session-ID derivation bounds,
  header identity mismatch, `node_instance` reuse, correction turns, redacted diagnostics,
  empty/non-JSON stdout, and the contained run's environment.
- `native_v2_candidate/provider_access/tests.rs` — the `pi` x provider matrix, including
  `native_local` rows and declared-alternate acceptance.
- `native_v2_cli/tests/environment.rs` — Pi rows in the two native-harness loops and in
  `uniform_gateway_and_bedrock_runtime_materializes_for_both_harnesses_with_exact_defaults`,
  whose name and inner `["codex", "claude"]` loop both widen to three harnesses.
- `native_v2_cli/execution/submission/tests.rs` — the uniform pair matrix.
- `native_v2_contract/tests.rs` — a round-trip and a shape-rejection case beside
  `unsupported_harness_provider_pair_is_rejected_by_shape` (:96).
- `native_v2_hosting/allocator/lifecycle_tests.rs`, `native_v2_hosting/tests.rs` — fixture
  updates for the new config field.
- `zeroshot/tests/native_v2_cli_help.rs` — the pair-list prose assertion.
- Unix fixtures use `openengine_cluster_testkit::fixture::write_executable`.

`zeroshot/tests/native_v2_cli_local/harness_environment.rs` gains nothing: it drives the two
configuration probes, so `exercise_local_configuration` ends in `unreachable!()` for any other
harness, and `assert_harness_arguments` hardcodes both resume and bypass flags. Pi has neither.

### Phase B — `build(target): install Pi in the target image`

Pin with `ARG PI_VERSION=1.0.3`, the current npm `latest`, matching the exact-pin style of `ARG
COPILOT_VERSION`, `ARG CODEX_VERSION`, and `ARG CLAUDE_CODE_VERSION`. The package publishes
`bin.pi -> dist/bundle/cli.js`, declares `engines.node >= 22.19.0` (satisfied by the image's
Node 24), and has no install script, so `--strict-allow-scripts` needs no new `--allow-scripts`
entry. Use npm, not `pi.dev/install.sh`: `pi.dev` installs a self-updating managed install that Pi
rewrites from its own startup path (`package-manager-cli.ts`, `cleanupManagedInstall`), which must
not happen inside an image.

- `docker/zeroshot-target/Dockerfile` — `ARG PI_VERSION`, the pinned npm install into
  `/opt/zeroshot/harness`, and the `zeroshot-harness` symlink.
- `docker/zeroshot-target/harness.sh` — add `pi` to the `codex | copilot` Node-interpreter
  branch, since its `bin` is a Node script.
- `docker/zeroshot-target/smoke-toolchains.sh` — `pi --version` beside the other three. It needs no
  update-suppressing flag because it prints and exits before any network work.
- `docker/zeroshot-target/README.md` — the harness list in the image description.
- `.github/workflows/ci.yml`, `.github/workflows/release.yml` — one `docker run --entrypoint pi`
  line and one isolated-user smoke step each, the shape `054ad3fd` added for Copilot; the existing
  entries do not generalize.

Build and smoke the Dockerfile locally, as CONTRIBUTING requires for image changes.

### Phase C — `docs(providers): document the Pi harness`

- `docs/reference/runtime-plan.md` — harness/provider table.
- `docs/concepts/runtimes-and-connections.md` — connection key table and lane notes.
- `docs/concepts/targets.md`, `docs/getting-started/install.md`, `docs/guides/acp.md`,
  `docs/index.md`, `docs/llms.txt` — prose that enumerates the harnesses.
- `README.md` — the harness lists at :39, :57, :60, and :227.
- `ui/src/RuntimeEditor.tsx` — one `harnessLabels` entry (:6). The provider list and the generic
  harness rendering already come from `runtimeSchema`, so no other UI change is needed.
- `sdks/python/src/zeroshot/runtime.py` — the `UniformRuntime.harness` docstring.
- `AGENTS.md` — runtime invariants: the Pi provider lanes, the absence of a native permission
  policy, session-identity mapping, and structured output relying on local validation. AGENTS.md
  requires this update for any architectural change.

Gate with `python -m mkdocs build --strict`, `npm run lint`, `npm test`, and the SDK lint
lane. ACP stays Codex/Claude-only (`native_v2_cli/acp.rs:848`); do not widen it silently.

## Validation

Per phase, narrowest first:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
npm run lint
npm test
npm run distribution:check
npm run protocol:check
cargo run -p zeroshot --example generate_cli_docs -- --check
python -m mkdocs build --strict
```

Phase A also needs the SDK lane only if `UniformRuntime.harness` docs change there (ruff,
pydoclint, mypy, pytest). Phase B needs the image build and smoke. Phase C needs
`npm run lint`, `npm test`, the UI feature lane, and the SDK lane. Adapters must keep the
four-parameter Clippy ceiling; prefer a request struct over a lint exception.

## Escalations

**Should the npm package install a managed Zeroshot skill copy at Pi's user scope?**

- Options: (a) out of scope for this change; (b) add Pi as a fourth managed-skill scope in
  `npm/zeroshot`; (c) keep Pi local-only.
- Recommend (a). Pi reads `<agent-dir>/AGENTS.md` alongside Codex, Copilot, and Claude Code, so a
  local `pi` run is the one local lane without the product skill. But a local run is meant to
  execute the user's existing coding agent, and AGENTS.md's "do not fork the skill by host" is
  about copying, not host coverage. (b) is a separate change to `npm/zeroshot/install.js` and the
  AGENTS.md host sentence, and needs its own decision on where Pi's home sits when
  `PI_CODING_AGENT_DIR` is set.
- Blocked: nothing in Phases A-C. Revisit only if local Pi runs prove unusable without it.