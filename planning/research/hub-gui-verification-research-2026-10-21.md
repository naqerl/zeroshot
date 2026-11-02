# Research notes: GUI verification for agents (2026-10-21)

> **Status: research notes, not product behavior.** Collected on 2026-10-21 as input for
> future hub eval and verification work. Nothing here describes what Zeroshot or the hub does
> today. Quotes are copied from the sources linked next to them; external pages can change.

Two questions:

1. How does Lauren Tan ([@poteto](https://x.com/poteto)) have agents interact with GUIs, VMs and
   servers when they verify work?
2. What are the 2025-2026 practices and tools for AI agents that drive GUIs on servers, and what
   setup fits a Linux server where coding agents need GUI verification?

## Part 1: Lauren Tan

### Summary

She never names a display server, VNC, or a desktop automation tool. Her stack is:

- **Precise control:** drive the app through the Chrome DevTools Protocol (CDP) "or equivalent",
  via project control skills, preferring the accessibility tree and stable selectors.
- **Real desktop:** Cursor Cloud Agent VMs, which provide "computer use" and screen recording.
- **Proof:** screenshots and video, reviewed by a different agent and by humans.

The Cursor Cloud Agent substrate under her VMs is documented by Cursor (see
[Cursor substrate](#cursor-substrate)): on Linux it is X11 with `xdotool` and `ffmpeg`, plus a
TigerVNC and Xfce desktop on headless machines.

### Articles

"How I Use Cursor" (2026-05-25, [post](https://x.com/poteto/status/2058975157503570132)):

- "First, he tries to reproduce the problem through computer use. Cursor Cloud Agents can run
  Cursor itself in the cloud, where they interact with the desktop, click on things, and send
  keyboard input. Internally, this uses more skills I made to control our products
  programmatically using protocols like CDP or equivalent." ("He" is her bug bot, Benny.)
- "Additional workers are spawned in this run to take a video of the before and after, and
  finally a worker opens the PR for review with the video in the description."
- "Benny understands images and video attachments, explores the codebase using pstack skills..."

"Loops You Can Trust" (2026-06-24, [post](https://x.com/poteto/status/2069824386283319343)):

- "I quickly put together a /control-glass skill for Cursor. It let agents launch a dev build
  with the Chrome DevTools Protocol enabled. This allowed agents to click and type, inspect the
  accessibility tree, take screenshots, record video, throttle the CPU or network, capture CPU
  profiles, and take heap snapshots."
- "Cursor is an Electron app, so I could use tools I was already familiar with."
- "Each agent could now get its own checkout, build, ports, browser state, and isolated Cursor
  instance. Several agents could investigate different problems in parallel without colliding."
- "The repro automation waits for that verdict, opens a real Cursor build in the cloud, and
  follows the same path as the reporter. It has to hit the exact broken state twice and capture
  screenshots and video."
- "For every component, the automation captures computed styles before and after across light,
  dark, and high-contrast themes. ... It records the interactions, pairs the screenshots, and asks
  another agent to inspect the media."
- "Artifacts, like screenshots or videos, beat a plausible sounding explanation which may be
  wrong. An artifact gives me something I can inspect and challenge myself."

### X posts

| Date       | Quote                                                                                                                                                                                                                                                                         | Link                                                    |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| 2026-05-13 | "you can literally fix anything with cursor debug mode and a way to control your app via CDP or equivalent"                                                                                                                                                                   | [post](https://x.com/poteto/status/2054428503299514677) |
| 2026-08-05 | "we have a verification skill that teaches agents how to run and control a dev build, so every agent can interact with it over cdp"                                                                                                                                           | [post](https://x.com/poteto/status/2084864096525234470) |
| 2026-08-29 | "you can run cursor cloud agents on your own machines? for example you can setup a mac mini ... as a worker, meaning you can verify your iOS apps in a simulator"                                                                                                             | [post](https://x.com/poteto/status/2093625075119632399) |
| 2026-08-31 | "the verification skill works for client changes too! cursor cloud agents can run it and take videos / screenshots"                                                                                                                                                           | [post](https://x.com/poteto/status/2094532214637961218) |
| 2026-10-06 | "they run your code in their own VM and take videos and screenshots. those artifacts are what i primarily review these days"                                                                                                                                                  | [post](https://x.com/poteto/status/2107529532144234937) |
| 2026-10-06 | "bots get their own computers in the cloud! so they can run your code in their own VM"                                                                                                                                                                                        | [post](https://x.com/poteto/status/2107330952812990537) |
| 2026-10-09 | "cloud agents run on fairly beefy machines, no GPU though"                                                                                                                                                                                                                    | [post](https://x.com/poteto/status/2108601188644192389) |
| 2026-10-09 | "I rely on videos / screenshots a lot although some interactions are hard for the agent to capture"                                                                                                                                                                           | [post](https://x.com/poteto/status/2108471414122815799) |
| 2026-10-12 | "giving your agent the ability to verify its own work, most commonly by actually running and interacting with it like a real user would ... most commonly this would be a video and/or screenshots for UI work ... verification is just approximation and can still be wrong" | [post](https://x.com/poteto/status/2109436202554372476) |

### pstack and related files

Source: [cursor/plugins](https://github.com/cursor/plugins) at `main` commit `190d287`
(2026-10-20). Paths are relative to that repository.

Authorship: the pstack plugin manifest names Lauren Tan as author. The `control-ui` and
`control-cli` skills that do the driving live in `cursor-team-kit`, whose manifest names Eric
Zakariasson; pstack points to them rather than defining its own driver.

No file in the repository mentions any of these terms: Wayland, X11, Xvfb, VNC, noVNC, TigerVNC,
xdotool, ffmpeg, "computer use", "virtual display". (Matches for "display" are the ordinary
word.)

Drivers (cursor-team-kit and third party, not hers):

- [`cursor-team-kit/skills/control-ui/SKILL.md`](https://github.com/cursor/plugins/blob/main/cursor-team-kit/skills/control-ui/SKILL.md):
  "Build or adapt a local browser/CDP harness to drive and inspect a web, IDE, or Electron UI."
  "For Electron/Chromium, enable a remote debugging port when supported." Examples use
  `chromium.launch()` and `chromium.connectOverCDP("http://127.0.0.1:<debug-port>")` with a
  `1280 x 800` viewport. "Prefer accessibility roles, labels, and stable `data-*` selectors over
  coordinates." "Avoid coordinate clicks unless a fresh screenshot was captured immediately
  before the click." Interaction loop: snapshot, one structural action, fresh snapshot.
- [`cursor-team-kit/skills/control-cli/SKILL.md`](https://github.com/cursor/plugins/blob/main/cursor-team-kit/skills/control-cli/SKILL.md):
  tmux (`capture-pane`, `send-keys`) or a Python PTY probe; "Recording a short terminal demo when
  output is easier to show than explain."
- [`cursor-team-kit/skills/run-smoke-tests/SKILL.md`](https://github.com/cursor/plugins/blob/main/cursor-team-kit/skills/run-smoke-tests/SKILL.md):
  "Run Playwright smoke tests, debug failures, and verify fixes."
- [`third_party/playwright/mcp.json`](https://github.com/cursor/plugins/blob/main/third_party/playwright/mcp.json):
  Playwright MCP plugin (`npx -y @playwright/mcp@latest`), authored by Cursor.

pstack (hers):

- [`pstack/skills/create-verification-skill/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/create-verification-skill/SKILL.md):
  "Drive: how can an agent interact with it programmatically? Existing harnesses first —
  Playwright/Cypress specs, expect scripts, PTY helpers, curl-able endpoints, a debug port. Only
  then pick a generic recipe: browser/CDP for web and Electron, a tmux/PTY harness for CLI/TUI,
  plain HTTP for services." "Isolate: can two instances run side by side (ports, data dirs,
  profiles)?" "Prefer stable handles (ARIA labels, data attributes, prompt strings, route paths)
  over coordinates and tab order." "proof artifacts survive the teardown".
- [`pstack/skills/create-verification-skill/references/feature-map-example/README.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/create-verification-skill/references/feature-map-example/README.md)
  (with `search.md` and `create-note.md`): "Prefer ARIA roles and accessible names over CSS
  selectors or DOM position." "UI proof includes an ARIA snapshot and a screenshot with the app
  identity visible." Example: `control-notes browser snapshot --aria --path ...` plus
  `control-notes browser screenshot --path ...`.
- [`pstack/skills/maintain-verification-skill/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/maintain-verification-skill/SKILL.md):
  "one long-lived instance driven serially for servers and UIs, or a fresh isolated session per
  drive for short-lived CLIs".
- [`pstack/automations/benny/skills/reproduce-and-fix-issues/references/control-adapter.md`](https://github.com/cursor/plugins/blob/main/pstack/automations/benny/skills/reproduce-and-fix-issues/references/control-adapter.md):
  her most detailed GUI contract. Required capabilities are Bring up, Drive UI ("Click, Type,
  Press keys, Scroll, Drag, Resize"), Inspect state ("Accessibility tree, DOM or view hierarchy,
  Process state..."), Screenshot ("must show enough app chrome to prove that the correct app is
  under test"), Recording ("Start and stop a screen recording around the full repro path";
  return "Captured window or region"), and Cleanup ("Debug ports and tunnels"). "Prefer roles,
  labels, and stable selectors. Use coordinates only after a fresh screenshot." "Do not set
  internal state, call hidden app methods, write directly to storage, or inject DOM changes to
  create the symptom." "Hardware prompts, operating-system permission dialogs, device-only APIs,
  and unavailable account states may be real blocks." Setup check ends with "Capture a
  screenshot. Record a short clip. Clean up."
- [`pstack/automations/benny/skills/reproduce-and-fix-issues/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/automations/benny/skills/reproduce-and-fix-issues/SKILL.md):
  "Capture screenshots. Start and stop a screen recording." "Do not pretend a screenshot, unit
  test, state mutation, or source reading is a UI repro." "Have a read-only media reviewer answer
  one question: does the evidence visibly show the discriminating broken state?"
- [`pstack/automations/benny/skills/reproduce-and-fix-issues/references/verify-existing-fix.md`](https://github.com/cursor/plugins/blob/main/pstack/automations/benny/skills/reproduce-and-fix-issues/references/verify-existing-fix.md):
  "Capture baseline recording, screenshot, and state check", then "Capture after recording,
  screenshot, and the same state check."
- [`pstack/automations/benny/skills/setup-benny/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/automations/benny/skills/setup-benny/SKILL.md):
  the adapter check must "Capture screenshots" and "Start and stop a recording".
- [`pstack/automations/benny/FOR_AGENTS.md`](https://github.com/cursor/plugins/blob/main/pstack/automations/benny/FOR_AGENTS.md):
  "reproduce the exact symptom twice through the real ui, and capture screenshots, video, and a
  read-only state cross-check."
- [`pstack/automations/benny/skills/triage-issue-reports/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/automations/benny/skills/triage-issue-reports/SKILL.md):
  "Read screenshots at full useful resolution. Review video for the state transition that
  separates correct and broken behavior."
- [`pstack/skills/poteto-mode/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/SKILL.md):
  "Shipping UI / IDE / CLI → the matching control skill. `cursor-team-kit` publishes
  `control-cli` (CLIs and TUIs) and `control-ui` (browser / Electron / web UIs)."
- [`pstack/skills/poteto-mode/playbooks/multi-phase-plan.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/multi-phase-plan.md):
  "Browser, Electron, and web UIs use `control-ui` from `cursor-team-kit`. ... Native mobile uses
  whatever simulator-driving skill the repo has." "Each live lane runs on its own cloud VM at the
  PR head." "Save every screenshot to `/tmp/swarm-<pr-id>/worker-<n>/<slug>.png`". "Record a 30
  to 60 second video of the change on a lane VM."
- [`pstack/skills/poteto-mode/playbooks/shipping.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/shipping.md):
  "One verifier per PR, each a Cursor cloud agent, runs the change the way a user would with the
  matching control skill (such as `control-ui` or `control-cli` from `cursor-team-kit`) ... Green
  CI and an approving bot review are not verdicts, because neither runs the change as a user
  would."
- [`pstack/skills/poteto-mode/playbooks/autopilot-full.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/autopilot-full.md):
  each PR owner is a Cursor cloud agent; "Assume the VM dies."
- [`pstack/skills/poteto-mode/playbooks/orchestrate.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/orchestrate.md):
  "Work that exists only on one VM when that VM dies was never done." "CI green is an input to a
  verdict, not a verdict."
- [`pstack/skills/poteto-mode/playbooks/visual-parity.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/visual-parity.md):
  "a visual regression harness that screenshots the current component across its states ...
  image diff ... A nonzero diff is a fail."
- [`pstack/skills/poteto-mode/playbooks/prototype.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/prototype.md):
  "screenshot each variant via the control skill and drive the interaction."
- [`pstack/skills/poteto-mode/playbooks/runtime-forensics.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/runtime-forensics.md)
  and [`trace-forensics.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/trace-forensics.md):
  "a CDP trace for a visual glitch"; "Inject instrumentation via CDP eval on the running
  process".
- [`pstack/skills/poteto-mode/playbooks/opening-a-pr.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/opening-a-pr.md):
  "attach videos or screenshots when they prove a claim."
- [`pstack/skills/swarm/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/swarm/SKILL.md):
  workers spawn with `environment: "cloud"`; "local" only when a worker needs the user's
  computer.
- [`pstack/skills/principle-prove-it-works/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/principle-prove-it-works/SKILL.md):
  "Indirect verification (file mtimes, output freshness, agent self-reports, cached screenshots)
  feels cheaper than direct observation."
- [`pstack/skills/ketchup/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/ketchup/SKILL.md)
  and [`pstack/skills/show-me-your-work/SKILL.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/show-me-your-work/SKILL.md):
  embed screenshots and videos inline; evidence is an artifact, trace, or screenshot path.
- [`pstack/docs/guide/02-poteto-mode.md`](https://github.com/cursor/plugins/blob/main/pstack/docs/guide/02-poteto-mode.md):
  "The cleanest isolation is a cloud subagent. Each one gets its own VM and branch, so it can
  install dependencies, run your app, and record video of the result without touching your
  machine."
- [`pstack/docs/guide/06-verify-and-ship.md`](https://github.com/cursor/plugins/blob/main/pstack/docs/guide/06-verify-and-ship.md):
  "an existing harness first, otherwise browser and CDP, a PTY, or plain HTTP"; "show me a video
  and screenshots as proof."
- [`pstack/docs/guide/04-design.md`](https://github.com/cursor/plugins/blob/main/pstack/docs/guide/04-design.md),
  [`07-overnight.md`](https://github.com/cursor/plugins/blob/main/pstack/docs/guide/07-overnight.md),
  [`10-recipes-and-pitfalls.md`](https://github.com/cursor/plugins/blob/main/pstack/docs/guide/10-recipes-and-pitfalls.md)
  and [`pstack/skills/poteto-help/references/recipes.md`](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-help/references/recipes.md):
  screenshot and video prompts such as "fix it and show me a video as proof"; "Parallel agents in
  one worktree ... Run them as cloud agents".

`harness/pstack/claude-code/...` mirrors the same pstack files for Claude Code.

### What she does not mention

- Wayland, X11, Xvfb, VNC, noVNC, TigerVNC, xdotool, ffmpeg, or any desktop environment: none,
  in posts, articles, or pstack.
- Any pixel-based computer-use API, or any screen resolution setting. The only resolution in
  the files is the `1280 x 800` Playwright viewport in `control-ui`, which is not hers.
- How recording is done. "Record video" is always a capability of the cloud VM or the control
  skill.
- agent-browser, browser-use, or Stagehand. CDP and Playwright appear only through
  `control-ui`.

### Cursor substrate

Cursor documents the environment her cloud verifiers run in:

- [Computer use and desktop sharing](https://cursor.com/docs/cloud-agent/self-hosted/computer-use):
  "On Linux, computer use uses an X11 display." Packages: `dbus-x11 ffmpeg
tigervnc-standalone-server x11-utils x11-xserver-utils xdotool xfce4`. With no display
  configured, "the worker starts its own TigerVNC desktop running an Xfce session." Desktop
  sharing uses "a worker-created, isolated agent desktop"; "No inbound ports open." On macOS a
  helper app needs Accessibility and Screen Recording permissions that a person must approve.
- [Capabilities](https://cursor.com/docs/cloud-agent/capabilities): "Each cloud agent runs in its
  own isolated VM with a full desktop environment." Artifacts "are attached to the PR so you can
  quickly validate changes without checking out the branch locally."
- [Cursor agents can now control their own computers](https://cursor.com/blog/agent-computer-use)
  (2026-02-24).

**Unverified:** a third-party teardown by mxschmitt
([gist](https://gist.github.com/mxschmitt/e83db32079f25c291853626d5aeb6cc5)) says the
`computerUse` subagent sees 1280x800 webp screenshots, injects input with `xdotool`, has "no
access to DOM, accessibility tree, or Chrome DevTools Protocol", and records the X11 display with
`ffmpeg` into `/opt/cursor/artifacts/`. Cursor has not confirmed this; treat it as unverified.

Taken together, her layering is: CDP and accessibility control skills for precision, the VM's
pixel computer use for real desktop interaction, and the VM's recording for proof.

## Part 2: practices and tools (2025-2026)

### X11 and Wayland

- X11 with Xvfb (or TigerVNC) remains the default for headless agent desktops. Anthropic's
  reference uses "A virtual X11 display server (using Xvfb)" with Mutter and Tint2
  ([docs](https://platform.claude.com/docs/en/agents-and-tools/tool-use/computer-use-tool)).
  Cursor uses X11, xdotool, ffmpeg and TigerVNC (above). E2B Desktop runs Xvfb, Xfce, x11vnc and
  noVNC on port 6080 ([docs](https://e2b.dev/docs/template/examples/desktop)). Daytona exposes
  `start()` and `stop()` "for the underlying Xvfb and VNC processes"
  ([OpenAI cookbook](https://developers.openai.com/cookbook/examples/agents_sdk/computer_use_with_daytona/computer_use_with_daytona)).
- Wayland is workable but fragmented. The sanctioned path is the xdg-desktop-portal
  RemoteDesktop portal: input through libei (`ConnectToEIS`, "EIS (recommended)") and capture
  through PipeWire (`ScreenCast.OpenPipeWireRemote`), with `persist_mode` and `restore_token` to
  skip later consent dialogs
  ([portal docs](https://flatpak.github.io/xdg-desktop-portal/docs/doc-org.freedesktop.portal.RemoteDesktop)).
  wlroots compositors offer virtual pointer and keyboard protocols, with `grim` for capture.
- `ydotool` "writes to `/dev/uinput`, which means root ..., no focus awareness ... It bypasses the
  compositor entirely"; `xdotool` "is X11-only and does not work on Wayland at all"
  ([wdotool README](https://github.com/cushycush/wdotool)).
- One CUA toolkit's Wayland backend refuses to load on compositors that "silently reject
  unprivileged input injection (GNOME mutter default, Hyprland ...)"
  ([docs](https://docs.getwayland.com/core/computer-use/)).

Conclusion: use X11 in dedicated agent VMs. Use Wayland only when the behavior under test is
Wayland-specific.

### Computer-use APIs

- Anthropic `computer` tool
  ([docs](https://platform.claude.com/docs/en/agents-and-tools/tool-use/computer-use-tool)): "For
  general desktop tasks, use 1024x768 or 1280x720; for web applications, use 1280x800 or
  1366x768. Avoid resolutions above 1920x1080." Scale screenshots and coordinates in your own
  tool rather than relying on API resizing. Put instruction text before the screenshot; use
  `zoom` for small text. Listed limitations: latency ("Focus on use cases where speed isn't
  critical (for example, background information gathering, automated software testing)"),
  coordinate hallucination, scrolling reliability, and prompt injection. Classifiers scan
  screenshots for injected instructions.
- OpenAI computer tool
  ([guide](https://developers.openai.com/api/docs/guides/tools-computer-use),
  [integration](https://developers.openai.com/api/docs/guides/tools-computer-use-integration)):
  use `detail: "original"`, remap coordinates after any downscale; "we observe strong performance
  with 1440 × 900 and 1600 × 900 desktop resolutions." "Use an isolated browser or VM ... Treat
  screen content as untrusted ... Confirm consequential actions ... check the actual outcome".
- Google Gemini: Gemini 2.5 Computer Use (2025-10-07) was "primarily optimized for web browsers
  ... not yet optimized for desktop OS-level control"
  ([blog](https://blog.google/innovation-and-ai/models-and-research/google-deepmind/gemini-computer-use-model/)).
  It was shut down on 2026-07-28; Gemini 3.x and 4 models support browser, mobile and desktop,
  with a per-action `intent` field and opt-in prompt-injection screenshot scanning
  ([docs](https://ai.google.dev/gemini-api/docs/computer-use)).

### Structure before pixels

- Playwright MCP "uses accessibility snapshots instead of screenshots"; its comparison rates
  snapshots low in token cost and exact in precision, against high cost and "coordinate
  guessing" for screenshots. It advises re-snapshotting after navigation, `browser_find` on large
  pages, and adding screenshots for canvas or charts
  ([docs](https://playwright.dev/mcp/snapshots)).
- Vercel agent-browser: a Rust CLI built on accessibility snapshots with `@eN` refs
  ([site](https://agent-browser.dev/)).
- Third-party comparisons (2026) suggest browser-use for open-ended Python agent tasks, Stagehand
  v4 for deterministic production scripts, and Playwright MCP for short QA and coding-agent work
  ([fp8.co](https://fp8.co/articles/Browser-Use-vs-Stagehand-vs-Playwright-MCP-AI-Agent-Browser-Automation)).
- Native desktops: read the accessibility tree first (AT-SPI on Linux, UI Automation on Windows,
  AX on macOS). Each widget gives "role ... name ... exact bounding box ... states", and you can
  "Verify by re-reading states". Caveat: "some Electron/Chromium apps and web content ... need the
  browser's own accessibility bridge turned on", so keep vision as a fallback
  ([Joche Ojeda, 2026-08-22](https://jocheojeda.com/2026/08/22/at-spi-first-grounding/);
  [ATSPI MCP server](https://github.com/C-Loftus/ATSPI-MCP-Server)).
- Electron: launch with `--remote-debugging-port` and attach with `connectOverCDP`, as in
  `control-ui`. This gives exact control plus traces and heap snapshots.

### Known limitations

- Latency: every screenshot and action is a model round trip (Anthropic docs).
- Coordinate grounding errors: keep resolution fixed and small, and scale in your own code.
- Token cost: screenshots cost far more than accessibility snapshots (Playwright docs).
- Prompt injection from screen content: Anthropic reported cutting attack success from 23.6% to
  11.2% in Claude for Chrome ([blog](https://claude.com/blog/claude-for-chrome), 2025-08;
  [research](https://www.anthropic.com/research/prompt-injection-defenses)). Every vendor
  recommends isolation, allowlists and human confirmation.
- Uncapturable interactions: Lauren notes "some interactions are hard for the agent to capture";
  macOS screen recording permission always needs a person (Cursor docs).

## Recommendation: Linux server with GUI verification

1. **Isolation.** One disposable VM or container per agent run, with its own display, ports,
   browser profile and app data directory. No real credentials; restricted egress.
2. **Display.** X11, not Wayland: Xvfb, or TigerVNC with Xfce when people need to watch. Fixed
   1280x800 (or 1366x768) at 24-bit depth, one window manager, no screen saver. Install
   `xdotool`, `ffmpeg`, `x11-utils` and Chromium. This matches Cursor's documented package list,
   so the same image can host `agent worker --computer-use`.
3. **Viewing.** noVNC or VNC bound to localhost or a private network, never a public port;
   view-only by default.
4. **Driving order.** The repository's own harness first (for example Playwright specs), then CDP
   or accessibility snapshots (Playwright MCP or CLI, agent-browser, `connectOverCDP` for
   Electron), then AT-SPI for native GTK or Qt apps, and pixel computer use only for what is left
   (canvas, OS dialogs). Click coordinates only from a fresh screenshot.
5. **Proof.** Record each run (`ffmpeg -f x11grab`, or Playwright `recordVideo` and traces). Save
   screenshots and accessibility snapshots outside the repository under a stable artifact path
   that survives cleanup, and attach them to the PR. A separate read-only reviewer agent checks
   that the media shows the discriminating state, as in pstack's `control-adapter.md`.
6. **Process.** Encode launch, doctor, drive, observe and cleanup as a repository verification
   skill in the style of `create-verification-skill`. A verifier that did not write the code runs
   it at the PR head. Green CI is an input to the verdict, not the verdict.
