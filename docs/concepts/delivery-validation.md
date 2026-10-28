# Caller-owned delivery validation

Local callers may select `ZEROSHOT_DELIVERY_CHECK_COMMAND` as one absolute private executable outside
of the checkout. Its path crosses the private controller bootstrap, never graph input or provider
environments. This is a trusted local integration point, not an OS isolation boundary against the
same user. Hosted profiles cannot select a local executable.

Before pushing the captured candidate (`publish`), immediately after a PR is synchronized (`published`),
while observing an open review (`review`), and immediately before requesting a merge (`merge`), delivery sends
`openengine.delivery-validation/v1` JSON on stdin. It includes the publication stage, repository,
branches, exact head and tree, trusted review base when available, and verifier-authored title and body.
No delivery credential is supplied. The helper returns exactly one verdict: `accepted`, `pending`,
`repair` with a public-safe reason, or `refused` with a public-safe reason. Pending keeps polling;
repair enters the authored repair/review loop; refusal, malformed output and process failure stop the
execution. Pending observation does not hide CI or new feedback; pending publication and merge block
those effects. Acceptance cannot override forge policy, required checks, reviews or merge queues.

The helper runs under the delivery poll policy's operation budget. A check that reads stdin, blocks
on a network mount, or deadlocks against the workspace therefore fails the delivery as a declared
timeout instead of leaving an execution that looks alive forever. Each forge authority call (branch
push, review observation, and merge request) gets one bounded operation deadline and narrows it to
the remaining time on retries; poll intervals govern repeated polling, not a single call.
Bounded pipes and cancellation use the normal process session; its process tree must be confirmed
gone before delivery releases the workspace, even if the outer operation is cancelled or its turn deadline expires. An accepted helper
must leave the captured candidate clean and unchanged. GitHub confirmation remains authoritative:
accepting or queuing a merge is not a merged receipt.

The published-stage notification precedes the first policy/status read, so a caller can durably record
the actual PR even when a subsequent inspection fails. Terminal merged/closed observations settle before
review-stage hooks or late feedback. A completed remote merge cannot become another repair.
