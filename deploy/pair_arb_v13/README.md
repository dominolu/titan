# pair_arb V13 deployment template

`runtime.toml` is pinned to the bundled ABI V13 artifact in `artifacts/pair_arb.titan`.
The strategy, both market sources, and both account connectors are intentionally disabled: this
repository task performed compilation and non-live verification only.

Before live activation, provision the referenced secret files, confirm the two account IDs and
instrument units, set a dedicated risk scope, and independently enable the required sources,
accounts, and strategy under a controlled rollout.

Strategy parameters are baked into the signed/digested artifact at compile time. Rebuild the
artifact and update `package.expected_digest` whenever `strategies/pair_arb/parameters.json`
changes; deployment-time `parameters` must remain `{}`.
