# titan-plugin-engine

`titan-plugin-engine` implements the control-plane kernel described by
`docs/plugin_engine_technical_design.md`. It owns plugin definitions, immutable assembly plans,
pre-bound service endpoint generations, activation gates, resource scopes, plugin lifecycle,
bounded control commands, and the restricted EventEngine control contract.

The crate intentionally does not implement exchange connectors, market/account/strategy plugins,
or the EventEngine. Those are separate components in the design. An EventEngine integrates through
`EventControl`; its implementation must preserve safe-point route transactions, enqueue delivery
away from the EventEngine thread, and retire all leases before completing subscription retirement.

Typical host flow:

1. Construct the EventEngine and an `Arc<dyn EventControl>` adapter.
2. Construct `PluginEngine`, which performs Core Runtime API negotiation.
3. Register static `PluginFactory` values.
4. Convert application configuration to `PluginSpec` values and call `apply`.
5. Use `change_plan`/`replace` for validated configuration replacement.
6. Call `quiesce_all`, then `stop_all`, before stopping the EventEngine.

Hot-path code keeps only `ServiceHandle`, `EventPublisher`, or restricted route handles. It never
looks up a provider in `PluginRegistry` or `ServiceRegistry`.
