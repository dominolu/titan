// Generated from Rust management DTOs. Do not edit.
export const API_VERSION = 1;
export const EXPECTED_BUILD = "de1103944b7f65ed3536affddb69a531305a186d335d463187b761533d1dbe58";
export type Role = "reader" | "operator";
export type Identity = { username: string, role: Role, nodes: Array<string>, accounts: Array<string>, environments: Array<string>, };
export type Session = { identity: Identity, csrf_token: string, expires_at_ns: string, api_version: number, build: string, };
export type OperationState = "pending" | "running" | "succeeded" | "failed" | "cancelled" | "unknown";
export type Action = { "kind": "describe", template_id: string, parameters: unknown, } | { "kind": "build", version_id: string, } | { "kind": "prepare", dataset_id: string, } | { "kind": "backtest", artifact_id: string, dataset_id: string, stress: boolean, } | { "kind": "report", backtest_id: string, } | { "kind": "deploy", artifact_id: string, binding_id: string, } | { "kind": "start", deployment_id: string, } | { "kind": "pause", run_id: string, generation: string, } | { "kind": "resume", run_id: string, generation: string, } | { "kind": "stop", run_id: string, generation: string, } | { "kind": "recover", run_id: string, generation: string, checkpoint_id: string, };
export type SubmitOperation = { idempotency_key: string, node_id: string, action: Action, };
export type Operation = { id: string, actor: string, node_id: string, input: Action, account: string | null, input_digest: string, state: OperationState, cancel_requested: boolean, created_at_ns: string, updated_at_ns: string, result: unknown, error: string | null, revision: string, };
export type Resource = { id: string, kind: string, node_id: string, revision: string, frozen: boolean, digest: string, body: unknown, };
export type ResearchWorkspace = { id: string, name: string, template_count: number, resource_count: number, };
export type SaveVersion = { node_id: string, template_id: string, parameters: unknown, expected_revision: string | null, freeze_operation_id: string | null, };
export type Snapshot = { api_version: number, cursor: string, server_time_ns: string, operations: Array<Operation>, resources: Array<Resource>, };
export type Event = { cursor: string, resource_id: string, kind: string, revision: string, payload: unknown, };
export type ApiError = { code: string, message: string, request_id: string, };
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
