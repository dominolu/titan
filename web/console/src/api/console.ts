import { http } from "@/utils/http";
import type {
  Action,
  Operation,
  Resource,
  SaveVersion,
  Snapshot,
  SubmitOperation
} from "@/generated/management";
export const consoleApi = {
  resource: (id: string) =>
    http.get<Resource>(`/api/v1/resources/${encodeURIComponent(id)}`),
  log: (item: Resource, offset: number) =>
    http.get<{ text: string; next_offset: string; more: boolean }>(
      `/api/v1/resources/${encodeURIComponent(item.id)}/log`,
      { params: { generation: (item.body as any).generation, offset } }
    ),
  history: (item: Resource, kind: string, offset: number) =>
    http.get<{ items: unknown[]; source: unknown; recording: unknown }>(
      `/api/v1/resources/${encodeURIComponent(item.id)}/history`,
      {
        params: {
          generation: (item.body as any).generation,
          kind,
          offset,
          limit: 20
        }
      }
    ),
  acknowledge: (item: Resource) =>
    http.post<Resource>(
      `/api/v1/resources/${encodeURIComponent(item.id)}/acknowledge`,
      {
        data: {
          expected_revision: item.revision,
          condition_identity: (item.body as any).condition_identity
        }
      }
    ),
  snapshot: () =>
    http.get<Snapshot>("/api/v1/snapshot", { params: { limit: 200 } }),
  submit: (data: SubmitOperation) =>
    http.post<Operation>("/api/v1/operations", { data }),
  find: (key: string) =>
    http.get<Operation | null>("/api/v1/operations", {
      params: { idempotency_key: key }
    }),
  cancel: (id: string) =>
    http.post<Operation>(`/api/v1/operations/${id}/cancel`),
  save: (data: SaveVersion, id?: string) =>
    http.request<Resource>(
      id ? "patch" : "post",
      id ? `/api/v1/parameter-versions/${id}` : "/api/v1/parameter-versions",
      { data }
    ),
  file: (id: string, path: string) =>
    `/api/v1/files/${encodeURIComponent(id)}/${path.split("/").map(encodeURIComponent).join("/")}`
};
export type {
  Action,
  Operation,
  Resource,
  SaveVersion,
  Snapshot,
  SubmitOperation
};
