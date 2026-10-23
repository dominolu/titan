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
