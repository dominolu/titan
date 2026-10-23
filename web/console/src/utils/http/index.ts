import Axios, { type AxiosRequestConfig } from "axios";
import type { RequestMethods, PureHttpRequestConfig } from "./types.d";
import { parseExact } from "@/utils/precision";
let csrfToken = "";
export function setCsrfToken(value: string) {
  csrfToken = value;
}
class PureHttp {
  private instance = Axios.create({
    timeout: 10000,
    withCredentials: true,
    transformResponse: [
      data => (typeof data === "string" && data ? parseExact(data) : data)
    ]
  });
  constructor() {
    this.instance.interceptors.request.use(config => {
      if (!["get", "head", "options"].includes(config.method ?? "get"))
        config.headers.set("X-CSRF-Token", csrfToken);
      return config;
    });
    this.instance.interceptors.response.use(
      response => response.data,
      error => {
        if (error.response?.status === 401)
          window.dispatchEvent(new Event("titan:session-expired"));
        error.message = error.response?.data?.message ?? error.message;
        return Promise.reject(error);
      }
    );
  }
  request<T>(
    method: RequestMethods,
    url: string,
    params?: AxiosRequestConfig,
    extra?: PureHttpRequestConfig
  ): Promise<T> {
    return this.instance.request({
      method,
      url,
      ...params,
      ...extra
    }) as Promise<T>;
  }
  post<T, P = unknown>(
    url: string,
    params?: AxiosRequestConfig<P>,
    extra?: PureHttpRequestConfig
  ) {
    return this.request<T>("post", url, params, extra);
  }
  get<T, P = unknown>(
    url: string,
    params?: AxiosRequestConfig<P>,
    extra?: PureHttpRequestConfig
  ) {
    return this.request<T>("get", url, params, extra);
  }
}
export const http = new PureHttp();
