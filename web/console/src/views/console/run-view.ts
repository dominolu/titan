import type { Resource, Operation } from "@/api/console";
import { scalarText } from "@/utils/precision";

// Presentation adapters only. Amounts, risk and lifecycle decisions come from E.
export const object = (value: unknown): Record<string, any> =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};
export const rows = (value: unknown): Record<string, any>[] =>
  Array.isArray(value) ? value : [];
export const body = (item?: Resource) => object(item?.body);
export const raw = (value: unknown) =>
  value == null
    ? "—"
    : typeof value === "object"
      ? JSON.stringify(value)
      : scalarText(value);
export const pretty = (value: unknown) =>
  JSON.stringify(value ?? null, null, 2);

export function decimal(value: unknown, digits = 2): string {
  const input = scalarText(value);
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(input);
  if (!match) return "—";
  const fraction = match[3] ?? "";
  let magnitude = BigInt(
    match[2] + fraction.slice(0, digits).padEnd(digits, "0")
  );
  if (Number(fraction[digits] ?? "0") >= 5) magnitude++;
  const fixed = magnitude.toString().padStart(digits + 1, "0");
  const whole = digits ? fixed.slice(0, -digits) : fixed;
  const tail = digits ? fixed.slice(-digits) : "";
  return `${match[1] && magnitude !== 0n ? "-" : ""}${whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}${tail ? "." + tail : ""}`;
}

export function scaled(value: unknown, unit: unknown): string | null {
  const scale = Number(object(unit).scale);
  const coefficient = scalarText(object(unit).coefficient);
  const integer = scalarText(value);
  if (
    !/^-?\d+$/.test(integer) ||
    !/^\d+$/.test(coefficient) ||
    BigInt(coefficient) === 0n ||
    !Number.isInteger(scale) ||
    scale < 0 ||
    scale > 30
  )
    return null;
  const amount = BigInt(integer) * BigInt(coefficient);
  const sign = amount < 0n ? "-" : "";
  const text = (amount < 0n ? -amount : amount)
    .toString()
    .padStart(scale + 1, "0");
  return (
    sign + (scale ? `${text.slice(0, -scale)}.${text.slice(-scale)}` : text)
  );
}

export function time(value: unknown, full = false): string {
  try {
    const input = scalarText(value);
    if (!/^\d+$/.test(input) || BigInt(input) === 0n) return "—";
    const date = new Date(Number(BigInt(input) / 1000000n));
    if (!Number.isFinite(date.getTime())) return "—";
    return full
      ? date.toLocaleString("zh-CN", { hour12: false })
      : date.toLocaleTimeString("zh-CN", { hour12: false });
  } catch {
    return "—";
  }
}

export function symbolLabel(symbol: unknown) {
  const parts = scalarText(symbol).split("_");
  return parts.length >= 2 ? parts.slice(-2).join(" / ") : raw(symbol);
}
export const lifecycleLabels: Record<string, string> = {
  Running: "运行中",
  Paused: "已暂停",
  Stopping: "停止中",
  Stopped: "已停止",
  Failed: "失败",
  Faulted: "故障",
  Recovering: "恢复中",
  Unknown: "未知",
  RUNNING: "进程运行",
  STOPPED: "进程停止",
  UNKNOWN: "未知"
};
export const operationLabels: Record<string, string> = {
  pending: "待执行",
  running: "执行中",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
  unknown: "结果未知"
};
export const actionLabels: Record<string, string> = {
  pause: "暂停",
  resume: "恢复运行",
  stop: "停止",
  recover: "checkpoint 恢复"
};
// Native account wire statuses (not the Python strategy callback enum).
export const orderStates: Record<string, string> = {
  "0": "未指定",
  "1": "已挂单",
  "2": "已过期",
  "3": "已成交",
  "4": "已撤销",
  "5": "部分成交",
  "6": "已拒绝"
};

export function runView(
  item: Resource | undefined,
  connection: string,
  nodes: Resource[]
) {
  const state = body(item),
    observation = object(state.observation);
  const snapshot = object(observation.account_snapshot);
  const image = object(snapshot.image ?? snapshot.last_valid_image);
  const financial = object(observation.financial);
  const strategies = rows(observation.strategies);
  const strategy =
    state.strategy_id != null
      ? (strategies.find(
          s => scalarText(s.strategy_id) === scalarText(state.strategy_id)
        ) ?? {})
      : strategies.length === 1
        ? strategies[0]
        : {};
  const business = object(strategy.business);
  const data = business.available === true ? object(business.data) : {};
  const instrument = rows(image.instruments).find(
    i => i.native_symbol === state.instrument
  );
  const risk = rows(image.risk?.assets).find(
    r =>
      (r.instrument == null
        ? instrument &&
          scalarText(r.asset_id) === scalarText(instrument.asset_id)
        : r.instrument === state.instrument) &&
      (!instrument ||
        r.asset_id == null ||
        scalarText(r.asset_id) === scalarText(instrument.asset_id))
  );
  const node = nodes.find(n => n.node_id === item?.node_id);
  const stale =
    !item ||
    state.available !== true ||
    state.observation_stale === true ||
    snapshot.available !== true ||
    connection !== "connected" ||
    body(node).online !== true;
  const lifecycle = raw(
    state.lifecycle ?? strategy.lifecycle ?? state.process_state
  );
  const riskKnown =
    !stale &&
    risk?.complete === true &&
    /^-?\d+(\.\d+)?$/.test(raw(risk.actual_notional?.amount)) &&
    /^-?\d+(\.\d+)?$/.test(raw(risk.pending_bound_notional?.amount));
  return {
    state,
    observation,
    snapshot,
    image,
    financial,
    strategy,
    business,
    data,
    instrument,
    risk,
    stale,
    lifecycle,
    status:
      state.mode === "observe"
        ? "只读账户观测"
        : (lifecycleLabels[lifecycle] ?? lifecycle),
    equity: rows(image.equity),
    positions: rows(image.positions).filter(
      p => instrument && raw(p.asset_id) === raw(instrument.asset_id)
    ),
    orders: rows(image.orders),
    net: rows(financial.net_pnl),
    drawdown: rows(financial.max_drawdown),
    riskKnown,
    actual: riskKnown ? risk.actual_notional?.amount : null,
    bound: riskKnown ? risk.pending_bound_notional?.amount : null,
    currency: risk?.currency ?? "",
    grid: object(data.grid),
    config: object(data.config),
    actions: Array.isArray(state.allowed_actions)
      ? (state.allowed_actions.filter(a => a in actionLabels) as string[])
      : []
  };
}

export function operationsForRun(operations: Operation[], item?: Resource) {
  return item
    ? operations.filter(
        op =>
          "run_id" in op.input &&
          op.input.run_id === item.id &&
          op.node_id === item.node_id
      )
    : operations;
}
