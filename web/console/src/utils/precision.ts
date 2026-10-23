// Preserve integer tokens before JSON.parse can round them. Unsupported browsers fail explicitly.
type SourceContext = { source?: string };
type ExactJSON = typeof JSON & { rawJSON?: (source: string) => unknown };
export function parseExact(text: string): any {
  return JSON.parse(
    text,
    (_key: string, value: unknown, context?: SourceContext) => {
      if (
        typeof value === "number" &&
        Number.isInteger(value) &&
        !Number.isSafeInteger(value)
      ) {
        const raw = (JSON as ExactJSON).rawJSON;
        if (!raw || !context?.source)
          throw new Error(
            "浏览器无法精确保留该整数，请使用支持精确 JSON 的新版浏览器"
          );
        return raw(context.source);
      }
      return value;
    }
  );
}
export function integerToken(text: string): unknown {
  if (!/^-?(0|[1-9]\d*)$/.test(text)) throw new Error("请输入十进制整数");
  return parseExact(text);
}
export function scalarText(value: unknown): string {
  if (value && typeof value === "object" && "rawJSON" in value)
    return String((value as { rawJSON: string }).rawJSON);
  return String(value ?? "");
}
