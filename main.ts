/**
 * xyc relay v4 — Claude 1h nested cache + runtime time after breakpoints
 * Deno / Zeabur 单文件
 *
 * 默认透传流式。FORCE_NON_STREAM=1 才改成非流式再转 SSE。
 *
 * 缓存：tools → 去掉时间后的 system → 稳定历史尾（最多 4 个 1h 断点）
 * 时间：代理生成，永远是最后一条 user，不带 cache_control
 *
 * 环境变量：
 *   UPSTREAM_URL        默认 https://cn.chatapi.app
 *   PROXY_TOKEN         可选；有则请求必须带 x-proxy-token
 *   CACHE_TTL_ON        "0" = 不改 cache；默认开
 *   INJECT_CURRENT_TIME "0" = 不注入时间；默认开
 *   TIME_ZONE           默认 Asia/Shanghai
 *   FORCE_NON_STREAM    "1" = 强制非流式 + SSE；默认关（透传流式）
 *   DEBUG_CACHE         "1" = 打日志
 *   PORT                默认 8000
 */

const PROVIDER = "xyc";
const DEFAULT_UPSTREAM = "https://cn.chatapi.app";
const TTL = "1h";
const BETA_FLAG = "extended-cache-ttl-2025-04-11";
const MAX_BREAKPOINTS = 4;
const MIN_CHARS = 2000;
const RUNTIME_MARKER = "xyc-proxy-runtime-time-v2";
const TIME_LINE_RE =
  /(?:^|\n)[ \t]*当前北京时间：\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?\s*[（(]Asia\/Shanghai[）)][ \t]*/;

// deno-lint-ignore no-explicit-any
type Any = any;

const UPSTREAM = (Deno.env.get("UPSTREAM_URL") || DEFAULT_UPSTREAM).replace(/\/+$/, "");
const PROXY_TOKEN = Deno.env.get("PROXY_TOKEN") || "";
const CACHE_ENABLED = Deno.env.get("CACHE_TTL_ON") !== "0";
const TIME_ENABLED = Deno.env.get("INJECT_CURRENT_TIME") !== "0";
const TIME_ZONE = Deno.env.get("TIME_ZONE") || "Asia/Shanghai";
const FORCE_NON_STREAM = Deno.env.get("FORCE_NON_STREAM") === "1";
const DEBUG_CACHE = Deno.env.get("DEBUG_CACHE") === "1";
const parsedPort = Number(Deno.env.get("PORT") || "8000");
const PORT = Number.isFinite(parsedPort) && parsedPort > 0 ? Math.trunc(parsedPort) : 8000;

const CACHEABLE = new Set([
  "text",
  "image",
  "document",
  "tool_use",
  "tool_result",
  "search_result",
]);

const CORS: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers":
    "content-type, x-proxy-token, x-api-key, authorization, anthropic-version, anthropic-beta, anthropic-dangerous-direct-browser-access",
  "access-control-expose-headers": "x-proxy-request-id",
  "access-control-max-age": "86400",
};

const STRIP = [
  "host",
  "connection",
  "content-length",
  "transfer-encoding",
  "keep-alive",
  "upgrade",
  "expect",
  "accept-encoding",
  "x-proxy-token",
  "x-proxy-request-id",
  "x-forwarded-for",
  "x-forwarded-host",
  "x-forwarded-proto",
];

let sequence = 0;

function isObj(v: unknown): v is Record<string, Any> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function json(data: unknown, status = 200, id = ""): Response {
  const headers = new Headers({ ...CORS, "content-type": "application/json; charset=utf-8" });
  if (id) headers.set("x-proxy-request-id", id);
  return new Response(JSON.stringify(data), { status, headers });
}

function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

function requestId(): string {
  return `${new Date().toISOString().replace(/[-:.]/g, "")}-${++sequence}-${crypto.randomUUID().slice(0, 8)}`;
}

function normalizePath(p: string): string {
  return p.replace(/\/+$/, "") || "/";
}

function isMessages(p: string): boolean {
  return p === "/v1/messages" || p === "/messages";
}

function isChat(p: string): boolean {
  return p === "/v1/chat/completions" || p === "/chat/completions";
}

function cc() {
  return { type: "ephemeral", ttl: TTL };
}

function formatTime(date = new Date()): string {
  try {
    return new Intl.DateTimeFormat("sv-SE", {
      timeZone: TIME_ZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }).format(date);
  } catch {
    return date.toISOString();
  }
}

function approxChars(body: Any): number {
  let n = 0;
  const walk = (v: unknown) => {
    if (typeof v === "string") n += v.length;
    else if (Array.isArray(v)) for (const x of v) walk(x);
    else if (isObj(v)) for (const x of Object.values(v)) walk(x);
  };
  walk(body?.system);
  walk(body?.messages);
  walk(body?.tools);
  return n;
}

function holders(body: Any): Record<string, Any>[] {
  const out: Record<string, Any>[] = [];
  const walk = (v: unknown) => {
    if (Array.isArray(v)) {
      for (const x of v) walk(x);
      return;
    }
    if (!isObj(v)) return;
    if (isObj(v.cache_control)) out.push(v);
    for (const x of Object.values(v)) walk(x);
  };
  walk(body?.system);
  walk(body?.messages);
  walk(body?.tools);
  return out;
}

function stripCache(body: Any): void {
  for (const h of holders(body)) delete h.cache_control;
}

function toBlocks(v: unknown): Record<string, Any>[] | null {
  if (typeof v === "string") return v.trim() === "" ? null : [{ type: "text", text: v }];
  if (Array.isArray(v)) {
    const b = v.filter(isObj);
    return b.length ? b : null;
  }
  return null;
}

function lastCacheable(blocks: Record<string, Any>[]): Record<string, Any> | null {
  for (let i = blocks.length - 1; i >= 0; i--) {
    if (typeof blocks[i].type === "string" && CACHEABLE.has(blocks[i].type)) return blocks[i];
  }
  return null;
}

function extractTimeLine(text: string): string {
  const m = text.match(TIME_LINE_RE);
  if (!m) return text;
  let next = (text.slice(0, m.index) + text.slice((m.index ?? 0) + m[0].length)).replace(/\n{3,}/g, "\n\n");
  if (next.startsWith("\n")) next = next.slice(1);
  return next;
}

function isRuntimeTimeText(text: string): boolean {
  return text.includes(RUNTIME_MARKER) || TIME_LINE_RE.test(`\n${text}`);
}

/** 清掉提示词/历史里旧的北京时间，以及代理上一轮追加的时间消息 */
function stripOldTime(body: Any): void {
  if (typeof body.system === "string") {
    body.system = extractTimeLine(body.system);
  } else if (Array.isArray(body.system)) {
    for (const block of body.system) {
      if (isObj(block) && typeof block.text === "string") {
        block.text = extractTimeLine(block.text);
      }
    }
    body.system = body.system.filter((b: unknown) => {
      if (!isObj(b)) return true;
      if (b.type === "text" && typeof b.text === "string" && b.text.trim() === "") return false;
      return true;
    });
  }

  if (!Array.isArray(body.messages)) return;
  body.messages = body.messages.filter((msg: unknown) => {
    if (!isObj(msg)) return true;

    if (typeof msg.content === "string") {
      if (msg.content.includes(RUNTIME_MARKER)) return false;
      msg.content = extractTimeLine(msg.content);
      return msg.content.trim() !== "";
    }

    if (!Array.isArray(msg.content)) return true;
    msg.content = msg.content.filter((block: unknown) => {
      if (!isObj(block) || typeof block.text !== "string") return true;
      if (block.text.includes(RUNTIME_MARKER)) return false;
      block.text = extractTimeLine(block.text);
      if (block.type === "text" && block.text.trim() === "") return false;
      return true;
    });
    return msg.content.length > 0;
  });
}

function normalizeAnthropicMessages(body: Any): void {
  if (!Array.isArray(body?.messages)) return;
  for (const msg of body.messages) {
    if (!isObj(msg)) continue;
    if (typeof msg.content === "string") {
      if (msg.content.trim() === "") continue;
      msg.content = [{ type: "text", text: msg.content }];
    }
  }
}

function injectBreakpoints(body: Any): void {
  stripCache(body);
  if (approxChars(body) < MIN_CHARS) return;

  let budget = MAX_BREAKPOINTS;
  const mark = (h: Record<string, Any>) => {
    if (budget <= 0 || isObj(h.cache_control)) return;
    h.cache_control = cc();
    budget--;
  };

  if (Array.isArray(body.tools) && body.tools.length > 0) {
    const tool = body.tools.filter(isObj).at(-1);
    if (tool) mark(tool);
  }

  if (body.system !== undefined) {
    const blocks = toBlocks(body.system);
    const target = blocks && lastCacheable(blocks);
    if (blocks && target) {
      body.system = blocks;
      mark(target);
    }
  }

  if (Array.isArray(body.messages) && body.messages.length > 0) {
    let placed = 0;
    for (let i = body.messages.length - 1; i >= 0 && placed < 2 && budget > 0; i--) {
      const msg = body.messages[i];
      if (!isObj(msg)) continue;
      const blocks = toBlocks(msg.content);
      const target = blocks && lastCacheable(blocks);
      if (!blocks || !target || isObj(target.cache_control)) continue;
      msg.content = blocks;
      mark(target);
      placed++;
    }
  }
}

function appendRuntimeTime(body: Any): void {
  if (!Array.isArray(body.messages)) body.messages = [];
  const line =
    `<!-- ${RUNTIME_MARKER} -->\n` +
    `当前北京时间：${formatTime()}（${TIME_ZONE}）`;
  body.messages.push({
    role: "user",
    content: [{ type: "text", text: line }],
  });
}

function injectOpenAI(body: Any): void {
  stripCache(body);
  if (approxChars(body) < MIN_CHARS || !Array.isArray(body.messages)) return;
  for (const msg of body.messages) {
    if (!isObj(msg) || msg.role !== "system") continue;
    if (Array.isArray(msg.content)) {
      const t = lastCacheable(msg.content.filter(isObj));
      if (t) t.cache_control = cc();
    } else if (typeof msg.content === "string") {
      msg.cache_control = cc();
    }
  }
}

function mergeBeta(current: string | null): string {
  const parts = (current ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  if (!parts.includes(BETA_FLAG)) parts.push(BETA_FLAG);
  return parts.join(",");
}

function responseHeaders(source: Headers, id: string): Headers {
  const out = new Headers(source);
  out.delete("content-encoding");
  out.delete("content-length");
  out.delete("transfer-encoding");
  out.delete("connection");
  for (const [k, v] of Object.entries(CORS)) out.set(k, v);
  out.set("x-proxy-request-id", id);
  return out;
}

function sseFrame(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}`;
}

function toSse(text: string): string {
  let parsed: Any;
  try {
    parsed = JSON.parse(text);
  } catch {
    return `${sseFrame("error", { type: "error", error: { type: "parse_error", message: text.slice(0, 500) } })}\n\n`;
  }
  if (!isObj(parsed)) {
    return `${sseFrame("error", { type: "error", error: { type: "invalid_response", message: text.slice(0, 500) } })}\n\n`;
  }
  if (isObj(parsed.error)) {
    return `${sseFrame("error", { type: "error", error: parsed.error })}\n\n`;
  }

  const events: string[] = [];
  events.push(sseFrame("message_start", { type: "message_start", message: { ...parsed, content: [] } }));

  const blocks = Array.isArray(parsed.content) ? parsed.content : [];
  blocks.forEach((block: unknown, index: number) => {
    if (!isObj(block)) return;
    const start: Any = { type: "content_block_start", index, content_block: { ...block } };
    if (block.type === "text") start.content_block.text = "";
    if (block.type === "tool_use") start.content_block.input = undefined;
    events.push(sseFrame("content_block_start", start));

    if (block.type === "text" && typeof block.text === "string") {
      events.push(sseFrame("content_block_delta", {
        type: "content_block_delta",
        index,
        delta: { type: "text_delta", text: block.text },
      }));
    } else if (block.type === "thinking" && typeof block.thinking === "string") {
      events.push(sseFrame("content_block_delta", {
        type: "content_block_delta",
        index,
        delta: { type: "thinking_delta", thinking: block.thinking },
      }));
    } else if (block.type === "tool_use") {
      events.push(sseFrame("content_block_delta", {
        type: "content_block_delta",
        index,
        delta: { type: "input_json_delta", partial_json: JSON.stringify(block.input ?? {}) },
      }));
    }
    events.push(sseFrame("content_block_stop", { type: "content_block_stop", index }));
  });

  events.push(sseFrame("message_delta", {
    type: "message_delta",
    delta: { stop_reason: parsed.stop_reason ?? null, stop_sequence: parsed.stop_sequence ?? null },
    usage: parsed.usage ?? {},
  }));
  events.push(sseFrame("message_stop", { type: "message_stop" }));
  return events.join("\n\n") + "\n\n";
}

async function forwardOnce(
  method: string,
  target: string,
  headers: Headers,
  body: BodyInit | null,
  meta: { id: string; convertSse?: boolean },
): Promise<Response> {
  let upstream: Response;
  try {
    upstream = await fetch(target, { method, headers, body });
  } catch (e) {
    return json({ error: `upstream error: ${e instanceof Error ? e.message : String(e)}` }, 502, meta.id);
  }

  if (meta.convertSse) {
    let text = "";
    try {
      text = await new Response(upstream.body).text();
    } catch (e) {
      return json({ error: `upstream read error: ${e instanceof Error ? e.message : String(e)}` }, 502, meta.id);
    }
    const out = responseHeaders(upstream.headers, meta.id);
    out.set("content-type", "text/event-stream; charset=utf-8");
    return new Response(toSse(text), { status: upstream.status, headers: out });
  }

  const out = responseHeaders(upstream.headers, meta.id);
  return new Response(upstream.body, { status: upstream.status, headers: out });
}

async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const path = normalizePath(url.pathname);

  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  if (PROXY_TOKEN) {
    const supplied = req.headers.get("x-proxy-token") || url.searchParams.get("proxy_token") || "";
    if (!safeEqual(supplied, PROXY_TOKEN)) return json({ error: "unauthorized" }, 401);
  }
  url.searchParams.delete("proxy_token");

  if (req.method === "GET" && (path === "/" || path === "/health")) {
    return json({
      ok: true,
      provider: PROVIDER,
      upstream: UPSTREAM,
      cache: CACHE_ENABLED ? "1h/tools+system+history" : "passthrough",
      beta: CACHE_ENABLED ? BETA_FLAG : "not-added",
      timeInjection: TIME_ENABLED ? "last-user-after-breakpoints" : "off",
      timeZone: TIME_ZONE,
      forceNonStream: FORCE_NON_STREAM,
      stream: FORCE_NON_STREAM ? "buffered-sse" : "passthrough",
    });
  }

  const id = requestId();
  const target = UPSTREAM + path + url.search;
  const headers = new Headers(req.headers);
  for (const h of STRIP) headers.delete(h);
  headers.set("x-proxy-request-id", id);

  let inbound: Uint8Array | null;
  try {
    inbound = req.body ? new Uint8Array(await req.arrayBuffer()) : null;
  } catch (e) {
    return json({ error: `request body read error: ${e instanceof Error ? e.message : String(e)}` }, 400, id);
  }

  const rewrite = req.method === "POST" && (isMessages(path) || isChat(path));
  if (!rewrite) {
    return await forwardOnce(req.method, target, headers, inbound ? inbound.buffer : null, { id });
  }

  let body: Any;
  try {
    body = JSON.parse(new TextDecoder().decode(inbound ?? new Uint8Array()));
  } catch {
    return json({ error: "bad json body" }, 400, id);
  }

  if (CACHE_ENABLED && isMessages(path)) {
    stripOldTime(body);
    normalizeAnthropicMessages(body);
    injectBreakpoints(body);
    if (TIME_ENABLED) appendRuntimeTime(body);
    headers.set("anthropic-beta", mergeBeta(headers.get("anthropic-beta")));

    if (DEBUG_CACHE) {
      const last = body.messages?.at?.(-1);
      console.log("DEBUG breakpoints:", holders(body).length);
      console.log("DEBUG lastRole:", last?.role);
      console.log("DEBUG lastIsTime:", JSON.stringify(last ?? "").includes(RUNTIME_MARKER));
      console.log("DEBUG sysHasTime:", JSON.stringify(body.system ?? "").includes("当前北京时间"));
      console.log("DEBUG stream:", body?.stream === true);
    }
  } else if (CACHE_ENABLED && isChat(path)) {
    injectOpenAI(body);
  }

  let convertSse = false;
  if (FORCE_NON_STREAM && isMessages(path) && body?.stream === true) {
    body.stream = false;
    convertSse = true;
  }

  headers.set("content-type", "application/json");
  return await forwardOnce(
    "POST",
    target,
    headers,
    new TextEncoder().encode(JSON.stringify(body)),
    { id, convertSse },
  );
}

Deno.serve({ port: PORT, onListen: () => {} }, handler);
