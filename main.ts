/**
 * xyc-proxy v5 — LobeHub → XYC Claude 1h prompt cache + full diagnostics
 *
 * 1h 三件套：
 *   1. anthropic-beta: extended-cache-ttl-2025-04-11
 *   2. cache_control: { type: "ephemeral", ttl: "1h" }
 *   3. 断点前前缀够长
 *
 * 最多 4 个断点：清掉 LobeHub 5m → tools 最后一项 → system 最后一块 → messages 从后往前。
 * 时间块加在最后一条 user 的断点之后，本身不带 cache_control。
 *
 * 日志默认全开（不依赖 DEBUG）：
 *   GET /logs        文本
 *   GET /logs.json   JSON
 *   GET|POST /logs/clear
 * 有 PROXY_TOKEN 时：/logs?proxy_token=...
 *
 * 环境变量：
 *   UPSTREAM_URL        默认 https://apicdn.xyc.ai
 *   PROXY_TOKEN         可选
 *   CACHE_TTL_ON        "0" = 不改 cache；默认开
 *   INJECT_CURRENT_TIME "0" = 不注入时间；默认开
 *   TIME_ZONE           默认 Asia/Shanghai
 *   FORCE_NON_STREAM    "1" = 强制非流式再转 SSE
 *   CACHE_TTL           "5m"(默认) | "1h"；5m 用原生 ephemeral(不发 extended beta)，1h 发 beta + ttl:1h
 *   LOG_BODY            "0" = 不记改写后请求体；默认记（图片/超长文本会裁）
 *   DEBUG               "1" = 成功请求也打 console，且请求体文本裁得更长
 *   PORT                Zeabur 注入；本地默认 8080
 *
 * v7: 前缀稳定性按 system 分组统计 —— canon(语义) / exact(字节) 双维度、
 *     commonMsgPrefix / firstDiffMsg 精确定位漂移消息；保留 v5 全部修复。
 */

const PROVIDER = "xyc";
const VERSION = "v7-nobeta";
const DEFAULT_UPSTREAM = "https://apicdn.xyc.ai";
const CACHE_TTL = (Deno.env.get("CACHE_TTL") || "5m").toLowerCase() === "1h" ? "1h" : "5m";
const TTL = CACHE_TTL;
const BETA_FLAG = TTL === "1h" ? "extended-cache-ttl-2025-04-11" : "";
const MAX_BREAKPOINTS = 4;
const MIN_CHARS = 1500;
const MAX_LOGS = 250;
const RAW_LIMIT = 2000;
const RUNTIME_MARKERS = [
  "xyc-proxy-runtime-time-v1",
  "xyc-proxy-runtime-time-v2",
  "xyc-proxy-runtime-time-v5",
];
const RUNTIME_MARKER = RUNTIME_MARKERS[2];
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
const DEBUG = Deno.env.get("DEBUG") === "1";
const LOG_BODY = Deno.env.get("LOG_BODY") !== "0";
const parsedPort = Number(Deno.env.get("PORT") || "8080");
const PORT = Number.isFinite(parsedPort) && parsedPort > 0 ? Math.trunc(parsedPort) : 8080;
const TEXT_KEEP = DEBUG ? 12000 : 4000;

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

interface LogRec {
  ts: string;
  id: string;
  method: string;
  path: string;
  model: string;
  streamIn: boolean;
  streamOut: boolean;
  convertSse: boolean;
  inboundBeta: string;
  outboundBeta: string;
  anthropicVersion: string;
  inboundBp: string;
  outboundBp: string;
  applied: string;
  timeAdded: string;
  chars: number;
  tools: number;
  roles: string;
  sysHash: string;
  toolsHash: string;
  msgHashes: string;
  prefixHash: string;
  samePrefixAsPrev: boolean | null;
  hasPrev: boolean | null;
  sameToolsAsPrev: boolean | null;
  msgHashesCanon: string;
  commonMsgPrefix: number;
  firstDiffMsg: number;
  prefixStableCanon: boolean | null;
  prefixStableExact: boolean | null;
  hasXApiKey: boolean;
  hasAuthorization: boolean;
  status?: number;
  ms?: number;
  respLen?: number;
  usage?: Record<string, string>;
  raw?: string;
  error?: string;
  body?: unknown;
}

const LOGS: LogRec[] = [];
let sequence = 0;
let lastPrefixHash: string | null = null;
// Per-system (≈ per-agent conversation) last-seen state for prefix-stability.
const lastBySystem = new Map<string, { toolsHash: string; msgsExact: string[]; msgsCanon: string[] }>();

function isObj(v: unknown): v is Record<string, Any> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function json(data: unknown, status = 200, id = ""): Response {
  const headers = new Headers({ ...CORS, "content-type": "application/json; charset=utf-8" });
  if (id) headers.set("x-proxy-request-id", id);
  return new Response(JSON.stringify(data, null, 2), { status, headers });
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
  return TTL === "1h" ? { type: "ephemeral", ttl: "1h" } : { type: "ephemeral" };
}

function formatTime(date = new Date(), withSeconds = false): string {
  try {
    const options: Intl.DateTimeFormatOptions = {
      timeZone: TIME_ZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    };
    if (withSeconds) options.second = "2-digit";
    return new Intl.DateTimeFormat("sv-SE", options).format(date);
  } catch {
    return date.toISOString();
  }
}

function clock(): string {
  return formatTime(new Date(), true).replace("T", " ");
}

function pushLog(rec: LogRec): void {
  LOGS.push(rec);
  while (LOGS.length > MAX_LOGS) LOGS.shift();
  const line = formatLog(rec);
  if (DEBUG || rec.error || (rec.status !== undefined && rec.status >= 400)) {
    console.log(line);
  } else {
    console.log(`${rec.ts} ${rec.id} ${rec.path} ${rec.model} ${rec.status ?? "-"} ${usageLine(rec)}`);
  }
}

function usageLine(rec: LogRec): string {
  const u = rec.usage ?? {};
  return `read=${u.cache_read_input_tokens ?? "-"} create=${u.cache_creation_input_tokens ?? "-"} w1h=${u.ephemeral_1h_input_tokens ?? "-"} w5m=${u.ephemeral_5m_input_tokens ?? "-"} in=${u.input_tokens ?? "-"} out=${u.output_tokens ?? "-"}`;
}

function formatLog(rec: LogRec): string {
  const lines = [
    `${rec.ts} ${rec.id} ${rec.method} ${rec.path}`,
    `  model=${rec.model} streamIn=${rec.streamIn} streamOut=${rec.streamOut} convertSse=${rec.convertSse}`,
    `  betaIn=${rec.inboundBeta || "-"} betaOut=${rec.outboundBeta || "-"} ver=${rec.anthropicVersion || "-"}`,
    `  auth x-api-key=${rec.hasXApiKey} authorization=${rec.hasAuthorization}`,
    `  inboundBp=${rec.inboundBp}`,
    `  outboundBp=${rec.outboundBp}`,
    `  applied=${rec.applied} time=${rec.timeAdded} chars=${rec.chars} tools=${rec.tools} roles=${rec.roles}`,
    `  sysHash=${rec.sysHash} toolsHash=${rec.toolsHash}`,
    `  msgHashes=${rec.msgHashes}`,
    `  prefixHash=${rec.prefixHash} samePrefixAsPrev=${rec.samePrefixAsPrev}`,
    `  stability hasPrev=${rec.hasPrev} sameTools=${rec.sameToolsAsPrev} commonMsgPrefix=${rec.commonMsgPrefix} firstDiffMsg=${rec.firstDiffMsg}`,
    `  prefixStable canon=${rec.prefixStableCanon} exact=${rec.prefixStableExact} msgHashesCanon=${rec.msgHashesCanon}`,
  ];
  if (rec.status !== undefined) {
    lines.push(`  status=${rec.status} ms=${rec.ms ?? "-"} respLen=${rec.respLen ?? "-"} ${usageLine(rec)}`);
  }
  if (rec.usage) {
    lines.push(`  usageJson=${JSON.stringify(rec.usage)}`);
  }
  if (rec.error) lines.push(`  error=${rec.error}`);
  if (rec.raw) lines.push(`  RAW <${rec.raw}>`);
  if (rec.body !== undefined) {
    try {
      lines.push(`  BODY ${JSON.stringify(rec.body)}`);
    } catch {
      lines.push("  BODY <unserializable>");
    }
  }
  return lines.join("\n");
}

function shortHash(v: unknown): string {
  let s: string;
  try {
    s = typeof v === "string" ? v : JSON.stringify(v ?? null);
  } catch {
    s = String(v);
  }
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) | 0;
  return `${(h >>> 0).toString(16).padStart(8, "0")}/${s.length}`;
}

function isRuntimeText(text: string): boolean {
  return RUNTIME_MARKERS.some((m) => text.includes(m)) || TIME_LINE_RE.test(`\n${text}`);
}

function diagnosticValue(v: unknown): unknown {
  if (Array.isArray(v)) {
    return v
      .filter((item) =>
        !(isObj(item) && item.type === "text" && typeof item.text === "string" && isRuntimeText(item.text))
      )
      .map(diagnosticValue);
  }
  if (!isObj(v)) return v;
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v)) {
    if (k === "cache_control") continue;
    out[k] = diagnosticValue(val);
  }
  return out;
}

function diagnosticHash(v: unknown): string {
  return shortHash(diagnosticValue(v));
}

function diagnosticValueCanon(v: unknown): unknown {
  if (Array.isArray(v)) {
    return v
      .filter((item) =>
        !(isObj(item) && item.type === "text" && typeof item.text === "string" && isRuntimeText(item.text))
      )
      .map(diagnosticValueCanon);
  }
  if (!isObj(v)) return v;
  const out: Record<string, unknown> = {};
  for (const k of Object.keys(v).sort()) {
    if (k === "cache_control") continue;
    out[k] = diagnosticValueCanon(v[k]);
  }
  return out;
}

function diagnosticHashCanon(v: unknown): string {
  return shortHash(diagnosticValueCanon(v));
}

function messageHashes(body: Any): string {
  if (!Array.isArray(body?.messages)) return "-";
  return body.messages.map((msg: unknown, i: number) => {
    if (!isObj(msg)) return `${i}?:invalid`;
    const role = String(msg.role ?? "?").slice(0, 1);
    return `${i}${role}:${diagnosticHash(msg.content)}`;
  }).join(",");
}

function messageHashesCanon(body: Any): string {
  if (!Array.isArray(body?.messages)) return "-";
  return body.messages.map((msg: unknown, i: number) => {
    if (!isObj(msg)) return `${i}?:invalid`;
    const role = String(msg.role ?? "?").slice(0, 1);
    return `${i}${role}:${diagnosticHashCanon(msg.content)}`;
  }).join(",");
}

function rolesOf(body: Any): string {
  if (!Array.isArray(body?.messages)) return "-";
  return body.messages.map((msg: unknown) => isObj(msg) ? String(msg.role ?? "?").slice(0, 1) : "?").join("");
}

function prefixHashOf(body: Any): string {
  return diagnosticHash({ tools: body?.tools ?? null, system: body?.system ?? null, messages: body?.messages ?? null });
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
  if (isObj(body) && isObj(body.cache_control)) out.push(body);
  return out;
}

function stripCache(body: Any): number {
  const found = holders(body);
  for (const h of found) delete h.cache_control;
  return found.length;
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

function stripOldTime(body: Any): void {
  if (typeof body.system === "string") {
    body.system = extractTimeLine(body.system);
  } else if (Array.isArray(body.system)) {
    for (const block of body.system) {
      if (isObj(block) && typeof block.text === "string") block.text = extractTimeLine(block.text);
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
      if (RUNTIME_MARKERS.some((m) => msg.content.includes(m))) return false;
      msg.content = extractTimeLine(msg.content);
      return msg.content.trim() !== "";
    }
    if (!Array.isArray(msg.content)) return true;
    msg.content = msg.content.filter((block: unknown) => {
      if (!isObj(block) || typeof block.text !== "string") return true;
      if (RUNTIME_MARKERS.some((m) => block.text.includes(m))) return false;
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
    if (typeof msg.content === "string" && msg.content.trim() !== "") {
      msg.content = [{ type: "text", text: msg.content }];
    }
  }
}

function injectBreakpoints(body: Any): { applied: string[]; skipped?: string } {
  const removed = stripCache(body);
  const applied: string[] = [];
  if (removed > 0) applied.push(`removed5m:${removed}`);
  if (approxChars(body) < MIN_CHARS) {
    return { applied, skipped: `too-small:${approxChars(body)}<${MIN_CHARS}` };
  }
  let budget = MAX_BREAKPOINTS;
  const mark = (h: Record<string, Any>, label: string) => {
    if (budget <= 0 || isObj(h.cache_control)) return;
    h.cache_control = cc();
    applied.push(label);
    budget--;
  };
  if (Array.isArray(body.tools) && body.tools.length > 0) {
    const tool = body.tools.filter(isObj).at(-1);
    if (tool) mark(tool, `tools[${body.tools.length - 1}]`);
  }
  if (body.system !== undefined) {
    const blocks = toBlocks(body.system);
    const target = blocks && lastCacheable(blocks);
    if (blocks && target) {
      body.system = blocks;
      mark(target, "system");
    }
  }
  if (Array.isArray(body.messages)) {
    for (let i = body.messages.length - 1; i >= 0 && budget > 0; i--) {
      const msg = body.messages[i];
      if (!isObj(msg)) continue;
      const blocks = toBlocks(msg.content);
      const target = blocks && lastCacheable(blocks);
      if (!blocks || !target || isObj(target.cache_control)) continue;
      msg.content = blocks;
      mark(target, `msg[${i}]:${msg.role ?? "?"}`);
    }
  }
  if (applied.filter((x) => !x.startsWith("removed")).length === 0) {
    return { applied, skipped: "no-cacheable-block" };
  }
  return { applied };
}

function appendRuntimeTime(body: Any): { added: boolean; reason?: string } {
  if (!Array.isArray(body?.messages) || body.messages.length === 0) {
    return { added: false, reason: "no-messages" };
  }
  const last = body.messages.at(-1);
  if (!isObj(last) || last.role !== "user") return { added: false, reason: "last-not-user" };
  if (typeof last.content === "string") last.content = [{ type: "text", text: last.content }];
  if (!Array.isArray(last.content)) return { added: false, reason: "unsupported-content" };
  last.content.push({
    type: "text",
    text: `<!-- ${RUNTIME_MARKER} -->\n当前北京时间：${formatTime()}（${TIME_ZONE}）`,
  });
  return { added: true };
}

function injectOpenAI(body: Any): { applied: string[]; skipped?: string } {
  const removed = stripCache(body);
  const applied: string[] = [];
  if (removed > 0) applied.push(`removed:${removed}`);
  if (approxChars(body) < MIN_CHARS) return { applied, skipped: "too-small" };
  if (!Array.isArray(body.messages)) return { applied, skipped: "no-messages" };
  for (const msg of body.messages) {
    if (!isObj(msg) || msg.role !== "system") continue;
    if (Array.isArray(msg.content)) {
      const t = lastCacheable(msg.content.filter(isObj));
      if (t) {
        t.cache_control = cc();
        applied.push("system-block");
      }
    } else if (typeof msg.content === "string") {
      msg.cache_control = cc();
      applied.push("system-message");
    }
  }
  return { applied };
}

function mergeBeta(current: string | null): string {
  const parts = (current ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  if (!parts.includes(BETA_FLAG)) parts.push(BETA_FLAG);
  return parts.join(",");
}

function scanBreakpoints(body: Any): string {
  const found: string[] = [];
  const ttlOf = (v: unknown): string | null => {
    if (!isObj(v) || !isObj(v.cache_control)) return null;
    const ttl = v.cache_control.ttl;
    const type = v.cache_control.type ?? "ephemeral";
    return `${type}:${ttl ?? "5m"}`;
  };
  if (isObj(body) && ttlOf(body)) found.push(`top:${ttlOf(body)}`);
  if (Array.isArray(body?.tools)) {
    body.tools.forEach((tool: unknown, i: number) => {
      const ttl = ttlOf(tool);
      if (ttl) found.push(`tool${i}:${ttl}`);
    });
  }
  if (Array.isArray(body?.system)) {
    body.system.forEach((block: unknown, i: number) => {
      const ttl = ttlOf(block);
      if (ttl) found.push(`sys${i}:${ttl}`);
    });
  } else if (typeof body?.system === "string" && isObj(body) && ttlOf(body)) {
    found.push(`sys:${ttlOf(body)}`);
  }
  if (Array.isArray(body?.messages)) {
    body.messages.forEach((msg: unknown, i: number) => {
      if (!isObj(msg)) return;
      const msgTtl = ttlOf(msg);
      if (msgTtl) found.push(`msg${i}:${msgTtl}`);
      if (!Array.isArray(msg.content)) return;
      msg.content.forEach((block: unknown, j: number) => {
        const ttl = ttlOf(block);
        if (ttl) found.push(`msg${i}.${j}:${ttl}`);
      });
    });
  }
  return found.length === 0 ? "none" : `${found.length}[${found.join(",")}]`;
}

function clipText(text: string): string {
  if (text.length <= TEXT_KEEP) return text;
  return `${text.slice(0, TEXT_KEEP)}…[len=${text.length}]`;
}

function sanitizeForLog(v: unknown): unknown {
  if (typeof v === "string") return clipText(v);
  if (Array.isArray(v)) return v.map(sanitizeForLog);
  if (!isObj(v)) return v;
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v)) {
    if ((k === "data" || k === "bytes" || k === "source") && typeof val === "string" && val.length > 80) {
      out[k] = `<blob ${shortHash(val)}>`;
    } else if (k === "source" && isObj(val) && typeof val.data === "string") {
      out[k] = { ...val, data: `<blob ${shortHash(val.data)}>` };
    } else {
      out[k] = sanitizeForLog(val);
    }
  }
  return out;
}

function extractUsage(text: string): Record<string, string> {
  const keys = [
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "ephemeral_1h_input_tokens",
    "ephemeral_5m_input_tokens",
    "input_tokens",
    "output_tokens",
    "thinking_tokens",
    "cache_creation",
  ];
  const usage: Record<string, string> = {};
  for (const key of keys) {
    if (key === "cache_creation") continue;
    const pattern = new RegExp(`"${key}"\\s*:\\s*(\\d+)`, "g");
    let value = "";
    for (const match of text.matchAll(pattern)) value = match[1];
    if (value) usage[key] = value;
  }
  const stop = text.match(/"stop_reason"\s*:\s*"([^"]*)"/);
  if (stop) usage.stop_reason = stop[1];
  return usage;
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

function finishLog(rec: LogRec, status: number, ms: number, text: string, ok: boolean): void {
  rec.status = status;
  rec.ms = ms;
  rec.respLen = text.length;
  rec.usage = extractUsage(text);
  const hasUsage = Boolean(rec.usage && (rec.usage.input_tokens || rec.usage.cache_read_input_tokens || rec.usage.cache_creation_input_tokens));
  if (!ok || !hasUsage) {
    rec.raw = text.slice(0, RAW_LIMIT).replace(/\s+/g, " ");
  }
  pushLog(rec);
}

async function forwardOnce(
  method: string,
  target: string,
  headers: Headers,
  body: BodyInit | null,
  rec: LogRec,
  convertSse: boolean,
): Promise<Response> {
  let upstream: Response;
  const started = performance.now();
  try {
    upstream = await fetch(target, { method, headers, body });
  } catch (e) {
    rec.error = `upstream fetch: ${e instanceof Error ? e.message : String(e)}`;
    rec.status = 502;
    rec.ms = Math.round(performance.now() - started);
    pushLog(rec);
    return json({ error: rec.error, id: rec.id }, 502, rec.id);
  }
  const elapsed = Math.round(performance.now() - started);

  if (convertSse) {
    let text = "";
    try {
      text = await new Response(upstream.body).text();
    } catch (e) {
      rec.error = `upstream read: ${e instanceof Error ? e.message : String(e)}`;
      rec.status = 502;
      rec.ms = elapsed;
      pushLog(rec);
      return json({ error: rec.error, id: rec.id }, 502, rec.id);
    }
    finishLog(rec, upstream.status, elapsed, text, upstream.ok);
    const out = responseHeaders(upstream.headers, rec.id);
    out.set("content-type", "text/event-stream; charset=utf-8");
    return new Response(toSse(text), { status: upstream.status, headers: out });
  }

  const out = responseHeaders(upstream.headers, rec.id);
  if (!upstream.body) {
    rec.status = upstream.status;
    rec.ms = elapsed;
    rec.error = "no-body";
    pushLog(rec);
    return new Response(null, { status: upstream.status, headers: out });
  }
  const [toClient, toLog] = upstream.body.tee();
  void (async () => {
    try {
      const text = await new Response(toLog).text();
      finishLog(rec, upstream.status, elapsed, text, upstream.ok);
    } catch (e) {
      rec.error = `usage-log: ${e instanceof Error ? e.message : String(e)}`;
      rec.status = upstream.status;
      rec.ms = elapsed;
      pushLog(rec);
    }
  })();
  return new Response(toClient, { status: upstream.status, headers: out });
}

function logsAuthorized(req: Request, url: URL): boolean {
  if (!PROXY_TOKEN) return true;
  const supplied = req.headers.get("x-proxy-token") || url.searchParams.get("proxy_token") || "";
  return safeEqual(supplied, PROXY_TOKEN);
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
      version: VERSION,
      upstream: UPSTREAM,
      cache: CACHE_ENABLED ? "1h/tools+system+messages" : "passthrough",
      beta: "not-sent",
      maxBreakpoints: MAX_BREAKPOINTS,
      cacheTtl: TTL,
      minChars: MIN_CHARS,
      timeInjection: TIME_ENABLED ? "last-user-block-after-breakpoint" : "off",
      timeZone: TIME_ZONE,
      forceNonStream: FORCE_NON_STREAM,
      logBody: LOG_BODY,
      debug: DEBUG,
      port: PORT,
      logsInThisInstance: LOGS.length,
      maxLogs: MAX_LOGS,
      note: "GET /logs  or  /logs.json  — instance memory, lost on redeploy",
    });
  }

  if (req.method === "GET" && path === "/logs") {
    const text = LOGS.length === 0 ? "No logs yet." : LOGS.map(formatLog).join("\n\n");
    return new Response(text, { headers: { ...CORS, "content-type": "text/plain; charset=utf-8" } });
  }

  if (req.method === "GET" && path === "/logs.json") {
    return json({ count: LOGS.length, lastPrefixHash, distinctSystems: lastBySystem.size, logs: LOGS });
  }

  if ((req.method === "GET" || req.method === "POST") && path === "/logs/clear") {
    LOGS.length = 0;
    lastPrefixHash = null;
    lastBySystem.clear();
    return new Response("Logs cleared.", { headers: { ...CORS, "content-type": "text/plain; charset=utf-8" } });
  }

  const id = requestId();
  const target = UPSTREAM + path + url.search;
  const headers = new Headers(req.headers);
  for (const h of STRIP) headers.delete(h);
  headers.set("x-proxy-request-id", id);

  const rec: LogRec = {
    ts: clock(),
    id,
    method: req.method,
    path,
    model: "?",
    streamIn: false,
    streamOut: false,
    convertSse: false,
    inboundBeta: req.headers.get("anthropic-beta") ?? "",
    outboundBeta: "",
    anthropicVersion: req.headers.get("anthropic-version") ?? "",
    inboundBp: "-",
    outboundBp: "-",
    applied: "passthrough",
    timeAdded: "off",
    chars: 0,
    tools: 0,
    roles: "-",
    sysHash: "-",
    toolsHash: "-",
    msgHashes: "-",
    prefixHash: "-",
    samePrefixAsPrev: null,
    hasPrev: null,
    sameToolsAsPrev: null,
    msgHashesCanon: "-",
    commonMsgPrefix: 0,
    firstDiffMsg: -1,
    prefixStableCanon: null,
    prefixStableExact: null,
    hasXApiKey: Boolean(req.headers.get("x-api-key")),
    hasAuthorization: Boolean(req.headers.get("authorization")),
  };

  let inbound: Uint8Array | null;
  try {
    inbound = req.body ? new Uint8Array(await req.arrayBuffer()) : null;
  } catch (e) {
    rec.error = `body read: ${e instanceof Error ? e.message : String(e)}`;
    rec.status = 400;
    pushLog(rec);
    return json({ error: rec.error, id }, 400, id);
  }

  const rewrite = req.method === "POST" && (isMessages(path) || isChat(path));
  if (!rewrite) {
    rec.outboundBeta = headers.get("anthropic-beta") ?? rec.inboundBeta;
    return await forwardOnce(req.method, target, headers, inbound as BodyInit, rec, false);
  }

  let body: Any;
  try {
    body = JSON.parse(new TextDecoder().decode(inbound ?? new Uint8Array()));
  } catch {
    rec.error = "bad json body";
    rec.status = 400;
    rec.raw = new TextDecoder().decode(inbound ?? new Uint8Array()).slice(0, RAW_LIMIT);
    pushLog(rec);
    return json({ error: "bad json body", id }, 400, id);
  }

  rec.model = typeof body?.model === "string" ? body.model : "?";
  rec.streamIn = body?.stream === true;
  rec.inboundBp = scanBreakpoints(body);
  rec.chars = approxChars(body);
  rec.tools = Array.isArray(body?.tools) ? body.tools.length : 0;
  rec.roles = rolesOf(body);

  if (CACHE_ENABLED && isMessages(path)) {
    stripOldTime(body);
    normalizeAnthropicMessages(body);
    const inj = injectBreakpoints(body);
    rec.applied = `${inj.applied.join(",") || "-"}${inj.skipped ? `|skip:${inj.skipped}` : ""}`;
    if (TIME_ENABLED) {
      const t = appendRuntimeTime(body);
      rec.timeAdded = t.added ? "yes" : `no:${t.reason ?? "?"}`;
    }
    // beta header intentionally NOT sent (legacy extended-cache-ttl beta obsolete per 2026 docs)
  } else if (CACHE_ENABLED && isChat(path)) {
    const inj = injectOpenAI(body);
    rec.applied = `${inj.applied.join(",") || "-"}${inj.skipped ? `|skip:${inj.skipped}` : ""}`;
  } else {
    rec.applied = "cache-off";
  }

  rec.outboundBp = scanBreakpoints(body);
  rec.outboundBeta = headers.get("anthropic-beta") ?? "";
  rec.sysHash = diagnosticHash(body?.system ?? null);
  rec.toolsHash = diagnosticHash(body?.tools ?? null);
  rec.msgHashes = messageHashes(body);
  rec.msgHashesCanon = messageHashesCanon(body);
  rec.prefixHash = prefixHashOf(body);
  const msgsExact = (body.messages ?? []).map((m: unknown, i: number) =>
    isObj(m) ? diagnosticHash(m.content) : `?${i}`
  );
  const msgsCanon = (body.messages ?? []).map((m: unknown, i: number) =>
    isObj(m) ? diagnosticHashCanon(m.content) : `?${i}`
  );
  const prevSys = lastBySystem.get(rec.sysHash);
  if (prevSys) {
    rec.hasPrev = true;
    rec.sameToolsAsPrev = prevSys.toolsHash === rec.toolsHash;
    const minLen = Math.min(prevSys.msgsExact.length, msgsExact.length);
    let commonExact = 0;
    while (commonExact < minLen && prevSys.msgsExact[commonExact] === msgsExact[commonExact]) commonExact++;
    let commonCanon = 0;
    while (commonCanon < minLen && prevSys.msgsCanon[commonCanon] === msgsCanon[commonCanon]) commonCanon++;
    rec.commonMsgPrefix = commonCanon;
    rec.firstDiffMsg = commonCanon < minLen ? commonCanon : -1;
    rec.prefixStableCanon = prevSys.msgsCanon.length > 0 && commonCanon >= prevSys.msgsCanon.length && rec.sameToolsAsPrev;
    rec.prefixStableExact = prevSys.msgsExact.length > 0 && commonExact >= prevSys.msgsExact.length && rec.sameToolsAsPrev;
  } else {
    rec.hasPrev = false;
    rec.sameToolsAsPrev = null;
    rec.commonMsgPrefix = 0;
    rec.firstDiffMsg = -1;
    rec.prefixStableCanon = null;
    rec.prefixStableExact = null;
  }
  lastBySystem.set(rec.sysHash, { toolsHash: rec.toolsHash, msgsExact, msgsCanon });
  rec.samePrefixAsPrev = rec.prefixStableExact;
  lastPrefixHash = rec.prefixHash; // display only
  rec.chars = approxChars(body);
  rec.roles = rolesOf(body);
  rec.tools = Array.isArray(body?.tools) ? body.tools.length : 0;

  if (FORCE_NON_STREAM && isMessages(path) && body?.stream === true) {
    body.stream = false;
    rec.convertSse = true;
  }
  rec.streamOut = body?.stream === true;

  if (LOG_BODY) rec.body = sanitizeForLog(body);

  headers.set("content-type", "application/json");
  return await forwardOnce(
    "POST",
    target,
    headers,
    new TextEncoder().encode(JSON.stringify(body)),
    rec,
    rec.convertSse,
  );
}

Deno.serve({ port: PORT, hostname: "0.0.0.0" }, handler);