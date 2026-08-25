/**
 * xyc relay v2 - Claude 1h prompt-cache relay
 * Single-file for Deno/Zeabur.
 *
 * LobeHub Anthropic Base URL:
 *   https://YOUR_PROJECT.deno.dev
 *
 * Protocol specifics (xyc):
 *   - Auth via x-api-key header (NOT Authorization: Bearer)
 *   - Requires anthropic-version header
 *   - 1h cache needs: anthropic-beta: extended-cache-ttl-2025-04-11
 *                     + cache_control {type:"ephemeral", ttl:"1h"} on prefix tail
 *                     + prefix >= ~1024 tokens
 *
 * Design for stable hits:
 *   - Breakpoints placed on system / tools / stable history tail (multi-breakpoint)
 *   - No current-time injection (removed entirely)
 *   - No message reordering (keeps tool_result order stable across turns)
 *   - Force non-stream by default, convert full JSON back to SSE
 */

const PROVIDER = "xyc";
const DEFAULT_UPSTREAM = "https://cn.chatapi.app";
const TTL = "1h";
const BETA_FLAG = "extended-cache-ttl-2025-04-11";
const MAX_BREAKPOINTS = 4;
const MIN_PREFIX_TOKENS_EST = 2048; // safety margin over ~1024 token requirement
const CACHEABLE_TYPES = new Set([
  "text",
  "image",
  "document",
  "tool_use",
  "tool_result",
  "search_result",
]);

// deno-lint-ignore no-explicit-any
type Any = any;

const UPSTREAM = (Deno.env.get("UPSTREAM_URL") || DEFAULT_UPSTREAM).replace(/\/+$/, "");
const PROXY_TOKEN = Deno.env.get("PROXY_TOKEN") || "";
const CACHE_ENABLED = Deno.env.get("CACHE_TTL_ON") !== "0";
const FORCE_NON_STREAM = Deno.env.get("FORCE_NON_STREAM") !== "0";
const parsedPort = Number(Deno.env.get("PORT") || "8000");
const PORT = Number.isFinite(parsedPort) && parsedPort > 0 ? Math.trunc(parsedPort) : 8000;

const CORS_HEADERS: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers":
    "content-type, x-proxy-token, x-api-key, authorization, anthropic-version, anthropic-beta, anthropic-dangerous-direct-browser-access",
  "access-control-expose-headers": "x-proxy-request-id",
  "access-control-max-age": "86400",
};

const STRIP_HEADERS = [
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

function json(data: unknown, status = 200, requestId = ""): Response {
  const headers = new Headers({
    ...CORS_HEADERS,
    "content-type": "application/json; charset=utf-8",
  });
  if (requestId) headers.set("x-proxy-request-id", requestId);
  return new Response(JSON.stringify(data), { status, headers });
}

function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

function requestId(): string {
  const stamp = new Date().toISOString().replace(/[-:.]/g, "");
  return `${stamp}-${++sequence}-${crypto.randomUUID().slice(0, 8)}`;
}

async function readRequestBody(req: Request): Promise<Uint8Array | null> {
  if (!req.body) return null;
  return new Uint8Array(await req.arrayBuffer());
}

function requestBodyInit(bytes: Uint8Array | null): ArrayBuffer | null {
  if (!bytes) return null;
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}

function normalizePath(pathname: string): string {
  return pathname.replace(/\/+$/, "") || "/";
}

function isMessagesPath(path: string): boolean {
  return path === "/v1/messages" || path === "/messages";
}

function isChatPath(path: string): boolean {
  return path === "/v1/chat/completions" || path === "/chat/completions";
}

function resolveUpstream(path: string): string {
  return UPSTREAM + path;
}

function cc() {
  return { type: "ephemeral", ttl: TTL };
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

function existingHolders(body: Any): Record<string, Any>[] {
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

function toBlocks(v: unknown): Record<string, Any>[] | null {
  if (typeof v === "string") {
    return v.trim() === "" ? null : [{ type: "text", text: v }];
  }
  if (Array.isArray(v)) {
    const blocks = v.filter(isObj);
    return blocks.length > 0 ? blocks : null;
  }
  return null;
}

function lastCacheable(blocks: Record<string, Any>[]): Record<string, Any> | null {
  for (let i = blocks.length - 1; i >= 0; i--) {
    const type = blocks[i].type;
    if (typeof type === "string" && CACHEABLE_TYPES.has(type)) return blocks[i];
  }
  return null;
}

/**
 * Multi-breakpoint injection: upgrade any existing breakpoints to 1h, then
 * top up tools / system / tail messages up to MAX_BREAKPOINTS.
 * Key: breakpoints go on STABLE parts (tools, system, history tail) — NOT on
 * the newest user message — so the prefix stays byte-identical between turns.
 */
function injectAnthropicAll(body: Any, tailBreakpoints: number): void {
  // 1. Normalize all existing cache_control to our TTL
  const holders = existingHolders(body);
  for (const holder of holders) {
    const old = holder.cache_control;
    if (old?.type !== "ephemeral" || old?.ttl !== TTL) {
      holder.cache_control = cc();
    }
  }

  if (approxChars(body) < MIN_PREFIX_TOKENS_EST) return;

  let budget = MAX_BREAKPOINTS - holders.length;
  if (budget <= 0) return;

  const mark = (holder: Record<string, Any>) => {
    if (!isObj(holder.cache_control)) {
      holder.cache_control = cc();
      budget--;
    }
  };

  // tools tail
  if (budget > 0 && Array.isArray(body.tools) && body.tools.length > 0) {
    const tool = body.tools.filter(isObj).at(-1);
    if (tool) mark(tool);
  }

  // system tail
  if (budget > 0 && body.system !== undefined) {
    const blocks = toBlocks(body.system);
    const target = blocks && lastCacheable(blocks);
    if (blocks && target) {
      body.system = blocks;
      mark(target);
    }
  }

  // tail messages (stable history tail, skip newest if it is the changing one)
  if (budget > 0 && tailBreakpoints > 0 && Array.isArray(body.messages)) {
    let placed = 0;
    // Start from second-to-last so the newest user msg (which changes) is not
    // a breakpoint; if fewer messages, fall back to scanning from end.
    const start = body.messages.length - 2;
    for (
      let i = Math.max(start, 0);
      i >= 0 && placed < tailBreakpoints && budget > 0;
      i--
    ) {
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

function injectOpenAI(body: Any): void {
  const holders = existingHolders(body);
  for (const holder of holders) {
    const old = holder.cache_control;
    if (old?.type !== "ephemeral" || old?.ttl !== TTL) holder.cache_control = cc();
  }
  if (approxChars(body) < MIN_PREFIX_TOKENS_EST) return;
  if (!Array.isArray(body.messages)) return;
  for (const msg of body.messages) {
    if (!isObj(msg) || msg.role !== "system") continue;
    if (Array.isArray(msg.content)) {
      const target = lastCacheable(msg.content.filter(isObj));
      if (target && !isObj(target.cache_control)) target.cache_control = cc();
    } else if (typeof msg.content === "string" && !isObj(msg.cache_control)) {
      msg.cache_control = cc();
    }
  }
}

function mergeBetaHeader(current: string | null): string {
  const parts = (current ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (!parts.includes(BETA_FLAG)) parts.push(BETA_FLAG);
  return parts.join(",");
}

function responseHeaders(source: Headers, requestId: string): Headers {
  const out = new Headers(source);
  out.delete("content-encoding");
  out.delete("content-length");
  out.delete("transfer-encoding");
  out.delete("connection");
  for (const [key, value] of Object.entries(CORS_HEADERS)) out.set(key, value);
  out.set("x-proxy-request-id", requestId);
  return out;
}

interface ForwardMeta {
  id: string;
  convertSse?: boolean;
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
  events.push(sseFrame("message_start", {
    type: "message_start",
    message: { ...parsed, content: [] },
  }));

  const blocks = Array.isArray(parsed.content) ? parsed.content : [];
  blocks.forEach((block: unknown, index: number) => {
    if (!isObj(block)) return;
    const start: Any = { type: "content_block_start", index, content_block: { ...block } };
    if (block.type === "text") start.content_block.text = "";
    if (block.type === "tool_use") start.content_block.input = undefined;
    events.push(sseFrame("content_block_start", start));

    if (block.type === "text" && typeof block.text === "string") {
      events.push(sseFrame("content_block_delta", {
        type: "content_block_delta", index,
        delta: { type: "text_delta", text: block.text },
      }));
    } else if (block.type === "thinking" && typeof block.thinking === "string") {
      events.push(sseFrame("content_block_delta", {
        type: "content_block_delta", index,
        delta: { type: "thinking_delta", thinking: block.thinking },
      }));
    } else if (block.type === "tool_use") {
      events.push(sseFrame("content_block_delta", {
        type: "content_block_delta", index,
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
  meta: ForwardMeta,
): Promise<Response> {
  let upstream: Response;
  try {
    upstream = await fetch(target, {
      method,
      headers,
      body,
      ...(body !== null && typeof body === "object" ? { duplex: "half" } : {}),
    } as RequestInit);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return json({ error: `upstream error: ${message}` }, 502, meta.id);
  }

  if (meta.convertSse) {
    let text = "";
    try {
      text = await new Response(upstream.body).text();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return json({ error: `upstream read error: ${message}` }, 502, meta.id);
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

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

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
      cache: CACHE_ENABLED ? `1h/multi` : "passthrough",
      beta: CACHE_ENABLED ? BETA_FLAG : "not-added",
      forceNonStream: FORCE_NON_STREAM,
      upstreamAttemptsPer
