/**
 * xyc relay - Claude 1h full-prefix cache + current-time injection
 * Single-file version for Deno/Zeabur.
 *
 * LobeHub Anthropic Base URL:
 * https://YOUR_PROJECT.deno.dev
 *
 * FORCE_NON_STREAM=1 (default on) rewrites incoming stream=true requests to
 * stream=false before forwarding upstream, then converts the full JSON response
 * back into an Anthropic SSE event stream. Set FORCE_NON_STREAM=0 to restore
 * passthrough streaming.
 *
 * Default behavior:
 * 1. Removes LobeHub's built-in 5m breakpoints, places a single 1h breakpoint on
 *    the last message (it caches all preceding tools/system/messages).
 * 2. Normalizes all Anthropic message text content to stable block form so a
 *    previous newest message keeps the same cache prefix on the next turn.
 * 3. Appends current time after the cache breakpoint of the last user message
 *    (the time block itself is never cached).
 * 4. Each incoming request triggers exactly 1 upstream request, with no retry.
 *
 * Optional env vars:
 * UPSTREAM_URL        default https://cn.chatapi.app
 * PROXY_TOKEN         optional access token; requests must carry x-proxy-token
 * CACHE_TTL_ON        "0" = do not touch cache; default on
 * BREAKPOINT_MODE     "message" (default, single message breakpoint) | "all" (max 4)
 * TAIL_BREAKPOINTS    number of tail message breakpoints; default 2, suggest 1~2
 * INJECT_CURRENT_TIME "0" = do not inject current time; default on
 * TIME_ZONE           default Asia/Shanghai
 * FORCE_NON_STREAM    "0" = passthrough streaming; default force non-stream + SSE
 * PORT                HTTP listen port; default 8000
 */

const PROVIDER = "xyc";
const DEFAULT_UPSTREAM = "https://cn.chatapi.app";
const TTL = "1h";
const BETA_FLAG = "extended-cache-ttl-2025-04-11";
const MAX_BREAKPOINTS = 4;
const MIN_CHARS = 2000;
const RUNTIME_MARKER = "xyc-proxy-runtime-time-v1";

// deno-lint-ignore no-explicit-any
type Any = any;

const UPSTREAM = (Deno.env.get("UPSTREAM_URL") || DEFAULT_UPSTREAM).replace(/\/+$/, "");
const PROXY_TOKEN = Deno.env.get("PROXY_TOKEN") || "";
const CACHE_ENABLED = Deno.env.get("CACHE_TTL_ON") !== "0";
const BREAKPOINT_MODE = (Deno.env.get("BREAKPOINT_MODE") || "all").toLowerCase();
const TIME_ENABLED = Deno.env.get("INJECT_CURRENT_TIME") !== "1";

const TIME_ZONE = Deno.env.get("TIME_ZONE") || "Asia/Shanghai";
const FORCE_NON_STREAM = Deno.env.get("FORCE_NON_STREAM") !== "0";
const parsedPort = Number(Deno.env.get("PORT") || "8000");
const PORT = Number.isFinite(parsedPort) && parsedPort > 0 ? Math.trunc(parsedPort) : 8000;

const parsedTail = Number(Deno.env.get("TAIL_BREAKPOINTS") ?? "2");
const TAIL_BREAKPOINTS = Number.isFinite(parsedTail)
  ? Math.max(0, Math.min(2, Math.trunc(parsedTail)))
  : 2;

const CACHEABLE_TYPES = new Set([
  "text",
  "image",
  "document",
  "tool_use",
  "tool_result",
  "search_result",
]);

const CORS_HEADERS: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers":
    "content-type, x-proxy-token, authorization, x-api-key, anthropic-version, anthropic-beta, anthropic-dangerous-direct-browser-access",
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
  for (let i = 0; i < a.length; i++) {
    diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
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

function formatTime(date = new Date()): string {
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
    return new Intl.DateTimeFormat("sv-SE", options).format(date);
  } catch {
    return date.toISOString();
  }
}

function cc() {
  return { type: "ephemeral", ttl: TTL };
}

function approxChars(body: Any): number {
  let n = 0;
  const walk = (v: unknown) => {
    if (typeof v === "string") {
      n += v.length;
    } else if (Array.isArray(v)) {
      for (const x of v) walk(x);
    } else if (isObj(v)) {
      for (const x of Object.values(v)) walk(x);
    }
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

/**
 * LobeHub may send the newest message as a text-block array while serializing
 * that same message as a plain string on the next turn. Normalize every
 * message so the cached prefix remains stable.
 */
function normalizeAnthropicMessages(body: Any): void {
  if (!Array.isArray(body?.messages)) return;

  for (const msg of body.messages) {
    if (!isObj(msg)) continue;

    if (typeof msg.content === "string") {
      if (msg.content.trim() === "") continue;
      msg.content = [{ type: "text", text: msg.content }];
      continue;
    }

    if (!Array.isArray(msg.content)) continue;
    msg.content = msg.content.map((block: unknown) => {
      if (
        !isObj(block) ||
        block.type !== "text" ||
        typeof block.text !== "string"
      ) {
        return block;
      }

      const {
        type: _type,
        text,
        cache_control,
        ...rest
      } = block;
      return {
        type: "text",
        text,
        ...rest,
        ...(cache_control === undefined ? {} : { cache_control }),
      };
    });
  }
}

function lastCacheable(blocks: Record<string, Any>[]): Record<string, Any> | null {
  for (let i = blocks.length - 1; i >= 0; i--) {
    const type = blocks[i].type;
    if (typeof type === "string" && CACHEABLE_TYPES.has(type)) return blocks[i];
  }
  return null;
}

/** Remove a previously injected runtime time block if a client saved it. */
function removeOldRuntimeBlocks(body: Any): void {
  if (!Array.isArray(body?.messages)) return;
  for (const msg of body.messages) {
    if (!isObj(msg) || !Array.isArray(msg.content)) continue;
    msg.content = msg.content.filter((block: unknown) => {
      return !(
        isObj(block) &&
        block.type === "text" &&
        typeof block.text === "string" &&
        block.text.includes(RUNTIME_MARKER)
      );
    });
  }
}

/**
 * Single message breakpoint mode. Remove existing breakpoints and place one 1h
 * breakpoint on the last cacheable block of the newest message.
 */
function injectAnthropicMessage(body: Any): void {
  const holders = existingHolders(body);
  for (const holder of holders) delete holder.cache_control;

  if (approxChars(body) < MIN_CHARS) return;
  if (!Array.isArray(body?.messages) || body.messages.length === 0) return;

  for (let i = body.messages.length - 1; i >= 0; i--) {
    const msg = body.messages[i];
    if (!isObj(msg)) continue;
    const blocks = toBlocks(msg.content);
    const target = blocks && lastCacheable(blocks);
    if (!blocks || !target) continue;
    msg.content = blocks;
    target.cache_control = cc();
    return;
  }
}

/**
 * Multi-breakpoint compatibility mode: upgrade existing breakpoints to 1h,
 * then top up tools, system and tail messages, up to four breakpoints.
 */
function injectAnthropicAll(body: Any, tailBreakpoints: number): void {
  const holders = existingHolders(body);
  for (const holder of holders) {
    const old = holder.cache_control;
    if (old?.type !== "ephemeral" || old?.ttl !== TTL) {
      holder.cache_control = cc();
    }
  }

  if (approxChars(body) < MIN_CHARS) return;

  let budget = MAX_BREAKPOINTS - holders.length;
  if (budget <= 0) return;

  const mark = (holder: Record<string, Any>) => {
    holder.cache_control = cc();
    budget--;
  };

  if (budget > 0 && Array.isArray(body.tools) && body.tools.length > 0) {
    const tool = body.tools.filter(isObj).at(-1);
    if (tool && !isObj(tool.cache_control)) mark(tool);
  }

  if (budget > 0 && body.system !== undefined) {
    const blocks = toBlocks(body.system);
    const target = blocks && lastCacheable(blocks);
    if (blocks && target && !isObj(target.cache_control)) {
      body.system = blocks;
      mark(target);
    }
  }

  if (budget > 0 && tailBreakpoints > 0 && Array.isArray(body.messages)) {
    let placed = 0;
    for (
      let i = body.messages.length - 1;
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
    if (old?.type !== "ephemeral" || old?.ttl !== TTL) {
      holder.cache_control = cc();
    }
  }

  if (approxChars(body) < MIN_CHARS) return;
  if (!Array.isArray(body.messages)) return;

  for (const msg of body.messages) {
    if (!isObj(msg) || msg.role !== "system") continue;
    if (Array.isArray(msg.content)) {
      const target = lastCacheable(msg.content.filter(isObj));
      if (target && !isObj(target.cache_control)) {
        target.cache_control = cc();
      }
    } else if (typeof msg.content === "string" && !isObj(msg.cache_control)) {
      msg.cache_control = cc();
    }
  }
}

/** Add uncached current-time context after the newest cache breakpoint. */
function appendCurrentTime(body: Any): void {
  if (!Array.isArray(body?.messages) || body.messages.length === 0) return;

  const last = body.messages.at(-1);
  if (!isObj(last) || last.role !== "user") return;

  if (typeof last.content === "string") {
    last.content = [{ type: "text", text: last.content }];
  }
  if (!Array.isArray(last.content)) return;

  const now = new Date();
  last.content.push({
    type: "text",
    text:
      `<!-- ${RUNTIME_MARKER} -->\n` +
      `<runtime_context source="request_proxy">\n` +
      `Current time: ${formatTime(now)}\n` +
      `Time zone: ${TIME_ZONE}\n` +
      `This is runtime info added by the proxy, not the user's original text. ` +
      `Only use it when the question involves now, today, dates, deadlines or relative time.\n` +
      `</runtime_context>`,
  });
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

/** Convert a full Anthropic JSON response into an Anthropic SSE event stream. */
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

/** Exactly one upstream fetch: no loops, backoff or automatic retry. */
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

  if (path === "/logs" || path === "/logs/clear") {
    return json({ error: "not found" }, 404);
  }

  if (req.method === "GET" && (path === "/" || path === "/health")) {
    return json({
      ok: true,
      provider: PROVIDER,
      upstream: UPSTREAM,
      cache: CACHE_ENABLED ? `${TTL}/${BREAKPOINT_MODE}` : "passthrough",
      beta: CACHE_ENABLED ? BETA_FLAG : "not-added",
      tailBreakpoints: TAIL_BREAKPOINTS,
      currentTimeInjection: TIME_ENABLED,
      timeZone: TIME_ZONE,
      forceNonStream: FORCE_NON_STREAM,
      upstreamAttemptsPerIncomingRequest: 1,
    });
  }

  const id = requestId();
  const target = resolveUpstream(path) + url.search;
  const headers = new Headers(req.headers);
  for (const header of STRIP_HEADERS) headers.delete(header);
  headers.set("x-proxy-request-id", id);

  let inboundBytes: Uint8Array | null;
  try {
    inboundBytes = await readRequestBody(req);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return json({ error: `request body read error: ${message}` }, 400, id);
  }

  const rewriteable = req.method === "POST" && (isMessagesPath(path) || isChatPath(path));
  if (!rewriteable) {
    return await forwardOnce(
      req.method,
      target,
      headers,
      requestBodyInit(inboundBytes),
      { id },
    );
  }

  let body: Any;
  try {
    body = JSON.parse(new TextDecoder().decode(inboundBytes ?? new Uint8Array()));
  } catch {
    return json({ error: "bad json body" }, 400, id);
  }

  removeOldRuntimeBlocks(body);
  if (isMessagesPath(path)) normalizeAnthropicMessages(body);

  if (CACHE_ENABLED) {
    if (isMessagesPath(path)) {
      if (BREAKPOINT_MODE === "all") {
        injectAnthropicAll(body, TAIL_BREAKPOINTS);
      } else {
        injectAnthropicMessage(body);
      }
      headers.set("anthropic-beta", mergeBetaHeader(headers.get("anthropic-beta")));
    } else {
      injectOpenAI(body);
    }
  }

  if (TIME_ENABLED && isMessagesPath(path)) appendCurrentTime(body);

  let convertSse = false;
  if (FORCE_NON_STREAM && isMessagesPath(path) && body?.stream === true) {
    body.stream = false;
    convertSse = true;
  }

  headers.set("content-type", "application/json");
  const outboundBytes = new TextEncoder().encode(JSON.stringify(body));

  return await forwardOnce(
    "POST",
    target,
    headers,
    outboundBytes,
    { id, convertSse },
  );
}

Deno.serve({ port: PORT, onListen: () => {} }, handler);
