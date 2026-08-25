/**
 * xyc relay - minimal stable system-breakpoint relay
 * Only puts a 1h cache breakpoint on the SYSTEM prompt (most stable part).
 * Does NOT touch messages, tools, or inject anything into the conversation,
 * so LobeHub chat-env config changes cannot break the cache prefix.
 */

const PROVIDER = "xyc";
const DEFAULT_UPSTREAM = "https://cn.chatapi.app";
const TTL = "1h";
const BETA_FLAG = "extended-cache-ttl-2025-04-11";

// deno-lint-ignore no-explicit-any
type Any = any;

const UPSTREAM = (Deno.env.get("UPSTREAM_URL") || DEFAULT_UPSTREAM).replace(/\/+$/, "");
const PROXY_TOKEN = Deno.env.get("PROXY_TOKEN") || "";
const FORCE_NON_STREAM = Deno.env.get("FORCE_NON_STREAM") !== "0";
const parsedPort = Number(Deno.env.get("PORT") || "8000");
const PORT = Number.isFinite(parsedPort) && parsedPort > 0 ? Math.trunc(parsedPort) : 8000;

const CORS_HEADERS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers":
    "content-type, x-proxy-token, x-api-key, authorization, anthropic-version, anthropic-beta, anthropic-dangerous-direct-browser-access",
  "access-control-expose-headers": "x-proxy-request-id",
  "access-control-max-age": "86400",
};

const STRIP_HEADERS = [
  "host", "connection", "content-length", "transfer-encoding", "keep-alive",
  "upgrade", "expect", "accept-encoding", "x-proxy-token", "x-proxy-request-id",
  "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto",
];

let sequence = 0;

function isObj(v) { return typeof v === "object" && v !== null && !Array.isArray(v); }

function json(data, status = 200, requestId = "") {
  const headers = new Headers({ ...CORS_HEADERS, "content-type": "application/json; charset=utf-8" });
  if (requestId) headers.set("x-proxy-request-id", requestId);
  return new Response(JSON.stringify(data), { status, headers });
}

function requestId() {
  const stamp = new Date().toISOString().replace(/[-:.]/g, "");
  return `${stamp}-${++sequence}-${crypto.randomUUID().slice(0, 8)}`;
}

async function readRequestBody(req) {
  if (!req.body) return null;
  return new Uint8Array(await req.arrayBuffer());
}

function normalizePath(p) { return p.replace(/\/+$/, "") || "/"; }
function isMessagesPath(p) { return p === "/v1/messages" || p === "/messages"; }
function isChatPath(p) { return p === "/v1/chat/completions" || p === "/chat/completions"; }
function resolveUpstream(p) { return UPSTREAM + p; }

/**
 * THE core: put ONE 1h breakpoint on the LAST text block of `system`.
 * System prompt is the most stable part of the request across turns,
 * so the cache prefix stays byte-identical -> reliable hits.
 * Tools/messages are left completely untouched.
 */
function injectSystemCache(body) {
  if (body.system === undefined) return;

  // system may be a string or an array of blocks
  let blocks;
  if (typeof body.system === "string") {
    if (body.system.trim() === "") return;
    blocks = [{ type: "text", text: body.system }];
  } else if (Array.isArray(body.system)) {
    blocks = body.system.filter(isObj);
  } else {
    return;
  }
  if (blocks.length === 0) return;

  // find last text block (or last block) and attach breakpoint
  const last = blocks[blocks.length - 1];
  last.cache_control = { type: "ephemeral", ttl: TTL };

  // write back as array form (stable)
  body.system = blocks;
}

function mergeBetaHeader(current) {
  const parts = (current ?? "").split(",").map(s => s.trim()).filter(Boolean);
  if (!parts.includes(BETA_FLAG)) parts.push(BETA_FLAG);
  return parts.join(",");
}

function responseHeaders(source, requestId) {
  const out = new Headers(source);
  out.delete("content-encoding");
  out.delete("content-length");
  out.delete("transfer-encoding");
  out.delete("connection");
  for (const [k, v] of Object.entries(CORS_HEADERS)) out.set(k, v);
  out.set("x-proxy-request-id", requestId);
  return out;
}

function sseFrame(event, data) { return `event: ${event}\ndata: ${JSON.stringify(data)}`; }

function toSse(text) {
  let parsed;
  try { parsed = JSON.parse(text); }
  catch {
    return `${sseFrame("error", { type: "error", error: { type: "parse_error", message: text.slice(0, 500) } })}\n\n`;
  }
  if (!isObj(parsed)) {
    return `${sseFrame("error", { type: "error", error: { type: "invalid_response", message: text.slice(0, 500) } })}\n\n`;
  }
  if (isObj(parsed.error)) {
    return `${sseFrame("error", { type: "error", error: parsed.error })}\n\n`;
  }

  const events = [];
  events.push(sseFrame("message_start", { type: "message_start", message: { ...parsed, content: [] } }));

  const blocks = Array.isArray(parsed.content) ? parsed.content : [];
  blocks.forEach((block, index) => {
    if (!isObj(block)) return;
    const start = { type: "content_block_start", index, content_block: { ...block } };
    if (block.type === "text") start.content_block.text = "";
    if (block.type === "tool_use") start.content_block.input = undefined;
    events.push(sseFrame("content_block_start", start));

    if (block.type === "text" && typeof block.text === "string") {
      events.push(sseFrame("content_block_delta", { type: "content_block_delta", index, delta: { type: "text_delta", text: block.text } }));
    } else if (block.type === "thinking" && typeof block.thinking === "string") {
      events.push(sseFrame("content_block_delta", { type: "content_block_delta", index, delta: { type: "thinking_delta", thinking: block.thinking } }));
    } else if (block.type === "tool_use") {
      events.push(sseFrame("content_block_delta", { type: "content_block_delta", index, delta: { type: "input_json_delta", partial_json: JSON.stringify(block.input ?? {}) } }));
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

async function forwardOnce(method, target, headers, body, meta) {
  let upstream;
  try {
    upstream = await fetch(target, { method, headers, body });
  } catch (error) {
    return json({ error: `upstream error: ${error.message}` }, 502, meta.id);
  }

  if (meta.convertSse) {
    let text;
    try { text = await new Response(upstream.body).text(); }
    catch (error) { return json({ error: `upstream read error: ${error.message}` }, 502, meta.id); }
    const out = responseHeaders(upstream.headers, meta.id);
    out.set("content-type", "text/event-stream; charset=utf-8");
    return new Response(toSse(text), { status: upstream.status, headers: out });
  }

  const out = responseHeaders(upstream.headers, meta.id);
  return new Response(upstream.body, { status: upstream.status, headers: out });
}

async function handler(req) {
  const url = new URL(req.url);
  const path = normalizePath(url.pathname);

  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS_HEADERS });

  if (PROXY_TOKEN) {
    const supplied = req.headers.get("x-proxy-token") || url.searchParams.get("proxy_token") || "";
    if (supplied !== PROXY_TOKEN) return json({ error: "unauthorized" }, 401);
  }
  url.searchParams.delete("proxy_token");

  if (req.method === "GET" && (path === "/" || path === "/health")) {
    return json({ ok: true, provider: PROVIDER, upstream: UPSTREAM, cache: "1h/system", beta: BETA_FLAG, forceNonStream: FORCE_NON_STREAM });
  }

  const id = requestId();
  const target = resolveUpstream(path) + url.search;
  const headers = new Headers(req.headers);
  for (const h of STRIP_HEADERS) headers.delete(h);
  headers.set("x-proxy-request-id", id);

  let inboundBytes;
  try { inboundBytes = await readRequestBody(req); }
  catch (error) { return json({ error: `request body read error: ${error.message}` }, 400, id); }

  const rewriteable = req.method === "POST" && (isMessagesPath(path) || isChatPath(path));
  if (!rewriteable) {
    return await forwardOnce(req.method, target, headers, inboundBytes ? new Uint8Array(inboundBytes).buffer : null, { id });
  }

  let body;
  try { body = JSON.parse(new TextDecoder().decode(inboundBytes ?? new Uint8Array())); }
  catch { return json({ error: "bad json body" }, 400, id); }

  // Only inject on the system prompt — the most stable part.
  if (isMessagesPath(path)) {
    // ======== 新增调试日志：打印 system 的长度和哈希 ========
    const sysLen = JSON.stringify(body.system).length;
    const sysDigest = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(JSON.stringify(body.system))
    );
    const sysHash = [...new Uint8Array(sysDigest)]
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("")
      .slice(0, 16);
    console.log("DEBUG sysLen:", sysLen, "sysHash:", sysHash);
    // ======== 调试日志结束 ========

    injectSystemCache(body);
    headers.set("anthropic-beta", mergeBetaHeader(headers.get("anthropic-beta")));
  }
  let convertSse = false;
  if (FORCE_NON_STREAM && isMessagesPath(path) && body?.stream === true) {
    body.stream = false;
    convertSse = true;
  }

  headers.set("content-type", "application/json");
  const outboundBytes = new TextEncoder().encode(JSON.stringify(body));

  return await forwardOnce("POST", target, headers, outboundBytes, { id, convertSse });
}

Deno.serve({ port: PORT, onListen: () => {} }, handler);
