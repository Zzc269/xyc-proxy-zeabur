// ============================================================
//  xyc-proxy v8-passthrough — 原生透传 + 完整诊断日志
//  ------------------------------------------------------------
//  覆盖仓库根目录 main.ts 后 push main，Zeabur 自动重部署。
//  运行: deno run --allow-net --allow-env --allow-read main.ts
//
//  行为:
//    - POST /v1/messages : 请求体/请求头 一字不改原样转发到 UPSTREAM_URL，
//      客户端 stream 属性原样保留（不做非流式强制）。
//    - 所有诊断仅在本地计算/记录，绝不回写请求。
//    - /logs /logs.json /logs/clear 保留；非 API 路径默认也透传（仅记一行）。
//
//  环境变量:
//    UPSTREAM_URL       默认 https://passion8.cc   (不要带尾斜杠)
//    PROXY_TOKEN        可选；设置后 /logs* 需带 ?proxy_token= 或 x-proxy-token
//    LOG_BODY           1|0  默认 1（记录入站完整 body，单条封顶 50KB）
//    MAX_LOGS           默认 250（内存日志条数，重启即失）
//    PASSTHROUGH_OTHER  1|0  默认 1（非 /v1/messages 路径透传+一行日志）
//    RETRY              0|1  默认 0（上游失败是否重试一次）
//  注意: FORCE_NON_STREAM / CACHE_TTL_ON / INJECT_CURRENT_TIME /
//        BREAKPOINT_MODE 在透传模式下不再生效，可以删除。
// ============================================================

const VERSION = "v8-passthrough";
const UPSTREAM = (Deno.env.get("UPSTREAM_URL") || "https://passion8.cc").replace(/\/+$/, "");
const PROXY_TOKEN = Deno.env.get("PROXY_TOKEN") || "";
const LOG_BODY = Deno.env.get("LOG_BODY") !== "0";
const MAX_LOGS = Math.max(10, parseInt(Deno.env.get("MAX_LOGS") ?? "250", 10) || 250);
const PASSTHROUGH_OTHER = Deno.env.get("PASSTHROUGH_OTHER") !== "0";
const RETRY = Deno.env.get("RETRY") === "1";
const BODY_CAP = 50000;

// ---------- 小工具 ----------
function fnv(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}
const dec = new TextDecoder();
function nowStr(): string {
  return new Date(Date.now() + 8 * 3600 * 1000).toISOString().replace("T", " ").slice(0, 19);
}
function rid(): string { return Math.random().toString(16).slice(2, 10); }
function brief(s: string, n: number): string { return s.length > n ? s.slice(0, n) + "...[cut]" : s; }
function json(o: unknown, status = 200): Response {
  return new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
}

// ---------- 日志存储（内存，重启即失） ----------
interface LogEntry {
  id: string;
  t: string;
  lines: string[];
  body?: string;
  result?: string;
}
const logs: LogEntry[] = [];
const lastBySys = new Map<string, { msgStable: string }>();
let seq = 0;

function addEntry(): LogEntry {
  if (logs.length >= MAX_LOGS) logs.shift();
  const e: LogEntry = { id: `${++seq}-${rid()}`, t: nowStr(), lines: [] };
  logs.push(e);
  return e;
}
function pushLine(e: LogEntry, s: string) {
  e.lines.push(s);
  console.log(`[${VERSION}] ${s}`);
}

// ---------- 请求体解析（只读，不改动任何东西） ----------
function stripStableTime(s: string): string {
  // 仅用于稳定性哈希：剔除历史残留的时间注入块（透传请求本身绝不修改）
  return s
    .replace(/<!-- pxy8-proxy-runtime-time-v1 -->/g, "")
    .replace(/<runtime_context[\s\S]*?<\/runtime_context>/g, "");
}
function scanBody(raw: string) {
  let parsed: any = null;
  try { parsed = JSON.parse(raw); } catch { /* 透传不受影响 */ }
  const model = parsed?.model ?? "-";
  const stream = typeof parsed?.stream === "boolean" ? String(parsed.stream) : (parsed?.stream === "true" ? "true" : "?");
  const msgs: any[] = parsed?.messages ?? [];
  const roles = msgs.map((m: any) => m?.role?.[0] ?? "?").join("");
  const rawLen = raw.length;

  const msgHashes: string[] = [];
  const bpPos: string[] = [];
  let bpCount = 0;
  msgs.forEach((m: any, i: number) => {
    const content = m?.content;
    let stable: string;
    if (typeof content === "string") stable = stripStableTime(content);
    else if (Array.isArray(content)) {
      const textOnly = content
        .filter((b: any) => typeof b === "object" && b?.type === "text")
        .map((b: any) => { const cp = { ...b }; delete cp.cache_control; return JSON.stringify(cp); });
      stable = stripStableTime(textOnly.join("\n"));
      for (const b of content) {
        if (b && typeof b === "object" && b.cache_control) {
          bpCount++;
          bpPos.push(`msg${i}.${b.type ?? "?"}:${b.cache_control.ttl ?? b.cache_control.type ?? "?"}`);
        }
      }
    } else stable = JSON.stringify(content);
    msgHashes.push(`${i}${m?.role?.[0] ?? "?"}:${fnv(stable)}/${stable.length}`);
  });

  const sys: any = parsed?.system;
  const sysStr = typeof sys === "string" ? sys : JSON.stringify(sys ?? []);
  if (Array.isArray(sys)) {
    sys.forEach((s: any, i: number) => {
      if (s && s.cache_control) { bpCount++; bpPos.push(`sys${i}:${s.cache_control.ttl ?? s.cache_control.type ?? "?"}`); }
    });
  }
  const tools: any[] = parsed?.tools ?? [];
  const toolsStr = JSON.stringify(tools);
  tools.forEach((t: any, i: number) => {
    if (t && t.cache_control) { bpCount++; bpPos.push(`tool${i}:${t.cache_control.ttl ?? t.cache_control.type ?? "?"}`); }
  });

  const msgStable = msgHashes.join(",");
  const prev = lastBySys.get(fnv(sysStr));
  lastBySys.set(fnv(sysStr), { msgStable });
  let st = { hasPrev: false, stable: false, common: 0, firstDiff: -2 };
  if (prev) {
    const a = prev.msgStable.split(",").map((s: string) => s.slice(s.indexOf(":") + 1));
    const b = msgStable.split(",").map((s: string) => s.slice(s.indexOf(":") + 1));
    let common = 0;
    for (let i = 0; i < Math.min(a.length, b.length); i++) {
      if (a[i] === b[i]) common++; else break;
    }
    const firstDiff = common >= Math.min(a.length, b.length) ? -1 : common;
    st = { hasPrev: true, stable: firstDiff === -1 && b.length >= a.length, common, firstDiff };
  }

  return {
    model, stream, roles, rawLen, msgHashes, bpCount, bpPos,
    sysHash: fnv(sysStr), sysLen: sysStr.length,
    toolsHash: fnv(toolsStr), toolsLen: toolsStr.length,
    msgStable, parsed, st,
  };
}

// ---------- 上游转发（原样） ----------
function extractUsage(t: string): string | null {
  const fields: [string, RegExp][] = [
    ["in", /"input_tokens"\s*:\s*(\d+)/],
    ["out", /"output_tokens"\s*:\s*(\d+)/],
    ["read", /"cache_read_input_tokens"\s*:\s*(\d+)/],
    ["create", /"cache_creation_input_tokens"\s*:\s*(\d+)/],
    ["w1h", /"ephemeral_1h_input_tokens"\s*:\s*(\d+)/],
    ["w5m", /"ephemeral_5m_input_tokens"\s*:\s*(\d+)/],
    ["stop", /"stop_reason"\s*:\s*"([^"]+)"/],
  ];
  const parts: string[] = [];
  for (const [k, re] of fields) {
    const m = t.match(re);
    if (m) parts.push(`${k}=${m[1]}`);
  }
  return parts.length ? parts.join(" ") : null;
}
function finishAttempt(e: LogEntry, status: number, ms: number, len: number, bodyText: string) {
  const usage = extractUsage(bodyText);
  const line = `  attempt=1 status=${status} ms=${ms} len=${len}${usage ? " usage=" + usage : ""}` +
    (status >= 400 ? ` ERR-BODY <${brief(bodyText, 1500)}>` : "");
  e.result = line;
  pushLine(e, line);
}
function collect(e: LogEntry, stream: ReadableStream<Uint8Array>, status: number, ms: number) {
  const reader = stream.getReader();
  let len = 0;
  let text = "";
  (async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) {
          len += value.byteLength;
          if (text.length < 1000000) text += dec.decode(value, { stream: true });
        }
      }
      finishAttempt(e, status, ms, len, text);
    } catch (err) {
      finishAttempt(e, status, ms, len, `[stream-error: ${err}]`);
    }
  })();
}

async function forwardMessage(e: LogEntry, raw: string, req: Request): Promise<Response> {
  const url = `${UPSTREAM}/v1/messages`;
  const headers = new Headers(req.headers);
  headers.delete("host");
  headers.delete("connection");
  headers.delete("transfer-encoding");
  headers.delete("x-proxy-token"); // 不透传代理自用 token 给上游
  const t0 = Date.now();

  const attempt = async (): Promise<{ resp: Response; ms: number }> => {
    const resp = await fetch(url, { method: "POST", headers, body: raw });
    return { resp, ms: Date.now() - t0 };
  };
  const respond = ({ resp, ms }: { resp: Response; ms: number }): Response => {
    if (resp.body) {
      const [a, b] = resp.body.tee();
      collect(e, b, resp.status, ms);
      return new Response(a, { status: resp.status, statusText: resp.statusText, headers: resp.headers });
    }
    finishAttempt(e, resp.status, ms, 0, "");
    return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers: resp.headers });
  };

  try {
    const r = await attempt();
    return respond(r);
  } catch (err) {
    const msg = `  attempt=1 status=000 ms=${Date.now() - t0} ERR <${brief(String(err), 1500)}>`;
    e.result = msg;
    pushLine(e, msg);
    if (RETRY) {
      try {
        return respond(await attempt());
      } catch (err2) {
        const m2 = `  attempt=2 status=000 ms=${Date.now() - t0} ERR <${brief(String(err2), 1500)}>`;
        e.result = m2;
        pushLine(e, m2);
      }
    }
    return json({ error: { type: "upstream_unreachable", message: brief(String(err), 300) } }, 502);
  }
}

function summaryLine(e: LogEntry, req: Request, s: ReturnType<typeof scanBody>) {
  let head = `#${e.id} ${e.t} path=/v1/messages model=${s.model} ` +
    `prompt=${fnv(JSON.stringify(s.parsed?.messages ?? []))}/${s.rawLen} ` +
    `sys=${s.sysHash}/${s.sysLen} tools=${s.toolsHash}/${s.toolsLen} ` +
    `msgs=${s.roles} stream=${s.stream} mode=native`;
  head += s.bpCount ? ` bpIn=${s.bpCount}[${s.bpPos.join(",")}]` : " bpIn=0";
  head += s.st.hasPrev
    ? ` prefixStable=${s.st.stable} common=${s.st.common} firstDiff=${s.st.firstDiff}`
    : " firstRequest";
  head += ` betaIn=${req.headers.get("anthropic-beta") || "-"}`;
  pushLine(e, head);
}

// ---------- 路由 ----------
function authed(req: Request): boolean {
  if (!PROXY_TOKEN) return true;
  if (new URL(req.url).searchParams.get("proxy_token") === PROXY_TOKEN) return true;
  return req.headers.get("x-proxy-token") === PROXY_TOKEN;
}

async function passthroughGeneric(req: Request, p: string): Promise<Response> {
  const e = addEntry();
  pushLine(e, `#${e.id} ${e.t} path=${p} passthrough`);
  const headers = new Headers(req.headers);
  headers.delete("host");
  headers.delete("connection");
  headers.delete("transfer-encoding");
  headers.delete("x-proxy-token");
  const up = `${UPSTREAM}${p}${new URL(req.url).search}`;
  const t0 = Date.now();
  const buf = req.method === "GET" || req.method === "HEAD" ? undefined : await req.arrayBuffer();
  try {
    const resp = await fetch(up, { method: req.method, headers, body: buf as BodyInit | undefined });
    pushLine(e, `  status=${resp.status} ms=${Date.now() - t0}`);
    return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers: resp.headers });
  } catch (err) {
    pushLine(e, `  status=000 ms=${Date.now() - t0} ERR <${brief(String(err), 500)}>`);
    return new Response(String(err), { status: 502 });
  }
}

async function handle(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const p = url.pathname;

  if (p === "/" || p === "/health") {
    return json({
      ok: true, provider: "passion8", version: VERSION,
      upstream: UPSTREAM, mode: "native-passthrough", rewrite: "none",
      forceNonStream: false, logsInThisInstance: logs.length,
      maxLogs: MAX_LOGS, distinctSystems: lastBySys.size,
    });
  }
  if (p === "/logs" || p === "/logs.json" || p === "/logs/clear") {
    if (!authed(req)) return new Response("forbidden", { status: 403 });
    if (p === "/logs/clear") { logs.length = 0; return json({ ok: true, cleared: true }); }
    if (p === "/logs.json") {
      return json({
        count: logs.length, distinctSystems: lastBySys.size, version: VERSION,
        logs: logs.map((e) => ({ id: e.id, t: e.t, lines: e.lines.join("\n"), result: e.result ?? null })),
      });
    }
    return new Response(logs.map((e) => e.lines.join("\n")).join("\n"), {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  }
  if (p === "/v1/messages" && req.method === "POST") {
    const raw = await req.text();
    const e = addEntry();
    const s = scanBody(raw);
    summaryLine(e, req, s);
    if (LOG_BODY) e.body = brief(raw, BODY_CAP);
    return await forwardMessage(e, raw, req);
  }
  if (PASSTHROUGH_OTHER) return await passthroughGeneric(req, p);
  return json({ error: "not found" }, 404);
}

// ---------- 启动 ----------
const port = parseInt(Deno.env.get("PORT") || "8080", 10);
console.log(`${VERSION} upstream=${UPSTREAM} port=${port} LOG_BODY=${LOG_BODY} MAX_LOGS=${MAX_LOGS} PROXY_TOKEN=${PROXY_TOKEN ? "set" : "none"}`);
Deno.serve({ port }, handle);
