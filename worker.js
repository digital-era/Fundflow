/**
 * TradeAgent Worker — 仅代理东财资金流（含净比 / 缓存 / 重试 / 备用源）
 *
 * GET  /health
 * GET  /fundflow?code=605058
 * GET  /fundflow/batch?codes=605058,603186
 * POST /fundflow/batch  body: { "codes": ["605058"] }
 *
 * 返回字段：
 *   code, main_net, main_net_pct,
 *   super_net, super_net_pct,
 *   large_net, large_net_pct,
 *   mid_net, mid_net_pct,
 *   small_net, small_net_pct,
 *   time, source
 */

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// 缓存 TTL 必须大于 300s 监控间隔：保证每轮基本命中上一轮缓存，
// 避免每轮都打东财导致被限流。
const CACHE_TTL_SECONDS = 350;

// 批量 ulist 分批参数：一次请求的 secids 不宜过多
const ULIST_BATCH_SIZE = 15;
const ULIST_BATCH_DELAY_MS = 250;

// 降级逐只请求的并发与延迟：避免瞬间并发打满东财限额
const DEGRADE_CONCURRENCY = 3;
const DEGRADE_DELAY_MS = 200;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...CORS },
  });
}

function secid(code) {
  const c = String(code).padStart(6, "0");
  return /^[569]/.test(c) ? `1.${c}` : `0.${c}`;
}

const EM_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
  Referer: "https://data.eastmoney.com/",
  Accept: "*/*",
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJson(url, retries = 2, baseDelay = 400) {
  for (let i = 0; i <= retries; i++) {
    try {
      const r = await fetch(url, { headers: EM_HEADERS, cf: { cacheTtl: 0 } });
      if (!r.ok) {
        console.log("http fail", r.status, url);
      } else {
        const text = await r.text();
        try {
          return JSON.parse(text);
        } catch {
          console.log("parse fail", url, text.slice(0, 200));
        }
      }
    } catch (e) {
      console.log("fetch error", e.message, url);
    }
    if (i < retries) {
      await sleep(baseDelay * (i + 1));
    }
  }
  return null;
}

function num(v) {
  if (v == null || v === "-" || v === "") return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function formatBeijing(ms) {
  const d = new Date(ms + 8 * 3600 * 1000); // 手动 +8 小时，再用 UTC getter 读
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
}

function tsFromSec(sec) {
  if (!sec || !Number.isFinite(Number(sec))) {
    return formatBeijing(Date.now());
  }
  return formatBeijing(Number(sec) * 1000);
}

function makeUlistRow(row) {
  const code = String(row.f12).padStart(6, "0");
  return {
    code,
    main_net: num(row.f62),
    main_net_pct: num(row.f184),
    super_net: num(row.f66),
    super_net_pct: num(row.f69),
    large_net: num(row.f72),
    large_net_pct: num(row.f75),
    mid_net: num(row.f78),
    mid_net_pct: num(row.f81),
    small_net: num(row.f84),
    small_net_pct: num(row.f87),
    time: tsFromSec(row.f124),
    source: "eastmoney_ulist",
  };
}

/** 源 1：ulist.np 快照（单只，保留原逻辑） */
async function tryUlist(code) {
  const sid = secid(code);
  const url = new URL("https://push2.eastmoney.com/api/qt/ulist.np/get");
  url.searchParams.set("fltt", "2");
  url.searchParams.set("secids", sid);
  url.searchParams.set(
    "fields",
    "f12,f14,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f124"
  );
  url.searchParams.set("_", String(Date.now()));

  const data = await fetchJson(url.toString());
  const diff = data?.data?.diff;
  const row = Array.isArray(diff) ? diff[0] : diff && diff[sid];
  if (!row) {
    console.log("ulist empty", code, JSON.stringify(data).slice(0, 200));
    return null;
  }
  return makeUlistRow(row);
}

/** 源 1-批量：分批请求，返回 { code: flow } */
async function tryUlistBatch(codes) {
  if (!codes.length) return {};

  const out = {};
  const batches = [];
  for (let i = 0; i < codes.length; i += ULIST_BATCH_SIZE) {
    batches.push(codes.slice(i, i + ULIST_BATCH_SIZE));
  }

  for (let i = 0; i < batches.length; i++) {
    if (i > 0) await sleep(ULIST_BATCH_DELAY_MS);
    const batch = batches[i];
    const secids = batch.map(secid).join(",");
    const url = new URL("https://push2.eastmoney.com/api/qt/ulist.np/get");
    url.searchParams.set("fltt", "2");
    url.searchParams.set("secids", secids);
    url.searchParams.set(
      "fields",
      "f12,f14,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f124"
    );
    url.searchParams.set("_", String(Date.now()));

    const data = await fetchJson(url.toString());
    const diff = data?.data?.diff;

    if (Array.isArray(diff)) {
      for (const row of diff) {
        if (!row || row.f12 == null) continue;
        const r = makeUlistRow(row);
        out[r.code] = r;
      }
    } else if (diff && typeof diff === "object") {
      for (const k of Object.keys(diff)) {
        const row = diff[k];
        if (!row || row.f12 == null) continue;
        const r = makeUlistRow(row);
        out[r.code] = r;
      }
    } else {
      console.log("ulist batch empty", JSON.stringify(data).slice(0, 200));
    }
  }

  return out;
}

function parseKline(p, code, source) {
  return {
    code: String(code).padStart(6, "0"),
    main_net: num(p[1]),
    small_net: num(p[2]),
    mid_net: num(p[3]),
    large_net: num(p[4]),
    super_net: num(p[5]),
    main_net_pct: num(p[6]),
    small_net_pct: num(p[7]),
    mid_net_pct: num(p[8]),
    large_net_pct: num(p[9]),
    super_net_pct: num(p[10]),
    time: p[0],
    source,
  };
}

/** 源 2：分钟级 kline */
async function tryMinute(code) {
  const sid = secid(code);
  const url = new URL("https://push2.eastmoney.com/api/qt/stock/fflow/kline/get");
  url.searchParams.set("secid", sid);
  url.searchParams.set("klt", "1");
  url.searchParams.set("fields1", "f1,f2,f3,f7");
  url.searchParams.set(
    "fields2",
    "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
  );
  url.searchParams.set("lmt", "240");
  url.searchParams.set("_", String(Date.now()));

  const data = await fetchJson(url.toString());
  const klines = data?.data?.klines || [];
  if (!klines.length) {
    console.log("minute empty", code, JSON.stringify(data).slice(0, 200));
    return null;
  }
  return parseKline(
    klines[klines.length - 1].split(","),
    code,
    "eastmoney_minute"
  );
}

/** 源 3：日级 kline（兜底） */
async function tryDaily(code) {
  const sid = secid(code);
  const url = new URL(
    "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
  );
  url.searchParams.set("secid", sid);
  url.searchParams.set("klt", "101");
  url.searchParams.set("fields1", "f1,f2,f3,f7");
  url.searchParams.set(
    "fields2",
    "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
  );
  url.searchParams.set("lmt", "5");
  url.searchParams.set("_", String(Date.now()));

  const data = await fetchJson(url.toString());
  const klines = data?.data?.klines || [];
  if (!klines.length) {
    console.log("daily empty", code, JSON.stringify(data).slice(0, 200));
    return null;
  }
  return parseKline(
    klines[klines.length - 1].split(","),
    code,
    "eastmoney_daily"
  );
}

function cacheKeyOf(code) {
  const key = String(code).padStart(6, "0");
  return {
    key,
    request: new Request(`https://cache.internal/fundflow/${key}`, {
      method: "GET",
    }),
  };
}

async function readCache(code) {
  const { request } = cacheKeyOf(code);
  const cache = caches.default;
  const hit = await cache.match(request);
  if (!hit) return null;
  try {
    const obj = await hit.json();
    if (obj && obj.main_net != null && obj.source) {
      return { ...obj, cached: true };
    }
  } catch {}
  return null;
}

async function writeCache(flow, ctx) {
  if (!flow || flow.main_net == null) return;
  const { request } = cacheKeyOf(flow.code);
  const cache = caches.default;
  const resp = new Response(JSON.stringify(flow), {
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `max-age=${CACHE_TTL_SECONDS}`,
    },
  });
  if (ctx && ctx.waitUntil) ctx.waitUntil(cache.put(request, resp.clone()));
  else await cache.put(request, resp.clone());
}

/** 带缓存：命中直接返回；未命中依次尝试三个源；成功才写缓存 */
async function fetchEastmoneyFundFlow(code, ctx) {
  const hit = await readCache(code);
  if (hit) return hit;

  let flow = null;
  for (const fn of [tryUlist, tryMinute, tryDaily]) {
    try {
      flow = await fn(code);
      if (flow && flow.main_net != null) break;
    } catch (e) {
      console.log("source fail", fn.name, code, e.message);
    }
    flow = null;
  }

  if (flow && flow.main_net != null) {
    await writeCache(flow, ctx);
  }

  return flow;
}

/** 简单并发控制：最多 concurrency 个 worker 同时执行 */
async function runWithConcurrency(items, concurrency, worker) {
  const queue = items.slice();
  const runners = Array.from(
    { length: Math.min(concurrency, queue.length) },
    async () => {
      while (queue.length) {
        const item = queue.shift();
        await worker(item);
      }
    }
  );
  await Promise.all(runners);
}

/** 批量：查缓存 → 分批 ulist → 逐只降级（限并发 + 延迟） */
async function fetchBatchFundFlow(codes, ctx) {
  const results = {};
  const missing = [];

  // 1) 并发查缓存（Cache API 很快，不增加上游压力）
  await Promise.all(
    codes.map(async (c) => {
      const key = String(c).padStart(6, "0");
      const hit = await readCache(key);
      if (hit) {
        results[key] = hit;
      } else {
        missing.push(key);
      }
    })
  );

  if (!missing.length) return results;

  // 稳定 secids 顺序
  missing.sort();

  // 2) 分批 ulist
  let ulistMap = {};
  try {
    ulistMap = await tryUlistBatch(missing);
  } catch (e) {
    console.log("ulist batch fail", e.message);
  }

  // 3) 批量结果落库
  const stillMissing = [];
  for (const key of missing) {
    const flow = ulistMap[key];
    if (flow && flow.main_net != null) {
      results[key] = flow;
      await writeCache(flow, ctx);
    } else {
      stillMissing.push(key);
    }
  }

  if (!stillMissing.length) return results;

  // 4) 仍缺失的逐只降级（限制并发 + 延迟）
  await runWithConcurrency(stillMissing, DEGRADE_CONCURRENCY, async (key) => {
    let flow = null;
    for (const fn of [tryMinute, tryDaily]) {
      try {
        flow = await fn(key);
        if (flow && flow.main_net != null) break;
      } catch (e) {
        console.log("source fail", fn.name, key, e.message);
      }
      flow = null;
    }
    if (flow && flow.main_net != null) {
      results[key] = flow;
      await writeCache(flow, ctx);
    } else {
      results[key] = null;
    }
    if (DEGRADE_DELAY_MS > 0) await sleep(DEGRADE_DELAY_MS);
  });

  return results;
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    const url = new URL(request.url);
    const path = url.pathname.replace(/\/$/, "") || "/";

    try {
      if (path === "/health") {
        return json({ ok: true, time: new Date().toISOString() });
      }

      if (path === "/fundflow" && request.method === "GET") {
        const code = url.searchParams.get("code");
        if (!code) return json({ error: "missing code" }, 400);
        const flow = await fetchEastmoneyFundFlow(code, ctx);
        if (!flow) return json({ error: "fundflow not found", code }, 404);
        return json(flow);
      }

      if (path === "/fundflow/batch" && request.method === "GET") {
        const codes = (url.searchParams.get("codes") || "")
          .split(",")
          .map((c) => c.trim())
          .filter(Boolean);
        if (!codes.length) return json({ error: "missing codes" }, 400);
        const results = await fetchBatchFundFlow(codes, ctx);
        return json({ time: new Date().toISOString(), results });
      }

      if (path === "/fundflow/batch" && request.method === "POST") {
        let body;
        try {
          body = await request.json();
        } catch {
          return json({ error: "invalid json" }, 400);
        }
        const codes = (body.codes || []).map((c) =>
          String(c).padStart(6, "0")
        );
        if (!codes.length) return json({ error: "missing codes" }, 400);
        const results = await fetchBatchFundFlow(codes, ctx);
        return json({ time: new Date().toISOString(), results });
      }

      return json({ error: "not found", path }, 404);
    } catch (e) {
      return json({ error: e.message || String(e) }, 500);
    }
  },
};
