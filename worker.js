/**
 * TradeAgent Worker — 仅代理东财资金流（含净比 / 缓存 / 回退 / 备用源）
 *
 * GET  /health
 * GET  /fundflow?code=605058
 * GET  /fundflow/batch?codes=605058,603186
 * POST /fundflow/batch  body: { "codes": ["605058"] }
 */

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// 新鲜 TTL：≤400s（数据年龄不超过 ~6.5 分钟）
const CACHE_FRESH_SECONDS = 400;
// 过期回退 TTL：20分钟（刷新失败时返回上次成功的数据，而不是 null）
const CACHE_STALE_SECONDS = 1200;

// 批量 ulist：恢复到 15，减少请求总数
const ULIST_BATCH_SIZE = 15;
const ULIST_BATCH_DELAY_MS = 400;

// 逐只 minute/daily 兜底（仅在批量 + 过期回退都拿不到时启用）
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
    if (i < retries) await sleep(baseDelay * (i + 1));
  }
  return null;
}

function num(v) {
  if (v == null || v === "-" || v === "") return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function formatBeijing(ms) {
  const d = new Date(ms + 8 * 3600 * 1000);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
}

function tsFromSec(sec) {
  if (!sec || !Number.isFinite(Number(sec))) return formatBeijing(Date.now());
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

/** 源 1：ulist.np 单只 */
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
    let got = 0;

    if (Array.isArray(diff)) {
      for (const row of diff) {
        if (!row || row.f12 == null) continue;
        const r = makeUlistRow(row);
        out[r.code] = r;
        got++;
      }
    } else if (diff && typeof diff === "object") {
      for (const k of Object.keys(diff)) {
        const row = diff[k];
        if (!row || row.f12 == null) continue;
        const r = makeUlistRow(row);
        out[r.code] = r;
        got++;
      }
    }
    console.log(
      `ulist batch ${i + 1}/${batches.length} req=${batch.length} got=${got}`
    );
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
  if (!klines.length) return null;
  return parseKline(klines[klines.length - 1].split(","), code, "eastmoney_minute");
}

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
  if (!klines.length) return null;
  return parseKline(klines[klines.length - 1].split(","), code, "eastmoney_daily");
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

/**
 * 读缓存，返回 { fresh, stale }：
 *   fresh：cached_at 距今 ≤ CACHE_FRESH_SECONDS，可直接用
 *   stale：已过期但仍在缓存，作为刷新失败时的回退
 */
async function readCache(code) {
  const { request } = cacheKeyOf(code);
  const cache = caches.default;
  const hit = await cache.match(request);
  if (!hit) return { fresh: null, stale: null };
  try {
    const obj = await hit.json();
    if (!obj || obj.main_net == null || !obj.source) {
      return { fresh: null, stale: null };
    }
    const age = (Date.now() - (obj.cached_at || 0)) / 1000;
    if (age <= CACHE_FRESH_SECONDS) return { fresh: obj, stale: null };
    return { fresh: null, stale: obj };
  } catch {
    return { fresh: null, stale: null };
  }
}

async function writeCache(flow, ctx) {
  if (!flow || flow.main_net == null) return;
  const { request } = cacheKeyOf(flow.code);
  const cache = caches.default;
  const body = JSON.stringify({ ...flow, cached_at: Date.now() });
  const resp = new Response(body, {
    headers: {
      "Content-Type": "application/json",
      // 缓存保留 CACHE_STALE_SECONDS；新鲜与否由 body 里的 cached_at 决定
      "Cache-Control": `max-age=${CACHE_STALE_SECONDS}`,
    },
  });
  if (ctx && ctx.waitUntil) ctx.waitUntil(cache.put(request, resp.clone()));
  else await cache.put(request, resp.clone());
}

/** 单只：新鲜缓存 → ulist → minute → daily → 过期回退 → null */
async function fetchEastmoneyFundFlow(code, ctx) {
  const { fresh, stale } = await readCache(code);
  if (fresh) return fresh;

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
    return flow;
  }
  if (stale) return { ...stale, cached: true, stale: true };
  return null;
}

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

/**
 * 批量：新鲜缓存 → 一次批量 ulist → 逐只 minute/daily → 过期回退 → null
 * 关键：
 *   1. 不再有逐只 ulist 重试（去掉放大器）
 *   2. 刷新失败时优先返回过期缓存，而不是 null
 */
async function fetchBatchFundFlow(codes, ctx) {
  const results = {};
  const fallbacks = {};
  const missing = [];

  // 1) 并发查缓存
  await Promise.all(
    codes.map(async (c) => {
      const key = String(c).padStart(6, "0");
      const { fresh, stale } = await readCache(key);
      if (fresh) {
        results[key] = fresh;
      } else {
        if (stale) fallbacks[key] = stale;
        missing.push(key);
      }
    })
  );

  if (!missing.length) return results;
  missing.sort();

  // 2) 一次批量 ulist（33 只 = 3 批）
  let ulistMap = {};
  try {
    ulistMap = await tryUlistBatch(missing);
  } catch (e) {
    console.log("ulist batch fail", e.message);
  }

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

  // 3) 逐只 minute/daily 兜底（少量并发）
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
    } else if (fallbacks[key]) {
      // 4) 回退到过期缓存（不显示 null）
      results[key] = { ...fallbacks[key], cached: true, stale: true };
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
