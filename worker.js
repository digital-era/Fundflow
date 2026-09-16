/**
 * TradeAgent Worker — 仅代理东财资金流（带缓存/重试/备用源）
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

const CACHE_TTL_SECONDS = 60;

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
    if (i < retries) await new Promise((res) => setTimeout(res, baseDelay * (i + 1)));
  }
  return null;
}

/** 源 1：ulist.np 快照（最稳、支持批量，实际是一对一调用） */
async function tryUlist(code) {
  const sid = secid(code);
  const url = new URL("https://push2.eastmoney.com/api/qt/ulist.np/get");
  url.searchParams.set("fltt", "2");
  url.searchParams.set("secids", sid);
  url.searchParams.set(
    "fields",
    "f12,f14,f62,f66,f72,f78,f84,f124"
  );
  url.searchParams.set("_", String(Date.now()));

  const data = await fetchJson(url.toString());
  const diff = data?.data?.diff;
  const row = Array.isArray(diff) ? diff[0] : diff && diff[sid];
  if (!row) {
    console.log("ulist empty", code, JSON.stringify(data).slice(0, 200));
    return null;
  }
  const num = (v) => (v == null || v === "-" ? 0 : Number(v));
  const t = row.f124
    ? new Date(row.f124 * 1000).toISOString().slice(0, 16).replace("T", " ")
    : new Date().toISOString().slice(0, 16).replace("T", " ");
  return {
    code: String(code).padStart(6, "0"),
    main_net: num(row.f62),
    super_net: num(row.f66),
    large_net: num(row.f72),
    mid_net: num(row.f78),
    small_net: num(row.f84),
    time: t,
    source: "eastmoney_ulist",
  };
}

/** 源 2：分钟级 kline（原逻辑） */
async function tryMinute(code) {
  const sid = secid(code);
  const url = new URL("https://push2.eastmoney.com/api/qt/stock/fflow/kline/get");
  url.searchParams.set("secid", sid);
  url.searchParams.set("klt", "1");
  url.searchParams.set("fields1", "f1,f2,f3,f7");
  url.searchParams.set("fields2", "f51,f52,f53,f54,f55,f56,f57");
  url.searchParams.set("lmt", "240");
  url.searchParams.set("_", String(Date.now()));

  const data = await fetchJson(url.toString());
  const klines = data?.data?.klines || [];
  if (!klines.length) {
    console.log("minute empty", code, JSON.stringify(data).slice(0, 200));
    return null;
  }
  const p = klines[klines.length - 1].split(",");
  return {
    code: String(code).padStart(6, "0"),
    main_net: +p[1],
    small_net: +p[2],
    mid_net: +p[3],
    large_net: +p[4],
    super_net: +p[5],
    time: p[0],
    source: "eastmoney_minute",
  };
}

/** 源 3：日级 kline（最终兜底） */
async function tryDaily(code) {
  const sid = secid(code);
  const url = new URL("https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get");
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
  const p = klines[klines.length - 1].split(",");
  return {
    code: String(code).padStart(6, "0"),
    main_net: +p[1],
    small_net: +p[2],
    mid_net: +p[3],
    large_net: +p[4],
    super_net: +p[5],
    time: p[0],
    source: "eastmoney_daily",
  };
}

/** 带缓存的实际获取：命中缓存直接返回，未命中依次尝试三个源 */
async function fetchEastmoneyFundFlow(code, ctx) {
  const key = String(code).padStart(6, "0");
  const cache = caches.default;
  const cacheKey = new Request(`https://cache.internal/fundflow/${key}`, {
    method: "GET",
  });

  const hit = await cache.match(cacheKey);
  if (hit) {
    try {
      const obj = await hit.json();
      if (obj && obj.main_net != null) {
        return { ...obj, cached: true };
      }
    } catch {}
  }

  let flow = null;
  for (const fn of [tryUlist, tryMinute, tryDaily]) {
    try {
      flow = await fn(key);
      if (flow && flow.main_net != null) break;
    } catch (e) {
      console.log("source fail", fn.name, key, e.message);
    }
    flow = null;
  }

  if (flow && flow.main_net != null) {
    const resp = new Response(JSON.stringify(flow), {
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": `max-age=${CACHE_TTL_SECONDS}`,
      },
    });
    if (ctx && ctx.waitUntil) ctx.waitUntil(cache.put(cacheKey, resp.clone()));
    else await cache.put(cacheKey, resp.clone());
  }

  return flow;
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
        const results = {};
        await Promise.all(
          codes.map(async (c) => {
            const k = c.padStart(6, "0");
            results[k] = await fetchEastmoneyFundFlow(k, ctx);
          })
        );
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
        const results = {};
        await Promise.all(
          codes.map(async (c) => {
            results[c] = await fetchEastmoneyFundFlow(c, ctx);
          })
        );
        return json({ time: new Date().toISOString(), results });
      }

      return json({ error: "not found", path }, 404);
    } catch (e) {
      return json({ error: e.message || String(e) }, 500);
    }
  },
};
