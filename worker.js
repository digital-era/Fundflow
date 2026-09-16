/**
 * TradeAgent Worker — 仅代理东财资金流
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

async function fetchEastmoneyFundFlow(code) {
  const sid = secid(code);
  const headers = {
    "User-Agent":
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    Referer: `https://data.eastmoney.com/zjlx/${code}.html`,
    Origin: "https://data.eastmoney.com",
    Accept: "*/*",
  };

  // 1) 分钟级
  try {
    const url = new URL("https://push2.eastmoney.com/api/qt/stock/fflow/kline/get");
    url.searchParams.set("secid", sid);
    url.searchParams.set("klt", "1");
    url.searchParams.set("fields1", "f1,f2,f3,f7");
    url.searchParams.set("fields2", "f51,f52,f53,f54,f55,f56,f57");
    url.searchParams.set("lmt", "240");
    url.searchParams.set("_", String(Date.now()));

    const r = await fetch(url.toString(), { headers });
    if (r.ok) {
      const data = await r.json();
      const klines = data?.data?.klines || [];
      if (klines.length) {
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
    }
  } catch (e) {
    console.log("minute fail", code, e.message);
  }

  // 2) 日级备用
  try {
    const url = new URL("https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get");
    url.searchParams.set("secid", sid);
    url.searchParams.set("klt", "101");
    url.searchParams.set("fields1", "f1,f2,f3,f7");
    url.searchParams.set("fields2", "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61");
    url.searchParams.set("lmt", "5");
    url.searchParams.set("_", String(Date.now()));

    const r = await fetch(url.toString(), { headers });
    if (r.ok) {
      const data = await r.json();
      const klines = data?.data?.klines || [];
      if (klines.length) {
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
    }
  } catch (e) {
    console.log("daily fail", code, e.message);
  }

  return null;
}

export default {
  async fetch(request) {
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
        const flow = await fetchEastmoneyFundFlow(code);
        if (!flow) return json({ error: "fundflow not found", code }, 404);
        return json(flow);
      }

      // GET /fundflow/batch?codes=a,b
      if (path === "/fundflow/batch" && request.method === "GET") {
        const codes = (url.searchParams.get("codes") || "")
          .split(",")
          .map((c) => c.trim())
          .filter(Boolean);
        if (!codes.length) return json({ error: "missing codes" }, 400);
        const results = {};
        await Promise.all(
          codes.map(async (c) => {
            results[c.padStart(6, "0")] = await fetchEastmoneyFundFlow(c);
          })
        );
        return json(results);
      }

      // POST /fundflow/batch  { "codes": ["605058"] }
      if (path === "/fundflow/batch" && request.method === "POST") {
        let body;
        try {
          body = await request.json();
        } catch {
          return json({ error: "invalid json" }, 400);
        }
        const codes = (body.codes || []).map((c) => String(c).padStart(6, "0"));
        if (!codes.length) return json({ error: "missing codes" }, 400);
        const results = {};
        await Promise.all(
          codes.map(async (c) => {
            results[c] = await fetchEastmoneyFundFlow(c);
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
