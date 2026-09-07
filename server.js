"use strict";

/*
=========================================================
🔥 MT5 FOREX AI ANALYZER V12
POWERED BY ELISY ANALYSIS DEVELOPER

Backend: Express + WebSocket
Frontend: public/index.html

IMPORTANT:
- This server does NOT automatically place live MT5 trades.
- MT5 data must be supplied by the MT5 EA/bridge.
- AI predictions are analysis only.
=========================================================
*/

const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({
  server,
  path: "/ws"
});


/* =========================================================
   CONFIG
========================================================= */

const PORT = Number(process.env.PORT || 10000);

const FRONTEND_VERSION = "12.0.2";
const SERVER_VERSION = "12.0.2";

const BOT_PASSWORD =
  process.env.BOT_PASSWORD || "CHANGE_THIS_PASSWORD";

const MT5_BRIDGE_SECRET =
  process.env.MT5_BRIDGE_SECRET || "";

const MAX_CANDLES =
  Number(process.env.MAX_CANDLES || 500);

const STALE_DATA_MS =
  Number(process.env.STALE_DATA_MS || 15000);

const AI_REFRESH_MS =
  Number(process.env.AI_REFRESH_MS || 60000);

const MAX_SPREAD_POINTS =
  Number(process.env.MAX_SPREAD_POINTS || 50);

const MAX_DAILY_LOSS_PERCENT =
  Number(process.env.MAX_DAILY_LOSS_PERCENT || 5);

const MAX_POSITIONS =
  Number(process.env.MAX_POSITIONS || 3);

const COOLDOWN_SECONDS =
  Number(process.env.COOLDOWN_SECONDS || 60);


/* =========================================================
   AI CONFIG
========================================================= */

const AI_CONFIG = {

  openai: {
    key: process.env.OPENAI_API_KEY || "",
    model:
      process.env.OPENAI_MODEL ||
      "gpt-4o-mini"
  },

  deepseek: {
    key: process.env.DEEPSEEK_API_KEY || "",
    model:
      process.env.DEEPSEEK_MODEL ||
      "deepseek-chat"
  },

  gemini: {
    key: process.env.GEMINI_API_KEY || "",
    model:
      process.env.GEMINI_MODEL ||
      "gemini-2.5-flash"
  },

  groq: {
    key: process.env.GROQ_API_KEY || "",
    model:
      process.env.GROQ_MODEL ||
      "openai/gpt-oss-20b"
  },

  openrouter: {
    key: process.env.OPENROUTER_API_KEY || "",
    model:
      process.env.OPENROUTER_MODEL ||
      "openrouter/free"
  },

  mistral: {
    key: process.env.MISTRAL_API_KEY || "",
    model:
      process.env.MISTRAL_MODEL ||
      "mistral-large-latest"
  }

};


/* =========================================================
   EXPRESS
========================================================= */

app.disable("x-powered-by");

app.use(
  express.json({
    limit: "2mb"
  })
);

app.use(
  express.urlencoded({
    extended: false,
    limit: "1mb"
  })
);


/* =========================================================
   SECURITY HEADERS
========================================================= */

app.use((req, res, next) => {

  res.setHeader(
    "X-Content-Type-Options",
    "nosniff"
  );

  res.setHeader(
    "X-Frame-Options",
    "SAMEORIGIN"
  );

  res.setHeader(
    "Referrer-Policy",
    "strict-origin-when-cross-origin"
  );

  res.setHeader(
    "Permissions-Policy",
    "geolocation=(), microphone=(), camera=()"
  );

  next();

});


/* =========================================================
   STATIC FILES
========================================================= */

const PUBLIC_DIR =
  path.join(__dirname, "public");

const INDEX_FILE =
  path.join(PUBLIC_DIR, "index.html");


app.use(
  express.static(PUBLIC_DIR, {
    index: false,

    etag: false,

    maxAge: 0,

    setHeaders: (res, filePath) => {

      if(
        filePath.endsWith("index.html")
      ){

        res.setHeader(
          "Cache-Control",
          "no-store, no-cache, must-revalidate, proxy-revalidate"
        );

        res.setHeader(
          "Pragma",
          "no-cache"
        );

        res.setHeader(
          "Expires",
          "0"
        );

      }

    }

  })
);


/* =========================================================
   STATE
========================================================= */

const state = {

  mt5: {

    connected: false,

    online: false,

    symbol: "EURUSD",

    timeframe: "M1",

    price: null,

    bid: null,

    ask: null,

    spread: null,

    spreadPoints: null,

    tick: null,

    timestamp: null,

    candles: []

  },

  technical: {

    rsi: null,

    ema20: null,

    ema50: null,

    ema200: null,

    atr: null,

    momentum: null,

    trend: "WAIT",

    candleStructure: "WAIT",

    support: null,

    resistance: null,

    decision: "WAIT",

    regime: "WAIT"

  },

  ai: {},

  consensus: {

    buy: 0,

    sell: 0,

    wait: 100,

    agreement: 0,

    technicalConfirmation: "WAIT"

  },

  finalDecision: {

    action: "WAIT",

    confidence: 0,

    reason:
      "Waiting for real MT5 data and AI confirmation.",

    entry: null,

    sl: null,

    tp: null

  },

  safety: {

    spreadStatus: "WAITING",

    dailyLoss: "0%",

    maxPositions: String(MAX_POSITIONS),

    cooldown: "READY",

    newsRisk: "UNKNOWN",

    dataStatus: "WAITING"

  },

  news: {

    mode: "MONITOR",

    countdown: null,

    minutesUntil: null,

    events: []

  },

  preNews: {

    forecast:
      "Waiting for news event data."

  },

  newsVerification: {

    result:
      "Waiting for released news."

  },

  lastMT5Update: 0,

  lastAIUpdate: 0,

  lastActionTime: 0

};


/* =========================================================
   LOGIN SESSIONS
========================================================= */

const sessions = new Map();

const SESSION_TTL =
  24 * 60 * 60 * 1000;


function createSession(){

  const token =
    crypto.randomBytes(32).toString("hex");

  sessions.set(
    token,
    Date.now()
  );

  return token;

}


function isValidSession(token){

  if(!token){
    return false;
  }

  const created =
    sessions.get(token);

  if(!created){
    return false;
  }

  if(
    Date.now() - created >
    SESSION_TTL
  ){

    sessions.delete(token);

    return false;

  }

  return true;

}


function getTokenFromRequest(req){

  const auth =
    req.headers.authorization || "";

  if(
    auth.startsWith("Bearer ")
  ){

    return auth.slice(7).trim();

  }

  const cookie =
    req.headers.cookie || "";

  const match =
    cookie.match(
      /mt5_session=([^;]+)/
    );

  return match
    ? decodeURIComponent(match[1])
    : "";

}


/* =========================================================
   LOGIN
========================================================= */

app.post(
  "/api/login",
  (req, res) => {

    const password =
      String(
        req.body?.password || ""
      );

    if(
      !BOT_PASSWORD ||
      BOT_PASSWORD ===
      "CHANGE_THIS_PASSWORD"
    ){

      return res.status(503).json({

        success: false,

        ok: false,

        message:
          "BOT_PASSWORD is not configured on the server."

      });

    }


    if(password !== BOT_PASSWORD){

      return res.status(401).json({

        success: false,

        ok: false,

        message:
          "Invalid bot password."

      });

    }


    const token =
      createSession();


    res.setHeader(
      "Set-Cookie",
      [
        "mt5_session=" +
        encodeURIComponent(token),

        "Path=/",

        "HttpOnly",

        "SameSite=Lax",

        "Max-Age=" +
        Math.floor(
          SESSION_TTL / 1000
        )
      ].join("; ")
    );


    return res.json({

      success: true,

      ok: true,

      token,

      message:
        "Login successful."

    });

  }
);


/* =========================================================
   AUTH MIDDLEWARE
========================================================= */

function requireAuth(req, res, next){

  const token =
    getTokenFromRequest(req);

  if(!isValidSession(token)){

    return res.status(401).json({

      success: false,

      message:
        "Authentication required."

    });

  }

  req.sessionToken = token;

  next();

}


/* =========================================================
   PUBLIC HEALTH
========================================================= */

app.get(
  "/health",
  (req, res) => {

    res.json({

      ok: true,

      service:
        "MT5 FOREX AI ANALYZER V12",

      frontend:
        FRONTEND_VERSION,

      server:
        SERVER_VERSION,

      time:
        new Date().toISOString(),

      mt5Online:
        state.mt5.online,

      aiProviders:
        Object.keys(AI_CONFIG)

    });

  }
);


/* =========================================================
   VERSION
========================================================= */

app.get(
  "/api/version",
  (req, res) => {

    res.json({

      success: true,

      frontend:
        FRONTEND_VERSION,

      server:
        SERVER_VERSION

    });

  }
);


/* =========================================================
   DASHBOARD
========================================================= */

app.get(
  "/api/dashboard",
  requireAuth,
  (req, res) => {

    refreshSafety();

    res.setHeader(
      "Cache-Control",
      "no-store"
    );

    res.json(
      buildDashboard()
    );

  }
);


/* =========================================================
   AI STATUS
========================================================= */

app.get(
  "/api/ai/status",
  requireAuth,
  (req, res) => {

    res.json({

      success: true,

      providers:
        Object.keys(AI_CONFIG)
          .map(name => {

            return {

              provider: name,

              configured:
                Boolean(
                  AI_CONFIG[name].key
                ),

              status:
                state.ai[name]?.status ||
                (
                  AI_CONFIG[name].key
                    ? "READY"
                    : "NOT_CONFIGURED"
                )

            };

          }),

      lastUpdate:
        state.lastAIUpdate

    });

  }
);


/* =========================================================
   AI MANUAL SCAN
========================================================= */

app.post(
  "/api/ai/scan",
  requireAuth,
  async (req, res) => {

    try{

      const result =
        await runAIConsensus();

      res.json({

        success: true,

        result

      });

    }catch(error){

      console.error(
        "AI scan error:",
        error
      );

      res.status(500).json({

        success: false,

        message:
          "AI scan failed."

      });

    }

  }
);


/* =========================================================
   MT5 PUSH
========================================================= */

app.post(
  "/api/mt5/push",
  (req, res) => {

    if(
      MT5_BRIDGE_SECRET
    ){

      const provided =
        String(
          req.headers["x-mt5-secret"] ||
          ""
        );

      if(
        provided !==
        MT5_BRIDGE_SECRET
      ){

        return res.status(401).json({

          success: false,

          message:
            "Invalid MT5 bridge secret."

        });

      }

    }


    try{

      const data =
        normalizeMT5Payload(
          req.body
        );


      updateMT5State(data);


      broadcast({

        type: "mt5",

        data:
          state.mt5

      });


      refreshAnalysis();


      broadcastDashboard();


      return res.json({

        success: true,

        message:
          "MT5 data accepted.",

        symbol:
          state.mt5.symbol,

        timeframe:
          state.mt5.timeframe,

        candles:
          state.mt5.candles.length

      });

    }catch(error){

      console.error(
        "MT5 push error:",
        error
      );

      return res.status(400).json({

        success: false,

        message:
          error.message ||
          "Invalid MT5 payload."

      });

    }

  }
);


/* =========================================================
   ROOT PAGE
========================================================= */

app.get(
  "/",
  (req, res) => {

    res.setHeader(
      "Cache-Control",
      "no-store, no-cache, must-revalidate, proxy-revalidate"
    );

    res.setHeader(
      "Pragma",
      "no-cache"
    );

    res.setHeader(
      "Expires",
      "0"
    );

    res.sendFile(
      INDEX_FILE
    );

  }
);


/* =========================================================
   SPA FALLBACK
========================================================= */

app.use(
  (req, res, next) => {

    if(
      req.method === "GET" &&
      !req.path.startsWith("/api/") &&
      req.path !== "/health"
    ){

      return res.sendFile(
        INDEX_FILE
      );

    }

    next();

  }
);


/* =========================================================
   API 404
========================================================= */

app.use(
  "/api",
  (req, res) => {

    res.status(404).json({

      success: false,

      message:
        "API endpoint not found."

    });

  }
);


/* =========================================================
   ERROR HANDLER
========================================================= */

app.use(
  (error, req, res, next) => {

    console.error(
      "SERVER ERROR:",
      error
    );


    if(res.headersSent){

      return next(error);

    }


    res.status(500).json({

      success: false,

      message:
        "Internal server error."

    });

  }
);


/* =========================================================
   MT5 NORMALIZATION
========================================================= */

function normalizeMT5Payload(body){

  if(
    !body ||
    typeof body !== "object"
  ){

    throw new Error(
      "MT5 payload must be an object."
    );

  }


  const source =
    body.data &&
    typeof body.data === "object"
      ? body.data
      : body;


  const candles =
    source.candles ||
    source.rates ||
    source.ohlc ||
    [];


  let normalizedCandles=[];


  if(Array.isArray(candles)){

    normalizedCandles =
      candles
        .map(c => {

          if(
            !c ||
            typeof c !== "object"
          ){

            return null;

          }


          return {

            time:
              c.time ??
              c.Time ??
              c.timestamp ??
              null,

            open:
              numberOrNull(
                c.open ??
                c.Open
              ),

            high:
              numberOrNull(
                c.high ??
                c.High
              ),

            low:
              numberOrNull(
                c.low ??
                c.Low
              ),

            close:
              numberOrNull(
                c.close ??
                c.Close ??
                c.price
              ),

            volume:
              numberOrNull(
                c.volume ??
                c.Volume ??
                0
              )

          };

        })
        .filter(Boolean)
        .filter(
          c =>
            Number.isFinite(c.close)
        )
        .slice(-MAX_CANDLES);

  }


  return {

    symbol:
      String(
        source.symbol ||
        source.Symbol ||
        state.mt5.symbol ||
        "EURUSD"
      ),

    timeframe:
      String(
        source.timeframe ||
        source.Timeframe ||
        state.mt5.timeframe ||
        "M1"
      ),

    price:
      numberOrNull(
        source.price ??
        source.last ??
        source.Last ??
        source.bid ??
        source.Bid
      ),

    bid:
      numberOrNull(
        source.bid ??
        source.Bid
      ),

    ask:
      numberOrNull(
        source.ask ??
        source.Ask
      ),

    spread:
      numberOrNull(
        source.spread ??
        source.spreadPoints
      ),

    spreadPoints:
      numberOrNull(
        source.spreadPoints ??
        source.spread
      ),

    tick:
      source.tick ??
      source.tickValue ??
      source.timestamp ??
      Date.now(),

    timestamp:
      source.timestamp ??
      source.time ??
      source.serverTime ??
      Date.now(),

    connected:
      source.connected === true ||
      source.online === true ||
      source.status === "online",

    candles:
      normalizedCandles

  };

}


function numberOrNull(value){

  const n =
    Number(value);

  return Number.isFinite(n)
    ? n
    : null;

}


/* =========================================================
   UPDATE MT5 STATE
========================================================= */

function updateMT5State(data){

  state.mt5.symbol =
    data.symbol;

  state.mt5.timeframe =
    data.timeframe;


  if(data.price !== null){

    state.mt5.price =
      data.price;

  }


  if(data.bid !== null){

    state.mt5.bid =
      data.bid;

  }


  if(data.ask !== null){

    state.mt5.ask =
      data.ask;

  }


  if(data.spread !== null){

    state.mt5.spread =
      data.spread;

  }


  if(
    data.spreadPoints !== null
  ){

    state.mt5.spreadPoints =
      data.spreadPoints;

  }


  state.mt5.tick =
    data.tick;


  state.mt5.timestamp =
    data.timestamp;


  state.mt5.candles =
    data.candles.length
      ? data.candles
      : state.mt5.candles;


  state.mt5.connected =
    data.connected !== false;

  state.mt5.online =
    true;


  state.lastMT5Update =
    Date.now();

}


/* =========================================================
   TECHNICAL ENGINE
========================================================= */

function refreshAnalysis(){

  const candles =
    state.mt5.candles;


  if(
    !Array.isArray(candles) ||
    candles.length < 2
  ){

    state.technical = {

      ...state.technical,

      decision: "WAIT",

      trend: "WAIT",

      regime: "WAIT"

    };

    refreshSafety();

    return;

  }


  const closes =
    candles
      .map(c =>
        Number(c.close)
      )
      .filter(Number.isFinite);


  const highs =
    candles
      .map(c =>
        Number(c.high)
      )
      .filter(Number.isFinite);


  const lows =
    candles
      .map(c =>
        Number(c.low)
      )
      .filter(Number.isFinite);


  const ema20 =
    calculateEMA(
      closes,
      20
    );


  const ema50 =
    calculateEMA(
      closes,
      50
    );


  const ema200 =
    calculateEMA(
      closes,
      200
    );


  const rsi =
    calculateRSI(
      closes,
      14
    );


  const atr =
    calculateATR(
      candles,
      14
    );


  const momentum =
    calculateMomentum(
      closes
    );


  const trend =
    determineTrend(
      closes,
      ema20,
      ema50,
      ema200
    );


  const support =
    lows.length
      ? Math.min(
          ...lows.slice(-50)
        )
      : null;


  const resistance =
    highs.length
      ? Math.max(
          ...highs.slice(-50)
        )
      : null;


  const candleStructure =
    determineCandleStructure(
      candles
    );


  const technicalDecision =
    determineTechnicalDecision({

      price:
        state.mt5.price,

      rsi,

      ema20,

      ema50,

      ema200,

      momentum,

      trend

    });


  const regime =
    determineRegime({

      trend,

      momentum,

      rsi

    });


  state.technical = {

    rsi,

    ema20,

    ema50,

    ema200,

    atr,

    momentum,

    trend,

    candleStructure,

    support,

    resistance,

    decision:
      technicalDecision,

    regime

  };


  refreshSafety();

}


/* =========================================================
   EMA
========================================================= */

function calculateEMA(values, period){

  if(!values.length){
    return null;
  }


  const usable =
    values.slice(
      -Math.max(
        values.length,
        period
      )
    );


  const multiplier =
    2 / (period + 1);


  let ema =
    usable[0];


  for(
    let i=1;
    i<usable.length;
    i++
  ){

    ema =
      (
        usable[i] -
        ema
      ) *
      multiplier +
      ema;

  }


  return roundNumber(ema);

}


/* =========================================================
   RSI
========================================================= */

function calculateRSI(values, period){

  if(
    values.length <
    period + 1
  ){

    return null;

  }


  const slice =
    values.slice(
      -(period + 1)
    );


  let gains=0;
  let losses=0;


  for(
    let i=1;
    i<slice.length;
    i++
  ){

    const diff =
      slice[i] -
      slice[i-1];


    if(diff > 0){

      gains += diff;

    }else{

      losses +=
        Math.abs(diff);

    }

  }


  if(losses === 0){
    return 100;
  }


  const rs =
    gains / losses;


  return roundNumber(
    100 -
    (
      100 /
      (1 + rs)
    )
  );

}


/* =========================================================
   ATR
========================================================= */

function calculateATR(candles, period){

  if(
    candles.length <
    period + 1
  ){

    return null;

  }


  const trs=[];


  for(
    let i=1;
    i<candles.length;
    i++
  ){

    const current =
      candles[i];

    const previous =
      candles[i-1];


    const high =
      Number(current.high);

    const low =
      Number(current.low);

    const previousClose =
      Number(previous.close);


    if(
      !Number.isFinite(high) ||
      !Number.isFinite(low) ||
      !Number.isFinite(previousClose)
    ){

      continue;

    }


    const tr =
      Math.max(

        high - low,

        Math.abs(
          high -
          previousClose
        ),

        Math.abs(
          low -
          previousClose
        )

      );


    trs.push(tr);

  }


  if(!trs.length){
    return null;
  }


  const recent =
    trs.slice(-period);


  return roundNumber(
    recent.reduce(
      (a,b) => a+b,
      0
    ) /
    recent.length
  );

}


/* =========================================================
   MOMENTUM
========================================================= */

function calculateMomentum(values){

  if(values.length < 6){
    return "NEUTRAL";
  }


  const current =
    values[values.length - 1];


  const previous =
    values[values.length - 6];


  const difference =
    current - previous;


  if(difference > 0){
    return "BULLISH";
  }


  if(difference < 0){
    return "BEARISH";
  }


  return "NEUTRAL";

}


/* =========================================================
   TREND
========================================================= */

function determineTrend(
  closes,
  ema20,
  ema50,
  ema200
){

  if(
    !Number.isFinite(
      closes.at(-1)
    )
  ){

    return "WAIT";

  }


  const price =
    closes.at(-1);


  if(
    Number.isFinite(ema20) &&
    Number.isFinite(ema50) &&
    Number.isFinite(ema200)
  ){

    if(
      price > ema20 &&
      ema20 > ema50 &&
      ema50 > ema200
    ){

      return "BULLISH";

    }


    if(
      price < ema20 &&
      ema20 < ema50 &&
      ema50 < ema200
    ){

      return "BEARISH";

    }

  }


  if(
    Number.isFinite(ema20)
  ){

    if(price > ema20){
      return "BULLISH";
    }

    if(price < ema20){
      return "BEARISH";
    }

  }


  return "NEUTRAL";

}


/* =========================================================
   CANDLE STRUCTURE
========================================================= */

function determineCandleStructure(candles){

  if(
    !candles.length
  ){

    return "WAIT";

  }


  const c =
    candles.at(-1);


  const open =
    Number(c.open);

  const high =
    Number(c.high);

  const low =
    Number(c.low);

  const close =
    Number(c.close);


  if(
    ![
      open,
      high,
      low,
      close
    ].every(
      Number.isFinite
    )
  ){

    return "WAIT";

  }


  const body =
    Math.abs(
      close - open
    );


  const upper =
    high -
    Math.max(
      open,
      close
    );


  const lower =
    Math.min(
      open,
      close
    ) -
    low;


  if(
    lower > body * 2 &&
    upper < body
  ){

    return "BULLISH REJECTION";

  }


  if(
    upper > body * 2 &&
    lower < body
  ){

    return "BEARISH REJECTION";

  }


  if(close > open){
    return "BULLISH";
  }


  if(close < open){
    return "BEARISH";
  }


  return "DOJI";

}


/* =========================================================
   TECHNICAL DECISION
========================================================= */

function determineTechnicalDecision(data){

  const {
    price,
    rsi,
    ema20,
    ema50,
    ema200,
    momentum,
    trend
  } = data;


  if(
    !Number.isFinite(price)
  ){

    return "WAIT";

  }


  let buyScore=0;
  let sellScore=0;


  if(trend === "BULLISH"){
    buyScore += 2;
  }


  if(trend === "BEARISH"){
    sellScore += 2;
  }


  if(momentum === "BULLISH"){
    buyScore += 1;
  }


  if(momentum === "BEARISH"){
    sellScore += 1;
  }


  if(
    Number.isFinite(rsi)
  ){

    if(
      rsi > 50 &&
      rsi < 70
    ){

      buyScore += 1;

    }


    if(
      rsi < 50 &&
      rsi > 30
    ){

      sellScore += 1;

    }

  }


  if(
    Number.isFinite(ema20) &&
    price > ema20
  ){

    buyScore += 1;

  }


  if(
    Number.isFinite(ema20) &&
    price < ema20
  ){

    sellScore += 1;

  }


  if(
    buyScore >= 4 &&
    buyScore > sellScore
  ){

    return "BUY";

  }


  if(
    sellScore >= 4 &&
    sellScore > buyScore
  ){

    return "SELL";

  }


  return "WAIT";

}


/* =========================================================
   MARKET REGIME
========================================================= */

function determineRegime(data){

  if(
    data.trend === "BULLISH" &&
    data.momentum === "BULLISH"
  ){

    return "TRENDING BULLISH";

  }


  if(
    data.trend === "BEARISH" &&
    data.momentum === "BEARISH"
  ){

    return "TRENDING BEARISH";

  }


  if(
    data.rsi !== null &&
    (
      data.rsi >= 70 ||
      data.rsi <= 30
    )
  ){

    return "EXTENDED";

  }


  return "RANGING / MIXED";

}


/* =========================================================
   SAFETY
========================================================= */

function refreshSafety(){

  const stale =
    !state.lastMT5Update ||
    Date.now() -
    state.lastMT5Update >
    STALE_DATA_MS;


  let spreadStatus =
    "WAITING";


  const spread =
    state.mt5.spreadPoints ??
    state.mt5.spread;


  if(
    Number.isFinite(spread)
  ){

    spreadStatus =
      spread <= MAX_SPREAD_POINTS
        ? "SAFE"
        : "HIGH SPREAD";

  }


  state.safety = {

    spreadStatus,

    dailyLoss:
      "0%",

    maxPositions:
      String(MAX_POSITIONS),

    cooldown:
      cooldownRemaining(),

    newsRisk:
      state.newsRisk ||
      "UNKNOWN",

    dataStatus:
      stale
        ? "STALE"
        : state.lastMT5Update
          ? "LIVE"
          : "WAITING"

  };

}


function cooldownRemaining(){

  if(!state.lastActionTime){

    return "READY";

  }


  const elapsed =
    (
      Date.now() -
      state.lastActionTime
    ) / 1000;


  const remaining =
    COOLDOWN_SECONDS -
    elapsed;


  if(remaining <= 0){

    return "READY";

  }


  return (
    Math.ceil(
      remaining
    ) +
    "s"
  );

}


/* =========================================================
   FINAL DECISION
========================================================= */

function buildFinalDecision(){

  const technical =
    state.technical.decision;


  const consensus =
    state.consensus;


  const stale =
    state.safety.dataStatus !== "LIVE";


  if(stale){

    return {

      action:"WAIT",

      confidence:0,

      reason:
        "MT5 data is stale or unavailable.",

      entry:null,

      sl:null,

      tp:null

    };

  }


  if(
    state.safety.spreadStatus ===
    "HIGH SPREAD"
  ){

    return {

      action:"WAIT",

      confidence:0,

      reason:
        "Spread safety filter blocked the setup.",

      entry:null,

      sl:null,

      tp:null

    };

  }


  const buy =
    Number(
      consensus.buy
    ) || 0;


  const sell =
    Number(
      consensus.sell
    ) || 0;


  const agreement =
    Number(
      consensus.agreement
    ) || 0;


  let action =
    "WAIT";


  if(
    technical === "BUY" &&
    buy > sell &&
    agreement >= 50
  ){

    action="BUY";

  }


  if(
    technical === "SELL" &&
    sell > buy &&
    agreement >= 50
  ){

    action="SELL";

  }


  const confidence =
    action === "WAIT"
      ? Math.round(
          Math.max(
            buy,
            sell
          )
        )
      : Math.round(
          (
            Math.max(
              buy,
              sell
            ) * 0.65
          ) +
          (
            agreement * 0.35
          )
        );


  let reason =
    "Waiting for technical and AI confirmation.";


  if(action === "BUY"){

    reason =
      "AI consensus and technical engine support BUY.";

  }


  if(action === "SELL"){

    reason =
      "AI consensus and technical engine support SELL.";

  }


  const price =
    state.mt5.price;


  const atr =
    state.technical.atr;


  let sl=null;
  let tp=null;


  if(
    action !== "WAIT" &&
    Number.isFinite(price) &&
    Number.isFinite(atr) &&
    atr > 0
  ){

    if(action === "BUY"){

      sl =
        roundPrice(
          price - atr * 1.5
        );

      tp =
        roundPrice(
          price + atr * 3
        );

    }else{

      sl =
        roundPrice(
          price + atr * 1.5
        );

      tp =
        roundPrice(
          price - atr * 3
        );

    }

  }


  return {

    action,

    confidence,

    reason,

    entry:
      Number.isFinite(price)
        ? roundPrice(price)
        : null,

    sl,

    tp

  };

}


/* =========================================================
   DASHBOARD
========================================================= */

function buildDashboard(){

  const finalDecision =
    buildFinalDecision();


  state.finalDecision =
    finalDecision;


  return {

    success:true,

    version:
      SERVER_VERSION,

    mt5: {

      ...state.mt5

    },

    technical: {

      ...state.technical

    },

    ai: {

      ...state.ai

    },

    consensus: {

      ...state.consensus,

      technicalConfirmation:
        state.technical.decision

    },

    finalDecision: {

      ...finalDecision

    },

    safety: {

      ...state.safety

    },

    news: {

      ...state.news

    },

    preNews: {

      ...state.preNews

    },

    newsVerification: {

      ...state.newsVerification

    },

    marketRegime:
      state.technical.regime,

    timestamp:
      Date.now()

  };

}


/* =========================================================
   AI PROMPT
========================================================= */

function buildAIPrompt(){

  const technical =
    state.technical;


  const market =
    state.mt5;


  return `
You are one analyst inside an MT5 forex market analysis system.

Do NOT place trades.
Do NOT claim certainty.
Return ONLY JSON.

Market:
Symbol: ${market.symbol}
Timeframe: ${market.timeframe}
Price: ${market.price}

Technical:
RSI14: ${technical.rsi}
EMA20: ${technical.ema20}
EMA50: ${technical.ema50}
EMA200: ${technical.ema200}
ATR14: ${technical.atr}
Momentum: ${technical.momentum}
Trend: ${technical.trend}
Candle: ${technical.candleStructure}
Support: ${technical.support}
Resistance: ${technical.resistance}

Return exactly:

{
  "action": "BUY|SELL|WAIT",
  "confidence": 0,
  "reason": "short explanation"
}

Confidence must be between 0 and 100.
`.trim();

}


/* =========================================================
   AI CONSENSUS
========================================================= */

async function runAIConsensus(){

  const prompt =
    buildAIPrompt();


  const providers =
    Object.keys(AI_CONFIG);


  const tasks =
    providers.map(
      provider =>
        callAIProvider(
          provider,
          prompt
        )
    );


  const results =
    await Promise.all(
      tasks
    );


  const ai={};


  results.forEach(result => {

    ai[result.provider] =
      result;

  });


  state.ai=ai;

  state.lastAIUpdate =
    Date.now();


  calculateConsensus();


  broadcast({

    type:"ai",

    data:
      state.ai

  });


  broadcast({

    type:"dashboard",

    data:
      buildDashboard()

  });


  return {

    ai:
      state.ai,

    consensus:
      state.consensus,

    finalDecision:
      state.finalDecision

  };

}


/* =========================================================
   PROVIDER DISPATCH
========================================================= */

async function callAIProvider(
  provider,
  prompt
){

  const config =
    AI_CONFIG[provider];


  if(!config.key){

    return {

      provider,

      status:
        "NOT_CONFIGURED",

      action:"WAIT",

      confidence:0,

      reason:
        "API key not configured."

    };

  }


  try{

    let result;


    if(provider === "openai"){

      result =
        await callOpenAI(
          config,
          prompt
        );

    }else if(provider === "deepseek"){

      result =
        await callDeepSeek(
          config,
          prompt
        );

    }else if(provider === "gemini"){

      result =
        await callGemini(
          config,
          prompt
        );

    }else if(provider === "groq"){

      result =
        await callGroq(
          config,
          prompt
        );

    }else if(provider === "openrouter"){

      result =
        await callOpenRouter(
          config,
          prompt
        );

    }else if(provider === "mistral"){

      result =
        await callMistral(
          config,
          prompt
        );

    }else{

      throw new Error(
        "Unknown AI provider."
      );

    }


    const normalized =
      normalizeAIResult(
        result
      );


    return {

      provider,

      status:"ONLINE",

      ...normalized

    };

  }catch(error){

    console.error(
      provider +
      " AI error:",
      error.message
    );


    return {

      provider,

      status:"ERROR",

      action:"WAIT",

      confidence:0,

      reason:
        error.message ||
        "AI provider error."

    };

  }

}


/* =========================================================
   GENERIC CHAT API
========================================================= */

async function callChatAPI(
  config,
  prompt,
  baseURL,
  headers={}
){

  const controller =
    new AbortController();


  const timer =
    setTimeout(
      () =>
        controller.abort(),
      30000
    );


  try{

    const response =
      await fetch(
        baseURL,
        {

          method:"POST",

          headers:{

            "Content-Type":
              "application/json",

            ...headers

          },

          body:
            JSON.stringify({

              model:
                config.model,

              messages:[

                {

                  role:"system",

                  content:
                    "Return valid JSON only."

                },

                {

                  role:"user",

                  content:
                    prompt

                }

              ],

              temperature:0.1

            }),

          signal:
            controller.signal

        }
      );


    const text =
      await response.text();


    if(!response.ok){

      throw new Error(
        "HTTP " +
        response.status +
        ": " +
        text.slice(0,300)
      );

    }


    let json={};


    try{

      json =
        JSON.parse(text);

    }catch(error){

      throw new Error(
        "Invalid provider JSON response."
      );

    }


    return extractChatContent(
      json
    );

  }finally{

    clearTimeout(timer);

  }

}


/* =========================================================
   OPENAI
========================================================= */

async function callOpenAI(
  config,
  prompt
){

  return callChatAPI(

    config,

    prompt,

    "https://api.openai.com/v1/chat/completions",

    {

      Authorization:
        "Bearer " +
        config.key

    }

  );

}


/* =========================================================
   DEEPSEEK
========================================================= */

async function callDeepSeek(
  config,
  prompt
){

  return callChatAPI(

    config,

    prompt,

    "https://api.deepseek.com/chat/completions",

    {

      Authorization:
        "Bearer " +
        config.key

    }

  );

}


/* =========================================================
   GROQ
========================================================= */

async function callGroq(
  config,
  prompt
){

  return callChatAPI(

    config,

    prompt,

    "https://api.groq.com/openai/v1/chat/completions",

    {

      Authorization:
        "Bearer " +
        config.key

    }

  );

}


/* =========================================================
   OPENROUTER
========================================================= */

async function callOpenRouter(
  config,
  prompt
){

  return callChatAPI(

    config,

    prompt,

    "https://openrouter.ai/api/v1/chat/completions",

    {

      Authorization:
        "Bearer " +
        config.key,

      "HTTP-Referer":
        process.env.APP_URL ||
        "http://localhost:10000",

      "X-Title":
        "MT5 FOREX AI ANALYZER V12"

    }

  );

}


/* =========================================================
   MISTRAL
========================================================= */

async function callMistral(
  config,
  prompt
){

  return callChatAPI(

    config,

    prompt,

    "https://api.mistral.ai/v1/chat/completions",

    {

      Authorization:
        "Bearer " +
        config.key

    }

  );

}


/* =========================================================
   GEMINI
========================================================= */

async function callGemini(
  config,
  prompt
){

  const controller =
    new AbortController();


  const timer =
    setTimeout(
      () =>
        controller.abort(),
      30000
    );


  try{

    const url =
      "https://generativelanguage.googleapis.com/v1beta/models/" +
      encodeURIComponent(
        config.model
      ) +
      ":generateContent?key=" +
      encodeURIComponent(
        config.key
      );


    const response =
      await fetch(
        url,
        {

          method:"POST",

          headers:{
            "Content-Type":
              "application/json"
          },

          body:
            JSON.stringify({

              contents:[

                {

                  parts:[

                    {

                      text:
                        prompt

                    }

                  ]

                }

              ],

              generationConfig:{

                temperature:0.1,

                responseMimeType:
                  "application/json"

              }

            }),

          signal:
            controller.signal

        }
      );


    const text =
      await response.text();


    if(!response.ok){

      throw new Error(
        "HTTP " +
        response.status +
        ": " +
        text.slice(0,300)
      );

    }


    const json =
      JSON.parse(text);


    return (
      json
        ?.candidates?.[0]
        ?.content?.parts?.[0]
        ?.text ||
      ""
    );

  }finally{

    clearTimeout(timer);

  }

}


/* =========================================================
   EXTRACT CHAT CONTENT
========================================================= */

function extractChatContent(json){

  const content =
    json
      ?.choices?.[0]
      ?.message?.content;


  if(typeof content === "string"){

    return content;

  }


  if(Array.isArray(content)){

    return content
      .map(part =>
        part?.text || ""
      )
      .join("");

  }


  return JSON.stringify(
    json
  );

}


/* =========================================================
   NORMALIZE AI
========================================================= */

function normalizeAIResult(raw){

  let value =
    raw;


  if(typeof raw === "string"){

    value =
      raw
        .trim()
        .replace(
          /^```json/i,
          ""
        )
        .replace(
          /^```/i,
          ""
        )
        .replace(
          /```$/i,
          ""
        )
        .trim();


    try{

      value =
        JSON.parse(value);

    }catch(error){

      const match =
        value.match(
          /\{[\s\S]*\}/
        );


      if(match){

        try{

          value =
            JSON.parse(
              match[0]
            );

        }catch(error2){

          value={};

        }

      }else{

        value={};

      }

    }

  }


  const action =
    String(
      value?.action ||
      value?.decision ||
      value?.signal ||
      "WAIT"
    ).toUpperCase();


  const safeAction =
    [
      "BUY",
      "SELL",
      "WAIT"
    ].includes(action)
      ? action
      : "WAIT";


  let confidence =
    Number(
      value?.confidence ??
      value?.score ??
      0
    );


  if(
    !Number.isFinite(confidence)
  ){

    confidence=0;

  }


  confidence =
    Math.max(
      0,
      Math.min(
        100,
        confidence
      )
    );


  return {

    action:
      safeAction,

    confidence:
      Math.round(
        confidence
      ),

    reason:
      String(
        value?.reason ||
        "No reason supplied."
      )

  };

}


/* =========================================================
   CONSENSUS
========================================================= */

function calculateConsensus(){

  const analysts =
    Object.values(
      state.ai
    )
      .filter(
        item =>
          item &&
          item.status === "ONLINE"
      );


  if(!analysts.length){

    state.consensus = {

      buy:0,

      sell:0,

      wait:100,

      agreement:0,

      technicalConfirmation:
        state.technical.decision

    };

    return;

  }


  let buy=0;
  let sell=0;
  let wait=0;


  analysts.forEach(
    item => {

      if(item.action === "BUY"){
        buy++;
      }else if(item.action === "SELL"){
        sell++;
      }else{
        wait++;
      }

    }
  );


  const total =
    analysts.length;


  const buyPercent =
    Math.round(
      buy / total * 100
    );


  const sellPercent =
    Math.round(
      sell / total * 100
    );


  const waitPercent =
    Math.max(
      0,
      100 -
      buyPercent -
      sellPercent
    );


  const strongest =
    Math.max(
      buy,
      sell,
      wait
    );


  const agreement =
    Math.round(
      strongest /
      total *
      100
    );


  state.consensus = {

    buy:
      buyPercent,

    sell:
      sellPercent,

    wait:
      waitPercent,

    agreement,

    technicalConfirmation:
      state.technical.decision

  };


  state.finalDecision =
    buildFinalDecision();

}


/* =========================================================
   REFRESH ANALYSIS
========================================================= */

let aiRunning=false;


async function refreshAnalysisAI(){

  if(aiRunning){
    return;
  }


  if(
    !state.mt5.online ||
    !state.lastMT5Update
  ){

    return;

  }


  const age =
    Date.now() -
    state.lastMT5Update;


  if(age > STALE_DATA_MS){

    return;

  }


  aiRunning=true;


  try{

    await runAIConsensus();

  }catch(error){

    console.error(
      "AI refresh failed:",
      error
    );

  }finally{

    aiRunning=false;

  }

}


/* =========================================================
   WEBSOCKET
========================================================= */

wss.on(
  "connection",
  (ws, req) => {

    ws.isAlive=true;

    ws.symbol =
      state.mt5.symbol;

    ws.timeframe =
      state.mt5.timeframe;


    ws.on(
      "pong",
      () => {

        ws.isAlive=true;

      }
    );


    ws.send(
      JSON.stringify({

        type:"dashboard",

        data:
          buildDashboard()

      })
    );


    ws.send(
      JSON.stringify({

        type:"log",

        level:"info",

        message:
          "WebSocket connected."

      })
    );


    ws.on(
      "message",
      raw => {

        try{

          const message =
            JSON.parse(
              raw.toString()
            );


          if(
            message.type ===
            "subscribe"
          ){

            if(
              message.symbol
            ){

              ws.symbol =
                String(
                  message.symbol
                );

            }


            if(
              message.timeframe
            ){

              ws.timeframe =
                String(
                  message.timeframe
                );

            }


            ws.send(
              JSON.stringify({

                type:"dashboard",

                data:
                  buildDashboard()

              })
            );


            return;

          }


          if(
            message.type ===
            "ping"
          ){

            ws.send(
              JSON.stringify({

                type:"pong",

                time:
                  Date.now()

              })
            );

          }

        }catch(error){

          ws.send(
            JSON.stringify({

              type:"log",

              level:"error",

              message:
                "Invalid WebSocket message."

            })
          );

        }

      }
    );


    ws.on(
      "close",
      () => {

        ws.isAlive=false;

      }
    );


    ws.on(
      "error",
      error => {

        console.error(
          "WebSocket error:",
          error.message
        );

      }
    );

  }
);


/* =========================================================
   WEBSOCKET HEARTBEAT
========================================================= */

const heartbeat =
  setInterval(
    () => {

      wss.clients.forEach(
        ws => {

          if(
            ws.isAlive === false
          ){

            return ws.terminate();

          }


          ws.isAlive=false;

          try{

            ws.ping();

          }catch(error){

            ws.terminate();

          }

        }
      );

    },
    30000
  );


wss.on(
  "close",
  () => {

    clearInterval(
      heartbeat
    );

  }
);


/* =========================================================
   BROADCAST
========================================================= */

function broadcast(message){

  const payload =
    JSON.stringify(
      message
    );


  wss.clients.forEach(
    ws => {

      if(
        ws.readyState ===
        WebSocket.OPEN
      ){

        try{

          ws.send(payload);

        }catch(error){

          console.error(
            "Broadcast error:",
            error.message
          );

        }

      }

    }
  );

}


/* =========================================================
   BROADCAST DASHBOARD
========================================================= */

function broadcastDashboard(){

  broadcast({

    type:"dashboard",

    data:
      buildDashboard()

  });

}


/* =========================================================
   MT5 WATCHDOG
========================================================= */

setInterval(
  () => {

    if(
      state.lastMT5Update &&
      Date.now() -
      state.lastMT5Update >
      STALE_DATA_MS
    ){

      state.mt5.online=false;

      state.mt5.connected=false;

      refreshSafety();


      broadcast({

        type:"mt5",

        data:
          state.mt5

      });


      broadcast({

        type:"dashboard",

        data:
          buildDashboard()

      });

    }

  },
  5000
);


/* =========================================================
   AI REFRESH TIMER
========================================================= */

setInterval(
  () => {

    refreshAnalysisAI();

  },
  AI_REFRESH_MS
);


/* =========================================================
   NEWS PLACEHOLDER ENGINE
========================================================= */

function updateNewsEngine(){

  /*
   * This section intentionally does not invent economic
   * calendar events. A real calendar/news adapter should
   * populate state.news.events.
   */

  if(
    !Array.isArray(
      state.news.events
    )
  ){

    state.news.events=[];

  }


  state.news.mode =
    "MONITOR";


  state.safety.newsRisk =
    state.news.events.length
      ? "EVENTS DETECTED"
      : "UNKNOWN";

}


setInterval(
  () => {

    updateNewsEngine();

  },
  30000
);


/* =========================================================
   UTILITY
========================================================= */

function roundNumber(value){

  if(!Number.isFinite(value)){
    return null;
  }


  return Number(
    value.toFixed(5)
  );

}


function roundPrice(value){

  if(!Number.isFinite(value)){
    return null;
  }


  const symbol =
    String(
      state.mt5.symbol || ""
    ).toUpperCase();


  const digits =
    symbol.includes("JPY")
      ? 3
      : symbol.includes("XAU")
        ? 2
        : 5;


  return Number(
    value.toFixed(
      digits
    )
  );

}


/* =========================================================
   PROCESS EVENTS
========================================================= */

process.on(
  "uncaughtException",
  error => {

    console.error(
      "UNCAUGHT EXCEPTION:",
      error
    );

  }
);


process.on(
  "unhandledRejection",
  reason => {

    console.error(
      "UNHANDLED REJECTION:",
      reason
    );

  }
);


/* =========================================================
   START
========================================================= */

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      "================================================="
    );

    console.log(
      "🔥 MT5 FOREX AI ANALYZER V12"
    );

    console.log(
      "POWERED BY ELISY ANALYSIS DEVELOPER"
    );

    console.log(
      "================================================="
    );

    console.log(
      "Server version:",
      SERVER_VERSION
    );

    console.log(
      "Frontend version:",
      FRONTEND_VERSION
    );

    console.log(
      "Port:",
      PORT
    );

    console.log(
      "Public directory:",
      PUBLIC_DIR
    );

    console.log(
      "Index file:",
      INDEX_FILE
    );

    console.log(
      "WebSocket:",
      "/ws"
    );

    console.log(
      "MT5 bridge:",
      MT5_BRIDGE_SECRET
        ? "SECRET ENABLED"
        : "SECRET NOT CONFIGURED"
    );

    console.log(
      "AI providers:",
      Object.keys(AI_CONFIG).join(", ")
    );

    console.log(
      "================================================="
    );

  }
);
