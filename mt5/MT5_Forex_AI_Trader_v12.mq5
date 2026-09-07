//+------------------------------------------------------------------+
//| MT5_Forex_AI_Trader_v12.mq5                                     |
//| MT5 FOREX AI ANALYZER V12                                       |
//| POWERED BY ELISY ANALYSIS DEVELOPER                             |
//|                                                                  |
//| PURPOSE:                                                         |
//|   Sends REAL MT5 market data to the Render AI Analyzer server.   |
//|                                                                  |
//| IMPORTANT:                                                       |
//|   ANALYSIS / DATA BRIDGE ONLY.                                  |
//|   This EA DOES NOT place trades.                                |
//+------------------------------------------------------------------+
#property strict
#property version   "12.0"
#property description "MT5 FOREX AI ANALYZER V12 - Market Data Bridge"
#property description "Sends live MT5 data to Render server"
#property description "No automatic trading"


//====================================================================
// INPUTS
//====================================================================

input string InpServerURL =
   "https://mt5-forex-ai-analyzer-v12.onrender.com/api/mt5/push";

input string InpBridgeSecret =
   "CHANGE_THIS_TO_YOUR_MT5_BRIDGE_SECRET";

input string InpSymbol =
   "EURUSD";

input ENUM_TIMEFRAMES InpTimeframe =
   PERIOD_M1;

input int InpCandleCount =
   500;

input int InpSendIntervalSeconds =
   2;

input int InpWebRequestTimeout =
   10000;

input bool InpSendOnEveryTick =
   true;

input bool InpShowChartComment =
   true;


//====================================================================
// GLOBAL STATE
//====================================================================

string   g_symbol = "";
int      g_digits = 5;

datetime g_lastSendTime = 0;

ulong    g_tickCounter = 0;

bool     g_lastSendOK = false;

string   g_lastServerMessage = "";

int      g_failCount = 0;

datetime g_lastSuccessfulSend = 0;


//====================================================================
// INIT
//====================================================================

int OnInit()
{
   Print("==============================================");
   Print("MT5 FOREX AI ANALYZER V12");
   Print("POWERED BY ELISY ANALYSIS DEVELOPER");
   Print("==============================================");

   // ---------------------------------------------------------------
   // Validate symbol
   // ---------------------------------------------------------------

   g_symbol = InpSymbol;

   if(g_symbol == "")
      g_symbol = _Symbol;

   if(!SymbolSelect(g_symbol,true))
   {
      Print("ERROR: Could not select symbol: ",g_symbol);

      return(INIT_FAILED);
   }

   g_digits =
      (int)SymbolInfoInteger(
         g_symbol,
         SYMBOL_DIGITS
      );


   // ---------------------------------------------------------------
   // Validate settings
   // ---------------------------------------------------------------

   if(InpCandleCount < 50)
   {
      Print("WARNING: Candle count increased to 50.");
   }

   if(InpCandleCount > 500)
   {
      Print("WARNING: Candle count limited to 500.");
   }


   // ---------------------------------------------------------------
   // Timer
   // ---------------------------------------------------------------

   EventSetTimer(
      MathMax(
         1,
         InpSendIntervalSeconds
      )
   );


   // ---------------------------------------------------------------
   // Initial status
   // ---------------------------------------------------------------

   g_lastSendTime = 0;

   g_lastSendOK = false;

   g_failCount = 0;


   UpdateChartComment();


   Print("Symbol: ",g_symbol);

   Print(
      "Timeframe: ",
      EnumToString(InpTimeframe)
   );

   Print(
      "Server: ",
      InpServerURL
   );

   Print(
      "Candle count: ",
      MathMin(
         MathMax(
            InpCandleCount,
            50
         ),
         500
      )
   );

   Print("EA initialized successfully.");

   Print(
      "Remember to allow WebRequest for:",
      " https://mt5-forex-ai-analyzer-v12.onrender.com"
   );


   // ---------------------------------------------------------------
   // Send immediately
   // ---------------------------------------------------------------

   SendMarketData();


   return(INIT_SUCCEEDED);
}


//====================================================================
// DEINIT
//====================================================================

void OnDeinit(
   const int reason
)
{
   EventKillTimer();

   Comment("");

   Print(
      "MT5 FOREX AI ANALYZER V12 stopped. Reason: ",
      reason
   );
}


//====================================================================
// TICK
//====================================================================

void OnTick()
{
   g_tickCounter++;

   if(!InpSendOnEveryTick)
      return;

   datetime now =
      TimeCurrent();

   if(
      g_lastSendTime == 0 ||
      (
         now -
         g_lastSendTime
      ) >= InpSendIntervalSeconds
   )
   {
      SendMarketData();
   }
}


//====================================================================
// TIMER
//====================================================================

void OnTimer()
{
   SendMarketData();
}


//====================================================================
// SEND MARKET DATA
//====================================================================

bool SendMarketData()
{
   g_lastSendTime =
      TimeCurrent();


   // ---------------------------------------------------------------
   // Check symbol
   // ---------------------------------------------------------------

   if(!SymbolSelect(g_symbol,true))
   {
      g_lastServerMessage =
         "Symbol unavailable";

      g_lastSendOK = false;

      UpdateChartComment();

      return false;
   }


   // ---------------------------------------------------------------
   // Get tick
   // ---------------------------------------------------------------

   MqlTick tick;

   if(!SymbolInfoTick(
         g_symbol,
         tick
      ))
   {
      g_lastServerMessage =
         "No tick data";

      g_lastSendOK = false;

      UpdateChartComment();

      return false;
   }


   // ---------------------------------------------------------------
   // Current values
   // ---------------------------------------------------------------

   double bid =
      tick.bid;

   double ask =
      tick.ask;

   double last =
      tick.last;


   if(last <= 0)
      last = bid;


   double point =
      SymbolInfoDouble(
         g_symbol,
         SYMBOL_POINT
      );


   double spreadPoints = 0;

   if(point > 0)
   {
      spreadPoints =
         (ask - bid) /
         point;
   }


   // ---------------------------------------------------------------
   // Candles
   // ---------------------------------------------------------------

   string candlesJSON =
      BuildCandlesJSON();


   // ---------------------------------------------------------------
   // Build JSON
   // ---------------------------------------------------------------

   string json = "";

   json += "{";

   json +=
      "\"symbol\":\"" +
      JsonEscape(g_symbol) +
      "\",";

   json +=
      "\"timeframe\":\"" +
      JsonEscape(
         TimeframeToString(
            InpTimeframe
         )
      ) +
      "\",";

   json +=
      "\"price\":" +
      DoubleToString(
         last,
         g_digits
      ) +
      ",";

   json +=
      "\"last\":" +
      DoubleToString(
         last,
         g_digits
      ) +
      ",";

   json +=
      "\"bid\":" +
      DoubleToString(
         bid,
         g_digits
      ) +
      ",";

   json +=
      "\"ask\":" +
      DoubleToString(
         ask,
         g_digits
      ) +
      ",";

   json +=
      "\"spread\":" +
      DoubleToString(
         spreadPoints,
         2
      ) +
      ",";

   json +=
      "\"spreadPoints\":" +
      DoubleToString(
         spreadPoints,
         2
      ) +
      ",";

   json +=
      "\"tick\":" +
      IntegerToString(
         (long)tick.time_msc
      ) +
      ",";

   json +=
      "\"tickValue\":" +
      IntegerToString(
         (long)tick.time_msc
      ) +
      ",";

   json +=
      "\"timestamp\":" +
      IntegerToString(
         (long)TimeCurrent()
      ) +
      ",";

   json +=
      "\"serverTime\":" +
      IntegerToString(
         (long)TimeCurrent()
      ) +
      ",";

   json +=
      "\"connected\":true,";

   json +=
      "\"online\":true,";

   json +=
      "\"status\":\"online\",";

   json +=
      "\"candles\":" +
      candlesJSON;

   json += "}";


   // ---------------------------------------------------------------
   // Headers
   // ---------------------------------------------------------------

   string headers =
      "Content-Type: application/json\r\n";

   headers +=
      "Accept: application/json\r\n";

   headers +=
      "x-mt5-secret: " +
      InpBridgeSecret +
      "\r\n";


   // ---------------------------------------------------------------
   // Convert body
   // ---------------------------------------------------------------

   uchar postData[];

   uchar result[];

   string resultHeaders;


   StringToCharArray(
      json,
      postData,
      0,
      StringLen(json),
      CP_UTF8
   );


   // ---------------------------------------------------------------
   // WebRequest
   // ---------------------------------------------------------------

   ResetLastError();


   int httpCode =
      WebRequest(
         "POST",
         InpServerURL,
         headers,
         InpWebRequestTimeout,
         postData,
         result,
         resultHeaders
      );


   // ---------------------------------------------------------------
   // WebRequest failure
   // ---------------------------------------------------------------

   if(httpCode == -1)
   {
      int errorCode =
         GetLastError();

      g_lastSendOK = false;

      g_failCount++;

      g_lastServerMessage =
         "WebRequest error " +
         IntegerToString(
            errorCode
         );


      Print(
         "WebRequest failed. Error: ",
         errorCode
      );


      Print(
         "IMPORTANT: Add this URL to MT5 WebRequest allowed list:"
      );

      Print(
         "https://mt5-forex-ai-analyzer-v12.onrender.com"
      );


      UpdateChartComment();

      return false;
   }


   // ---------------------------------------------------------------
   // Read server response
   // ---------------------------------------------------------------

   string response =
      CharArrayToString(
         result,
         0,
         -1,
         CP_UTF8
      );


   g_lastServerMessage =
      response;


   // ---------------------------------------------------------------
   // HTTP success
   // ---------------------------------------------------------------

   if(
      httpCode >= 200 &&
      httpCode < 300
   )
   {
      g_lastSendOK = true;

      g_failCount = 0;

      g_lastSuccessfulSend =
         TimeCurrent();


      Print(
         "✅ MT5 data sent successfully. HTTP ",
         httpCode
      );


      UpdateChartComment();

      return true;
   }


   // ---------------------------------------------------------------
   // HTTP failure
   // ---------------------------------------------------------------

   g_lastSendOK = false;

   g_failCount++;


   Print(
      "❌ Server rejected MT5 data. HTTP ",
      httpCode
   );

   Print(
      "Server response: ",
      response
   );


   UpdateChartComment();


   return false;
}


//====================================================================
// BUILD CANDLES JSON
//====================================================================

string BuildCandlesJSON()
{
   int requested =
      InpCandleCount;


   requested =
      MathMax(
         50,
         requested
      );


   requested =
      MathMin(
         500,
         requested
      );


   MqlRates rates[];


   ArraySetAsSeries(
      rates,
      true
   );


   int copied =
      CopyRates(
         g_symbol,
         InpTimeframe,
         0,
         requested,
         rates
      );


   if(copied <= 0)
   {
      Print(
         "CopyRates failed. Error: ",
         GetLastError()
      );

      return "[]";
   }


   string json = "[";


   // ---------------------------------------------------------------
   // Reverse order:
   // oldest → newest
   // ---------------------------------------------------------------

   for(
      int i = copied - 1;
      i >= 0;
      i--
   )
   {
      if(
         StringLen(json) > 1
      )
      {
         json += ",";
      }


      json += "{";


      json +=
         "\"time\":" +
         IntegerToString(
            (long)rates[i].time
         ) +
         ",";


      json +=
         "\"open\":" +
         DoubleToString(
            rates[i].open,
            g_digits
         ) +
         ",";


      json +=
         "\"high\":" +
         DoubleToString(
            rates[i].high,
            g_digits
         ) +
         ",";


      json +=
         "\"low\":" +
         DoubleToString(
            rates[i].low,
            g_digits
         ) +
         ",";


      json +=
         "\"close\":" +
         DoubleToString(
            rates[i].close,
            g_digits
         ) +
         ",";


      json +=
         "\"tickVolume\":" +
         IntegerToString(
            (long)rates[i].tick_volume
         ) +
         ",";


      json +=
         "\"spread\":" +
         IntegerToString(
            (long)rates[i].spread
         ) +
         ",";


      json +=
         "\"realVolume\":" +
         IntegerToString(
            (long)rates[i].real_volume
         );


      json += "}";
   }


   json += "]";


   return json;
}


//====================================================================
// TIMEFRAME STRING
//====================================================================

string TimeframeToString(
   ENUM_TIMEFRAMES timeframe
)
{
   switch(timeframe)
   {
      case PERIOD_M1:
         return "M1";

      case PERIOD_M2:
         return "M2";

      case PERIOD_M3:
         return "M3";

      case PERIOD_M4:
         return "M4";

      case PERIOD_M5:
         return "M5";

      case PERIOD_M6:
         return "M6";

      case PERIOD_M10:
         return "M10";

      case PERIOD_M12:
         return "M12";

      case PERIOD_M15:
         return "M15";

      case PERIOD_M20:
         return "M20";

      case PERIOD_M30:
         return "M30";

      case PERIOD_H1:
         return "H1";

      case PERIOD_H2:
         return "H2";

      case PERIOD_H3:
         return "H3";

      case PERIOD_H4:
         return "H4";

      case PERIOD_H6:
         return "H6";

      case PERIOD_H8:
         return "H8";

      case PERIOD_H12:
         return "H12";

      case PERIOD_D1:
         return "D1";

      case PERIOD_W1:
         return "W1";

      case PERIOD_MN1:
         return "MN1";
   }


   return "M1";
}


//====================================================================
// JSON ESCAPE
//====================================================================

string JsonEscape(
   string value
)
{
   string result =
      value;


   StringReplace(
      result,
      "\\",
      "\\\\"
   );


   StringReplace(
      result,
      "\"",
      "\\\""
   );


   StringReplace(
      result,
      "\r",
      "\\r"
   );


   StringReplace(
      result,
      "\n",
      "\\n"
   );


   StringReplace(
      result,
      "\t",
      "\\t"
   );


   return result;
}


//====================================================================
// CHART COMMENT
//====================================================================

void UpdateChartComment()
{
   if(!InpShowChartComment)
   {
      Comment("");

      return;
   }


   string status =
      g_lastSendOK
         ? "🟢 CONNECTED"
         : "🔴 DISCONNECTED";


   string lastSend = "--";


   if(g_lastSuccessfulSend > 0)
   {
      lastSend =
         TimeToString(
            g_lastSuccessfulSend,
            TIME_SECONDS
         );
   }


   string comment = "";


   comment +=
      "🔥 MT5 FOREX AI ANALYZER V12\n";


   comment +=
      "POWERED BY ELISY ANALYSIS DEVELOPER\n";


   comment +=
      "------------------------------------\n";


   comment +=
      "Server: " +
      status +
      "\n";


   comment +=
      "Symbol: " +
      g_symbol +
      "\n";


   comment +=
      "Timeframe: " +
      TimeframeToString(
         InpTimeframe
      ) +
      "\n";


   comment +=
      "Last successful send: " +
      lastSend +
      "\n";


   comment +=
      "Failures: " +
      IntegerToString(
         g_failCount
      ) +
      "\n";


   if(
      g_lastServerMessage != ""
   )
   {
      string shortMessage =
         g_lastServerMessage;


      if(
         StringLen(
            shortMessage
         ) > 150
      )
      {
         shortMessage =
            StringSubstr(
               shortMessage,
               0,
               150
            );
      }


      comment +=
         "Server: " +
         shortMessage +
         "\n";
   }


   comment +=
      "------------------------------------\n";


   comment +=
      "ANALYSIS / DATA BRIDGE ONLY\n";


   comment +=
      "AUTO TRADING: DISABLED";


   Comment(comment);
}
