//+------------------------------------------------------------------+
//| MT5 Forex AI Trader V12 - Data Bridge                             |
//| ELISY ANALYSIS DEVELOPER                                         |
//| ANALYSIS ONLY by default                                         |
//+------------------------------------------------------------------+
#property strict
#property version   "12.0"
#property description "MT5 -> Node.js live market data bridge"

input string ServerURL = "https://YOUR-RENDER-SERVICE.onrender.com";
input string BridgeSecret = "CHANGE_THIS_SECRET";

input ENUM_TIMEFRAMES DataTimeframe = PERIOD_M5;
input int CandleCount = 200;

input bool AnalysisOnly = true;
input bool SendEveryTick = true;
input int SendIntervalMilliseconds = 1000;

datetime lastSend = 0;
ulong lastTickMilliseconds = 0;

//+------------------------------------------------------------------+
//| Expert initialization                                             |
//+------------------------------------------------------------------+
int OnInit()
{
   if(StringLen(ServerURL) < 10)
   {
      Print("ERROR: Set ServerURL first.");
      return(INIT_FAILED);
   }

   if(StringLen(BridgeSecret) < 10 ||
      BridgeSecret == "CHANGE_THIS_SECRET")
   {
      Print("ERROR: Set a real BridgeSecret first.");
      return(INIT_FAILED);
   }

   EventSetMillisecondTimer(
      MathMax(250, SendIntervalMilliseconds)
   );

   Print("==============================================");
   Print("MT5 FOREX AI ANALYZER V12");
   Print("ELISY ANALYSIS DEVELOPER");
   Print("Bridge initialized.");
   Print("Symbol: ", _Symbol);
   Print("Timeframe: ", EnumToString(DataTimeframe));
   Print("Analysis Only: ", AnalysisOnly);
   Print("==============================================");

   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization                                           |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();

   Print("MT5 V12 bridge stopped. Reason: ", reason);
}

//+------------------------------------------------------------------+
//| Tick event                                                        |
//+------------------------------------------------------------------+
void OnTick()
{
   if(!SendEveryTick)
      return;

   ulong nowMs = GetTickCount64();

   if(lastTickMilliseconds != 0 &&
      nowMs - lastTickMilliseconds <
      (ulong)SendIntervalMilliseconds)
   {
      return;
   }

   lastTickMilliseconds = nowMs;

   SendMarketData();
}

//+------------------------------------------------------------------+
//| Timer                                                             |
//+------------------------------------------------------------------+
void OnTimer()
{
   if(!SendEveryTick)
      SendMarketData();
}

//+------------------------------------------------------------------+
//| Send market data                                                  |
//+------------------------------------------------------------------+
void SendMarketData()
{
   MqlTick tick;

   if(!SymbolInfoTick(_Symbol, tick))
   {
      Print("Unable to read current tick.");
      return;
   }

   int digits = (int)SymbolInfoInteger(
      _Symbol,
      SYMBOL_DIGITS
   );

   double point = SymbolInfoDouble(
      _Symbol,
      SYMBOL_POINT
   );

   double spreadPoints = 0;

   if(point > 0)
      spreadPoints = (tick.ask - tick.bid) / point;

   string json = "{";

   json += "\"type\":\"mt5_tick\",";
   json += "\"symbol\":\"" + JsonEscape(_Symbol) + "\",";
   json += "\"timeframe\":\"" +
           JsonEscape(EnumToString(DataTimeframe)) + "\",";

   json += "\"price\":" +
           DoubleToString(tick.last > 0 ? tick.last : tick.bid, digits) + ",";

   json += "\"bid\":" +
           DoubleToString(tick.bid, digits) + ",";

   json += "\"ask\":" +
           DoubleToString(tick.ask, digits) + ",";

   json += "\"spread\":" +
           DoubleToString(spreadPoints, 2) + ",";

   json += "\"time\":\"" +
           TimeToString(
              TimeCurrent(),
              TIME_DATE | TIME_SECONDS
           ) +
           "\",";

   json += "\"timestamp\":" +
           IntegerToString((long)TimeCurrent()) + ",";

   json += "\"candles\":";

   json += BuildCandlesJSON(digits);

   json += ",";

   json += "\"indicators\":";

   json += BuildIndicatorsJSON();

   json += ",";

   json += "\"analysisOnly\":" +
           (AnalysisOnly ? "true" : "false");

   json += "}";

   SendToServer(json);
}

//+------------------------------------------------------------------+
//| Build candles JSON                                                |
//+------------------------------------------------------------------+
string BuildCandlesJSON(int digits)
{
   MqlRates rates[];

   ArraySetAsSeries(rates, true);

   int copied = CopyRates(
      _Symbol,
      DataTimeframe,
      0,
      CandleCount,
      rates
   );

   if(copied <= 0)
   {
      return "[]";
   }

   string result = "[";

   // Send oldest -> newest
   for(int i = copied - 1; i >= 0; i--)
   {
      if(i != copied - 1)
         result += ",";

      result += "{";

      result += "\"time\":" +
                IntegerToString((long)rates[i].time) + ",";

      result += "\"open\":" +
                DoubleToString(rates[i].open, digits) + ",";

      result += "\"high\":" +
                DoubleToString(rates[i].high, digits) + ",";

      result += "\"low\":" +
                DoubleToString(rates[i].low, digits) + ",";

      result += "\"close\":" +
                DoubleToString(rates[i].close, digits) + ",";

      result += "\"volume\":" +
                IntegerToString((long)rates[i].tick_volume);

      result += "}";
   }

   result += "]";

   return result;
}

//+------------------------------------------------------------------+
//| Build indicator JSON                                              |
//+------------------------------------------------------------------+
string BuildIndicatorsJSON()
{
   double rsi = GetRSI();
   double ema20 = GetEMA(20);
   double ema50 = GetEMA(50);
   double ema200 = GetEMA(200);
   double atr = GetATR();

   string json = "{";

   json += "\"rsi\":" + NumberOrNull(rsi) + ",";
   json += "\"ema20\":" + NumberOrNull(ema20) + ",";
   json += "\"ema50\":" + NumberOrNull(ema50) + ",";
   json += "\"ema200\":" + NumberOrNull(ema200) + ",";
   json += "\"atr\":" + NumberOrNull(atr);

   json += "}";

   return json;
}

//+------------------------------------------------------------------+
//| RSI                                                               |
//+------------------------------------------------------------------+
double GetRSI()
{
   int handle = iRSI(
      _Symbol,
      DataTimeframe,
      14,
      PRICE_CLOSE
   );

   if(handle == INVALID_HANDLE)
      return EMPTY_VALUE;

   double buffer[];

   ArraySetAsSeries(buffer, true);

   double value = EMPTY_VALUE;

   if(CopyBuffer(handle, 0, 0, 1, buffer) > 0)
      value = buffer[0];

   IndicatorRelease(handle);

   return value;
}

//+------------------------------------------------------------------+
//| EMA                                                               |
//+------------------------------------------------------------------+
double GetEMA(int period)
{
   int handle = iMA(
      _Symbol,
      DataTimeframe,
      period,
      0,
      MODE_EMA,
      PRICE_CLOSE
   );

   if(handle == INVALID_HANDLE)
      return EMPTY_VALUE;

   double buffer[];

   ArraySetAsSeries(buffer, true);

   double value = EMPTY_VALUE;

   if(CopyBuffer(handle, 0, 0, 1, buffer) > 0)
      value = buffer[0];

   IndicatorRelease(handle);

   return value;
}

//+------------------------------------------------------------------+
//| ATR                                                               |
//+------------------------------------------------------------------+
double GetATR()
{
   int handle = iATR(
      _Symbol,
      DataTimeframe,
      14
   );

   if(handle == INVALID_HANDLE)
      return EMPTY_VALUE;

   double buffer[];

   ArraySetAsSeries(buffer, true);

   double value = EMPTY_VALUE;

   if(CopyBuffer(handle, 0, 0, 1, buffer) > 0)
      value = buffer[0];

   IndicatorRelease(handle);

   return value;
}

//+------------------------------------------------------------------+
//| Number or null                                                    |
//+------------------------------------------------------------------+
string NumberOrNull(double value)
{
   if(value == EMPTY_VALUE ||
      !MathIsValidNumber(value))
   {
      return "null";
   }

   return DoubleToString(value, 8);
}

//+------------------------------------------------------------------+
//| JSON escape                                                       |
//+------------------------------------------------------------------+
string JsonEscape(string text)
{
   StringReplace(text, "\\", "\\\\");
   StringReplace(text, "\"", "\\\"");
   StringReplace(text, "\r", "");
   StringReplace(text, "\n", "");

   return text;
}

//+------------------------------------------------------------------+
//| HTTP POST                                                         |
//+------------------------------------------------------------------+
bool SendToServer(string json)
{
   string url = ServerURL + "/api/mt5/push";

   string headers =
      "Content-Type: application/json\r\n"
      "X-MT5-Secret: " + BridgeSecret + "\r\n";

   char post[];
   char result[];

   string resultHeaders;

   int size = StringToCharArray(
      json,
      post,
      0,
      WHOLE_ARRAY,
      CP_UTF8
   );

   if(size > 0)
      ArrayResize(post, size - 1);

   ResetLastError();

   int responseCode = WebRequest(
      "POST",
      url,
      headers,
      5000,
      post,
      result,
      resultHeaders
   );

   if(responseCode == -1)
   {
      int errorCode = GetLastError();

      Print(
         "WebRequest failed. Error: ",
         errorCode
      );

      return false;
   }

   if(responseCode < 200 ||
      responseCode >= 300)
   {
      Print(
         "Server rejected MT5 data. HTTP: ",
         responseCode
      );

      return false;
   }

   return true;
}
//+------------------------------------------------------------------+
