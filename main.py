import websocket, json
import pandas as pd
import numpy as np

#wss://stream.binance.com:9443/ws/!miniTicker@arr

def PullMarketData(streamname):
    endpoint = "wss://stream.binance.com:9443"
    stream = f"{endpoint}/ws/{streamname}"
    socket = websocket.WebSocketApp(stream, on_message=Callback)
    socket.run_forever()

def Callback(socket, message):
    path = "./Data/"
    socketdata = json.loads(message)
    #print(socketdata)
    symbol = [x for x in socketdata if x['s'].endswith('USDT')]
    frame = pd.DataFrame(symbol)[['E','s','c']]
    frame.E = pd.to_datetime(frame.E, unit='ms')
    frame.c = frame.c.astype(float)
    for row in range(len(frame)):
        data = frame[row:row+1]
        data[['E', 'c']].to_csv(path+data['s'].values[0], mode='a', header=False)



def main():
    try:
        PullMarketData("!miniTicker@arr")
    except:
        print("Error")


if __name__ == "__main__":
    main()

"""
Payload
{
  "e": "1hTicker",    // Event type
  "E": 123456789,     // Event time
  "s": "BNBBTC",      // Symbol
  "p": "0.0015",      // Price change
  "P": "250.00",      // Price change percent
  "o": "0.0010",      // Open price
  "h": "0.0025",      // High price
  "l": "0.0010",      // Low price
  "c": "0.0025",      // Last price
  "w": "0.0018",      // Weighted average price
  "v": "10000",       // Total traded base asset volume
  "q": "18",          // Total traded quote asset volume
  "O": 0,             // Statistics open time
  "C": 86400000,      // Statistics close time
  "F": 0,             // First trade ID
  "L": 18150,         // Last trade Id
  "n": 18151          // Total number of trades
}

"""