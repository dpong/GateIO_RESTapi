package gateapi

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

const NullPrice = "null"

type StreamTickerBranch struct {
	bid    tobBranch
	ask    tobBranch
	cancel *context.CancelFunc
	reCh   chan error
}

type tobBranch struct {
	mux   sync.RWMutex
	price string
	qty   string
	ts    time.Time
}

// func SwapStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
// 	return localStreamTicker("swap", symbol, logger)
// }

// ex: symbol = BTC_USDT
func SpotStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
	return localStreamTicker("spot", symbol, logger)
}

func localStreamTicker(product, symbol string, logger *log.Logger) *StreamTickerBranch {
	var s StreamTickerBranch
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = &cancel
	ticker := make(chan map[string]interface{}, 50)
	errCh := make(chan error, 5)
	// initial data with rest api first
	//s.initialWithSpotDetail(product, symbol)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := gateTickerSocket(ctx, product, symbol, "spot.book_ticker", logger, &ticker, &errCh); err == nil {
					return
				} else {
					logger.Warningf("Reconnect %s %s ticker stream with err: %s\n", symbol, product, err.Error())
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.maintainStreamTicker(ctx, product, symbol, &ticker, &errCh); err == nil {
					return
				}
			}
		}
	}()
	return &s
}

func (s *StreamTickerBranch) Close() {
	(*s.cancel)()
	s.bid.mux.Lock()
	s.bid.price = NullPrice
	s.bid.mux.Unlock()
	s.ask.mux.Lock()
	s.ask.price = NullPrice
	s.ask.mux.Unlock()
}

func (s *StreamTickerBranch) GetBid() (price, qty string, timeStamp time.Time, ok bool) {
	s.bid.mux.RLock()
	defer s.bid.mux.RUnlock()
	price = s.bid.price
	qty = s.bid.qty
	if price == NullPrice || price == "" {
		return price, qty, timeStamp, false
	}
	timeStamp = s.bid.ts
	return price, qty, timeStamp, true
}

func (s *StreamTickerBranch) GetAsk() (price, qty string, timeStamp time.Time, ok bool) {
	s.ask.mux.RLock()
	defer s.ask.mux.RUnlock()
	price = s.ask.price
	qty = s.ask.qty
	if price == NullPrice || price == "" {
		return price, qty, timeStamp, false
	}
	timeStamp = s.ask.ts
	return price, qty, timeStamp, true
}

func (s *StreamTickerBranch) updateBidData(price, qty string, timeStamp time.Time) {
	s.bid.mux.Lock()
	defer s.bid.mux.Unlock()
	s.bid.price = price
	s.bid.qty = qty
	s.bid.ts = timeStamp
}

func (s *StreamTickerBranch) updateAskData(price, qty string, timeStamp time.Time) {
	s.ask.mux.Lock()
	defer s.ask.mux.Unlock()
	s.ask.price = price
	s.ask.qty = qty
	s.ask.ts = timeStamp
}

func (s *StreamTickerBranch) maintainStreamTicker(
	ctx context.Context,
	product, symbol string,
	ticker *chan map[string]interface{},
	errCh *chan error,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-(*ticker):
			var bidPrice, askPrice, bidQty, askQty string
			if bid, ok := message["b"].(string); ok {
				bidPrice = bid
			} else {
				bidPrice = NullPrice
			}
			if ask, ok := message["a"].(string); ok {
				askPrice = ask
			} else {
				askPrice = NullPrice
			}
			if bidqty, ok := message["B"].(string); ok {
				bidQty = bidqty
			}
			if askqty, ok := message["A"].(string); ok {
				askQty = askqty
			}

			var timeStamp time.Time
			if ts, ok := message["t"].(float64); ok {
				timeStamp = time.UnixMilli(int64(ts))
			}
			s.updateBidData(bidPrice, bidQty, timeStamp)
			s.updateAskData(askPrice, askQty, timeStamp)
		}
	}
}

func gateTickerSocket(
	ctx context.Context,
	product, symbol, channel string,
	logger *log.Logger,
	mainCh *chan map[string]interface{},
	errCh *chan error,
) error {
	var w wS
	var duration time.Duration = 30
	innerErr := make(chan string, 1)
	w.logger = logger
	var url string
	switch product {
	case "spot":
		url = "wss://api.gateio.ws/ws/v4/"
	case "swap":
		// pass
	default:
		return errors.New("not supported product, cancel socket connection")
	}
	dailCtx, _ := context.WithDeadline(ctx, time.Now().Add(time.Second*5))
	conn, _, err := websocket.DefaultDialer.DialContext(dailCtx, url, nil)
	if err != nil {
		return err
	}
	logger.Infof("Gate.io %s %s %s socket connected.\n", symbol, product, channel)
	w.conn = conn
	defer conn.Close()
	err = w.subscribeTo(channel, symbol)
	if err != nil {
		return err
	}
	if err := w.conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
		return err
	}
	// ping pong part
	go func() {
		ping := time.NewTicker(time.Second * 20)
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-innerErr:
				return
			case <-ping.C:
				if err := w.getPingPong(); err != nil {
					w.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 5))
				}
			}
		}
	}()
	w.conn.SetPingHandler(nil)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-*errCh:
			innerErr <- "restart"
			return err
		default:
			_, buf, err := w.conn.ReadMessage()
			if err != nil {
				innerErr <- "restart"
				return err
			}
			res, err1 := decodingMap(buf, logger)
			if err1 != nil {
				innerErr <- "restart"
				return err1
			}
			err2 := w.handleGateIOSocketData(res, mainCh)
			if err2 != nil {
				innerErr <- "restart"
				return err2
			}
			if err := w.conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
				innerErr <- "restart"
				return err
			}
		}
	}
}

// func (s *StreamTickerBranch) initialWithSpotDetail(product, symbol string) error {
// 	switch product {
// 	case "spot":
// 		client := New("", "", "")
// 		res, err := client.SpotDepth(symbol, 1)
// 		if err != nil {
// 			return err
// 		}
// 		// 0 => price, 1 => qty
// 		s.updateBidData(res.Bids[0][0], res.Bids[0][1])
// 		s.updateAskData(res.Asks[0][0], res.Asks[0][1])
// 	case "swap":
// 		//
// 	default:
// 		return errors.New("not supported product to initial spot detail")
// 	}

// 	return nil
// }

func (w *wS) getPingPong() error {
	t := time.Now().Unix()
	pingMsg := NewMsg("spot.ping", "", t, []string{})
	err := pingMsg.send(w.conn)
	if err != nil {
		return err
	}
	return nil
}
