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
}

// func SwapStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
// 	return localStreamTicker("swap", symbol, logger)
// }

// ex: symbol = btcusdt
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
	s.initialWithSpotDetail(product, symbol)
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
				} else {
					logger.Warningf("Refreshing %s %s ticker stream with err: %s\n", symbol, product, err.Error())
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

func (s *StreamTickerBranch) GetBid() (price, qty string, ok bool) {
	s.bid.mux.RLock()
	defer s.bid.mux.RUnlock()
	price = s.bid.price
	qty = s.bid.qty
	if price == NullPrice || price == "" {
		return price, qty, false
	}
	return price, qty, true
}

func (s *StreamTickerBranch) GetAsk() (price, qty string, ok bool) {
	s.ask.mux.RLock()
	defer s.ask.mux.RUnlock()
	price = s.ask.price
	qty = s.ask.qty
	if price == NullPrice || price == "" {
		return price, qty, false
	}
	return price, qty, true
}

func (s *StreamTickerBranch) updateBidData(price, qty string) {
	s.bid.mux.Lock()
	defer s.bid.mux.Unlock()
	s.bid.price = price
	s.bid.qty = qty
}

func (s *StreamTickerBranch) updateAskData(price, qty string) {
	s.ask.mux.Lock()
	defer s.ask.mux.Unlock()
	s.ask.price = price
	s.ask.qty = qty
}

func (s *StreamTickerBranch) maintainStreamTicker(
	ctx context.Context,
	product, symbol string,
	ticker *chan map[string]interface{},
	errCh *chan error,
) error {
	lastUpdate := time.Now()
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
			s.updateBidData(bidPrice, bidQty)
			s.updateAskData(askPrice, askQty)
			lastUpdate = time.Now()
		default:
			if time.Now().After(lastUpdate.Add(time.Second * 60)) {
				// 60 sec without updating
				err := errors.New("reconnect because of time out")
				*errCh <- err
				return err
			}
			time.Sleep(time.Millisecond * 100)
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
	var duration time.Duration = 300
	w.Logger = logger
	w.OnErr = false
	var url string
	switch product {
	case "spot":
		url = "wss://api.gateio.ws/ws/v4/"
	case "swap":
		// pass
	default:
		return errors.New("not supported product, cancel socket connection")
	}
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	logger.Infof("Gate.io %s %s %s socket connected.\n", symbol, product, channel)
	w.Conn = conn
	defer conn.Close()
	err = w.subscribeTo(channel, symbol)
	if err != nil {
		return err
	}
	if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
		return err
	}
	w.Conn.SetPingHandler(nil)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-*errCh:
			return err
		default:
			_, buf, err := conn.ReadMessage()
			if err != nil {
				d := w.outGateIoErr()
				*mainCh <- d
				return err
			}
			res, err1 := decodingMap(buf, logger)
			if err1 != nil {
				d := w.outGateIoErr()
				*mainCh <- d
				return err1
			}
			err2 := w.handleGateIOSocketData(res, mainCh)
			if err2 != nil {
				d := w.outGateIoErr()
				*mainCh <- d
				return err2
			}
			if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
				return err
			}
		}
	}
}

func (s *StreamTickerBranch) initialWithSpotDetail(product, symbol string) error {
	switch product {
	case "spot":
		client := New("", "", "")
		res, err := client.SpotDepth(symbol, 1)
		if err != nil {
			return err
		}
		// 0 => price, 1 => qty
		s.updateBidData(res.Bids[0][0], res.Bids[0][1])
		s.updateAskData(res.Asks[0][0], res.Asks[0][1])
	case "swap":
		//
	default:
		return errors.New("not supported product to initial spot detail")
	}

	return nil
}
