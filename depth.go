package gateapi

import (
	"net/http"
)

type SpotDepthOpts struct {
	Symbol string `url:"currency_pair"`
	Limit  int    `url:"limit"`
	WithID bool   `url:"with_id"`
}

// symbol ex: BTC_USDT
func (b *Client) SpotDepth(symbol string) (*SpotDepth, error) {
	opts := SpotDepthOpts{
		Symbol: symbol,
		Limit:  100,
		WithID: true,
	}
	res, err := b.do("spot", http.MethodGet, "spot/order_book", opts, false, false)
	if err != nil {
		return nil, err
	}
	depth := &SpotDepth{}
	err = json.Unmarshal(res, &depth)
	if err != nil {
		return nil, err
	}
	return depth, nil
}

type SpotDepth struct {
	ID      int64      `json:"id"`
	Current int64      `json:"current"`
	Update  int64      `json:"update"`
	Asks    [][]string `json:"asks"`
	Bids    [][]string `json:"bids"`
}
