package gateapi

import (
	"github.com/gorilla/websocket"
)

type Msg struct {
	Time    int64    `json:"time"`
	Channel string   `json:"channel"`
	Event   string   `json:"event"`
	Payload []string `json:"payload"`
	Auth    *Auth    `json:"auth"`
}

type Auth struct {
	Method string `json:"method"`
	KEY    string `json:"KEY"`
	SIGN   string `json:"SIGN"`
}

// func sign(channel, event string, t int64) string {
// 	message := fmt.Sprintf("channel=%s&event=%s&time=%d", channel, event, t)
// 	h2 := hmac.New(sha512.New, []byte(Secret))
// 	io.WriteString(h2, message)
// 	return hex.EncodeToString(h2.Sum(nil))
// }

// func (msg *Msg) sign() {
// 	signStr := sign(msg.Channel, msg.Event, msg.Time)
// 	msg.Auth = &Auth{
// 		Method: "api_key",
// 		KEY:    Key,
// 		SIGN:   signStr,
// 	}
// }

func (msg *Msg) send(c *websocket.Conn) error {
	msgByte, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.WriteMessage(websocket.TextMessage, msgByte)
}

func NewMsg(channel, event string, t int64, payload []string) *Msg {
	return &Msg{
		Time:    t,
		Channel: channel,
		Event:   event,
		Payload: payload,
	}
}
