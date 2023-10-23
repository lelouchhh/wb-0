package nats

import (
	"encoding/json"
	"github.com/nats-io/stan.go"
	"wb-0-nats/domain"
)

func Connect(url, clusterID, clientID string) (stan.Conn, error) {

	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(url))
	if err != nil {
		return nil, err
	}
	return sc, nil
}

func SubscribeOrder(sc stan.Conn, subject string, orderCh chan domain.Order) error {
	var orderData domain.Order

	_, err := sc.Subscribe(subject, func(m *stan.Msg) {
		if err := json.Unmarshal(m.Data, &orderData); err != nil {
			return
		}

		if err := validateOrder(orderData); err != nil {
			return
		}

		orderCh <- orderData
	})
	if err != nil {
		return err
	}

	return nil
}

func validateOrder(order domain.Order) error {
	// Add your validation logic here
	return nil
}
