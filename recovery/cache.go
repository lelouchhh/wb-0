package recovery

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"wb-0-nats/cache"
	"wb-0-nats/postgres"
)

func RecoveryCache(c *cache.Cache, db *sqlx.DB) error {
	orders, err := postgres.GetOrders(db)
	if err != nil {
		return err
	}
	fmt.Println(orders)
	for _, order := range orders {
		c.Set(order.OrderUID, order)
	}
	return nil
}
