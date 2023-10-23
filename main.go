package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"wb-0-nats/cache"
	"wb-0-nats/domain"
	"wb-0-nats/nats"
	"wb-0-nats/postgres"
	"wb-0-nats/recovery"
)

func main() {
	cache := cache.NewCache()
	clusterID := "test-cluster"
	clientID := "wb"
	connNats, err := nats.Connect("nats://0.0.0.0:4223", clusterID, clientID)
	if err != nil {
		panic(err)
	}
	connPsql, err := postgres.Connect(postgres.ConnectionString{
		Host:     "0.0.0.0",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		DBName:   "postgres",
	})
	if err != nil {
		fmt.Println(err)
	}
	err = recovery.RecoveryCache(cache, connPsql)
	if err != nil {
		return
	}
	orderCh := make(chan domain.Order)
	err = nats.SubscribeOrder(connNats, "nats-streaming", orderCh)
	if err != nil {
		fmt.Println(err)
	}
	if err != nil {
		fmt.Println(err)
	}

	go func() {
		for {
			order := <-orderCh
			err := postgres.InsertOrder(connPsql, order)
			if err != nil {
				fmt.Println(err)
			}
			cache.Set(order.CustomerID, order)
		}
	}()
	r := gin.Default()
	r.GET("/order/:id", func(c *gin.Context) {
		id := c.Param("id")
		order := cache.Get(id)
		c.JSON(http.StatusOK, gin.H{
			"message": order,
		})
	})

	// Start the Gin server on port 8080
	r.Run(":8080")
}
