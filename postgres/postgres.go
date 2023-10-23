package postgres

import (
	"fmt"
	"wb-0-nats/domain"

	"github.com/jmoiron/sqlx" // Import sqlx package
	_ "github.com/lib/pq"     // PostgreSQL driver
)

// ConnectionString holds the connection details.
type ConnectionString struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// Connect establishes a connection to the PostgreSQL database.
func Connect(connStr ConnectionString) (*sqlx.DB, error) {
	// Construct the connection string
	connectionStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", connStr.Host, connStr.Port, connStr.User, connStr.Password, connStr.DBName)

	// Open a database connection
	db, err := sqlx.Open("postgres", connectionStr)
	if err != nil {
		return nil, err
	}

	// Check the connection
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
func GetOrders(db *sqlx.DB) ([]domain.Order, error) {
	var orders []domain.Order
	query := `
		SELECT
			o.order_uid,
			o.track_number,
			o.entry,
			o.locale,
			o.internal_signature,
			o.customer_id,
			o.delivery_service,
			o.shardkey,
			o.sm_id,
			o.date_created,
			o.oof_shard,
			d.name AS "delivery.name",
			d.phone AS "delivery.phone",
			d.zip AS "delivery.zip",
			d.city AS "delivery.city",
			d.address AS "delivery.address",
			d.region AS "delivery.region",
			d.email AS "delivery.email",
			p.transaction AS "payment.transaction",
			p.request_id AS "payment.request_id",
			p.currency AS "payment.currency",
			p.provider AS "payment.provider",
			p.amount AS "payment.amount",
			p.payment_dt AS "payment.payment_dt",
			p.bank AS "payment.bank",
			p.delivery_cost AS "payment.delivery_cost",
			p.goods_total AS "payment.goods_total",
			p.custom_fee AS "payment.custom_fee"
			-- Include other fields you need
		FROM
			orders o
		LEFT JOIN
			wb.delivery d ON o.order_uid = d.order_uid
		LEFT JOIN
			wb.payment p ON o.order_uid = p.transaction
	`
	err := db.Select(&orders, query)
	if err != nil {
		return nil, err
	}
	return orders, nil

}
func GetOrder(db *sqlx.DB, orderUID string) (domain.Order, error) {
	var order domain.Order

	query := `
		SELECT
			o.order_uid,
			o.track_number,
			o.entry,
			o.locale,
			o.internal_signature,
			o.customer_id,
			o.delivery_service,
			o.shardkey,
			o.sm_id,
			o.date_created,
			o.oof_shard,
			d.name AS "delivery.name",
			d.phone AS "delivery.phone",
			d.zip AS "delivery.zip",
			d.city AS "delivery.city",
			d.address AS "delivery.address",
			d.region AS "delivery.region",
			d.email AS "delivery.email",
			p.transaction AS "payment.transaction",
			p.request_id AS "payment.request_id",
			p.currency AS "payment.currency",
			p.provider AS "payment.provider",
			p.amount AS "payment.amount",
			p.payment_dt AS "payment.payment_dt",
			p.bank AS "payment.bank",
			p.delivery_cost AS "payment.delivery_cost",
			p.goods_total AS "payment.goods_total",
			p.custom_fee AS "payment.custom_fee"
			-- Include other fields you need
		FROM
			orders o
		LEFT JOIN
			wb.delivery d ON o.order_uid = d.order_uid
		LEFT JOIN
			wb.payment p ON o.order_uid = p.transaction
		WHERE
			o.order_uid = $1
	`

	err := db.Get(&order, query, orderUID)
	if err != nil {
		return domain.Order{}, err
	}

	// You may need to populate the Items field using another query or method

	return order, nil
}

func InsertOrder(db *sqlx.DB, order domain.Order) error {
	// Start a new database transaction.
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
		}
	}()
	order.Payment.OrderUID = order.OrderUID
	order.Delivery.OrderUID = order.OrderUID

	// Insert order information into the 'orders' table.
	orderQuery := `
		INSERT INTO wb.orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey,
			sm_id, date_created, oof_shard)
		VALUES (:order_uid, :track_number, :entry, :locale, :internal_signature, :customer_id, :delivery_service, :shardkey,
			:sm_id, :date_created, :oof_shard)`

	// Insert delivery information into the 'delivery' table.
	deliveryQuery := `
		INSERT INTO wb.delivery (order_uid, name, phone, zip, city, address, region, email)
		VALUES (:order_uid, :name, :phone, :zip, :city, :address, :region, :email)`

	// Insert payment information into the 'payment' table.
	paymentQuery := `
		INSERT INTO wb.payment (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES (:transaction, :request_id, :currency, :provider, :amount, :payment_dt, :bank, :delivery_cost, :goods_total, :custom_fee)`

	// Insert items information into the 'items' table.
	itemsQuery := `
		INSERT INTO wb.items (chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
		VALUES (:chrt_id, :track_number, :price, :rid, :name, :sale, :size, :total_price, :nm_id, :brand, :status)`

	// Execute the SQL queries with data binding.
	_, err = tx.NamedExec(orderQuery, order)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.NamedExec(deliveryQuery, order.Delivery)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.NamedExec(paymentQuery, order.Payment)
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, item := range order.Items {
		_, err = tx.NamedExec(itemsQuery, item)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	// Commit the transaction if all queries succeed.
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
