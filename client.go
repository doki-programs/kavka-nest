package kavkanest

import (
	"errors"
)

var (
	ErrInvalidBrokersUrl = errors.New("at least one broker is required")
	ErrInvalidUsername   = errors.New("SASL username is required")
	ErrInvalidPassword   = errors.New("SASL password is required")
)

type Client struct {
	Id             string
	Username       string // The SASL username
	Password       string // The SASL password
	ScramAlgorithm string // The SASL SCRAM SHA algorithm SCRAM-SHA-256 or SCRAM-SHA-512 as mechanism
	BrokersUrl     string // comma separated brokers url
}
