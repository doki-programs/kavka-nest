package kavkanest

import (
	"errors"
)

var (
	ErrInvalidBrokersUrl = errors.New("at least one broker is required")
	ErrInvalidUsername   = errors.New("SASL username is required")
	ErrInvalidPassword   = errors.New("SASL password is required")
)

type ScramAlg string

var (
	SCRAM_SHA_256 ScramAlg = "SCRAM-SHA-256"
	SCRAM_SHA_512 ScramAlg = "SCRAM-SHA-512"
)

func (s ScramAlg) String() string {
	return string(s)
}

type Client struct {
	Id             string
	Username       string   // The SASL username
	Password       string   // The SASL password
	ScramAlgorithm ScramAlg // The SASL SCRAM SHA algorithm SCRAM-SHA-256 or SCRAM-SHA-512 as mechanism
	BrokersUrl     string   // comma separated brokers url
}
