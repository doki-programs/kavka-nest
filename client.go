package kavkanest

import (
	"errors"
)

var (
	ErrNilKafkaClient  = errors.New("nil client")
	ErrEmptyBrokersUrl = errors.New("at least one broker is required")
	ErrEmptyUsername   = errors.New("SASL username is required")
	ErrEmptyPassword   = errors.New("SASL password is required")
)

type ScramAlg string

var (
	SCRAM_SHA_256 ScramAlg = "SCRAM-SHA-256"
	SCRAM_SHA_512 ScramAlg = "SCRAM-SHA-512"
)

func (s ScramAlg) String() string {
	return string(s)
}

type DebugLevel string

var (
	DEBUG_LEVEL_GENERIC  DebugLevel = "generic"
	DEBUG_LEVEL_BROKER   DebugLevel = "broker"
	DEBUG_LEVEL_SECURITY DebugLevel = "security"
	DEBUG_LEVEL_ALL      DebugLevel = "generic,broker,security"
)

func (s DebugLevel) String() string {
	return string(s)
}

type KafkaClient struct {
	Id             string
	Username       string     // The SASL username
	Password       string     // The SASL password
	ScramAlgorithm ScramAlg   // The SASL SCRAM SHA algorithm SCRAM-SHA-256 or SCRAM-SHA-512 as mechanism
	BrokersUrl     string     // Comma separated brokers url
	DebugLevel     DebugLevel // Debug level
}
