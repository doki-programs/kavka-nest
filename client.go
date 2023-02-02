package kavkanest

import (
	"errors"
	"log"
	"os"
)

var (
	ErrNilKafkaClient    = errors.New("nil client")
	ErrEmptyBrokersUrl   = errors.New("at least one broker is required")
	ErrEmptyUsername     = errors.New("SASL username is required")
	ErrEmptyPassword     = errors.New("SASL password is required")
	ErrEmptyCertLocation = errors.New("SSL certificate location is required")
	ErrCertFileNotExist  = errors.New("certificate file not exists")
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
	DEBUG_LEVEL_CONF     DebugLevel = "conf"
	DEBUG_LEVEL_ALL      DebugLevel = "generic,broker,security,conf"
)

func (s DebugLevel) String() string {
	return string(s)
}

type KafkaClient struct {
	Id             string
	Username       string     // The SASL username
	Password       string     // The SASL password
	ScramAlgorithm ScramAlg   // The SASL SCRAM SHA algorithm SCRAM-SHA-256 or SCRAM-SHA-512 as mechanism
	CertLocation   string     // Your certificate relative path (e.g., "./examples/ca.crt")
	BrokersUrl     string     // Comma separated brokers url
	DebugLevel     DebugLevel // Debug level
}

func (client *KafkaClient) Validate() error {
	if client == nil {
		return ErrNilKafkaClient
	}
	if len(client.BrokersUrl) == 0 {
		return ErrEmptyBrokersUrl
	}
	if client.Username == "" {
		return ErrEmptyUsername
	}
	if client.Password == "" {
		return ErrEmptyPassword
	}
	if client.CertLocation == "" {
		return ErrEmptyCertLocation
	} else {
		info, err := os.Stat(client.CertLocation)
		if err != nil {
			if os.IsNotExist(err) {
				return ErrCertFileNotExist
			}
			return err
		}
		log.Printf("found certificate file: %s", info.Name())
	}

	return nil

}
