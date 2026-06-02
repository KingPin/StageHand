package config

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Load reads, parses, defaults, and validates the config file at path.
// It returns the config, a list of non-fatal warnings, and an error that
// aggregates every validation failure found.
func Load(path string) (*Config, []string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("reading config: %w", err)
	}
	return Parse(raw)
}

// Parse parses, defaults, and validates raw YAML config bytes.
func Parse(raw []byte) (*Config, []string, error) {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true) // reject unknown/typo'd keys loudly

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.applyDefaults()
	warnings, err := cfg.validate()
	if err != nil {
		return nil, warnings, err
	}
	return &cfg, warnings, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Host == "" {
		c.Server.Host = DefaultHost
	}
	if c.Server.Port == 0 {
		c.Server.Port = DefaultPort
	}
	if c.Server.DockerSocketPath == "" {
		c.Server.DockerSocketPath = DefaultDockerSocketPath
	}
	if c.Server.MaxQueueSize == 0 {
		c.Server.MaxQueueSize = DefaultMaxQueueSize
	}
	for name, svc := range c.Services {
		if svc.HealthPath == "" {
			svc.HealthPath = DefaultHealthPath
		}
		if svc.StartupTimeoutSeconds == 0 {
			svc.StartupTimeoutSeconds = DefaultStartupTimeoutSeconds
		}
		c.Services[name] = svc
	}
}
