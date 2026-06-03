// Package config defines, loads, and validates StageHand's declarative
// configuration (config.yaml): the server settings, VRAM pools, services,
// and routing rules described in PRD.md §2.
package config

import (
	"slices"
	"strings"
	"time"
)

// Defaults applied during Load when a field is omitted.
const (
	DefaultHost                  = "0.0.0.0"
	DefaultPort                  = 8080
	DefaultDockerSocketPath      = "/var/run/docker.sock"
	DefaultMaxQueueSize          = 100
	DefaultHealthPath            = "/"
	DefaultStartupTimeoutSeconds = 180
)

// Config is the root of config.yaml.
type Config struct {
	Server    Server              `yaml:"server"`
	VRAMPools map[string]VRAMPool `yaml:"vram_pools"`
	Services  map[string]Service  `yaml:"services"`
	Routes    []Route             `yaml:"routes"`
}

// Server holds global listener and proxy settings.
type Server struct {
	Host               string   `yaml:"host"`
	Port               int      `yaml:"port"`
	DockerSocketPath   string   `yaml:"docker_socket_path"`
	CORSAllowedOrigins []string `yaml:"cors_allowed_origins"`
	MaxQueueSize       int      `yaml:"max_queue_size"`
	Auth               Auth     `yaml:"auth"`
}

// Auth holds the optional API tokens that gate StageHand's HTTP surface
// (PRD §5). An empty AdminToken means StageHand generates a random one at
// boot and prints it once (admin auth is on by default); an empty ProxyToken
// leaves regular proxied traffic unauthenticated. Tokens are presented in
// dedicated headers and compared in constant time, never reflected to
// backends.
type Auth struct {
	AdminToken string `yaml:"admin_token"`
	ProxyToken string `yaml:"proxy_token"`
}

// VRAMPool is a mutual-exclusion group: at most one member service's
// container runs at any moment.
type VRAMPool struct {
	GracePeriodSeconds int `yaml:"grace_period_seconds"`
	CooldownSeconds    int `yaml:"cooldown_seconds"`
	// DefaultService, when set, is swapped in on cooldown expiry.
	// nil means the pool spins fully down ("cold pool").
	DefaultService *string `yaml:"default_service"`
}

// GracePeriod returns the anti-thrashing minimum runtime as a Duration.
func (p VRAMPool) GracePeriod() time.Duration {
	return time.Duration(p.GracePeriodSeconds) * time.Second
}

// Cooldown returns the idle-shutdown delay as a Duration (0 = disabled).
func (p VRAMPool) Cooldown() time.Duration {
	return time.Duration(p.CooldownSeconds) * time.Second
}

// Service is one proxied AI backend container.
type Service struct {
	ContainerName         string `yaml:"container_name"`
	TargetURL             string `yaml:"target_url"`
	HealthPath            string `yaml:"health_path"`
	StartupTimeoutSeconds int    `yaml:"startup_timeout_seconds"`
	// MaxQueueSize overrides Server.MaxQueueSize for this service when > 0.
	MaxQueueSize int `yaml:"max_queue_size"`
	// VRAMPool names the pool this service belongs to.
	// nil means the service is always-on and bypasses pool orchestration.
	VRAMPool *string `yaml:"vram_pool"`
}

// AlwaysOn reports whether the service bypasses VRAM pool orchestration.
func (s Service) AlwaysOn() bool { return s.VRAMPool == nil }

// StartupTimeout returns the cold-start budget as a Duration.
func (s Service) StartupTimeout() time.Duration {
	return time.Duration(s.StartupTimeoutSeconds) * time.Second
}

// HealthURL returns the absolute URL polled for readiness.
func (s Service) HealthURL() string {
	return strings.TrimSuffix(s.TargetURL, "/") + s.HealthPath
}

// Route maps incoming requests to a service. Routes are evaluated in
// declared order; first match wins (PRD §2.1).
type Route struct {
	PathPrefix string `yaml:"path_prefix"`
	// Service is the target, or the fallback when Models is set but the
	// request's model name has no entry.
	Service string `yaml:"service"`
	// Headers, when set, must all match for this route to apply;
	// otherwise the request falls through to later routes.
	Headers map[string]string `yaml:"headers"`
	// Models maps a JSON body "model" field value to a service name.
	Models map[string]string `yaml:"models"`
}

// QueueSize returns the effective queue bound for a service.
func (c *Config) QueueSize(svc Service) int {
	if svc.MaxQueueSize > 0 {
		return svc.MaxQueueSize
	}
	return c.Server.MaxQueueSize
}

// ContainerNames returns every configured container name, sorted —
// the list boot and reload validation hand to Docker.
func (c *Config) ContainerNames() []string {
	names := make([]string, 0, len(c.Services))
	for _, svc := range c.Services {
		names = append(names, svc.ContainerName)
	}
	slices.Sort(names)
	return names
}
