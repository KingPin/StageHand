package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// validate checks every rule from PRD §2.2. It returns non-fatal warnings
// and an error aggregating ALL violations (not just the first), so users
// can fix a broken config in one pass.
func (c *Config) validate() ([]string, error) {
	var errs []error
	var warnings []string

	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}
	warn := func(format string, args ...any) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}

	// --- server ---
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		fail("server.port %d out of range 1-65535", c.Server.Port)
	}
	if c.Server.MaxQueueSize < 1 {
		fail("server.max_queue_size must be >= 1, got %d", c.Server.MaxQueueSize)
	}

	// --- services ---
	if len(c.Services) == 0 {
		fail("at least one service must be declared")
	}
	containerOwners := map[string]string{} // container_name -> service name
	for name, svc := range c.Services {
		if svc.ContainerName == "" {
			fail("service %q: container_name is required", name)
		} else if prev, dup := containerOwners[svc.ContainerName]; dup {
			fail("services %q and %q share container_name %q — two services cannot manage one container",
				prev, name, svc.ContainerName)
		} else {
			containerOwners[svc.ContainerName] = name
		}

		u, err := url.Parse(svc.TargetURL)
		switch {
		case svc.TargetURL == "":
			fail("service %q: target_url is required", name)
		case err != nil:
			fail("service %q: target_url %q: %v", name, svc.TargetURL, err)
		case u.Scheme != "http" && u.Scheme != "https":
			fail("service %q: target_url %q must be absolute http(s)", name, svc.TargetURL)
		case u.Host == "":
			fail("service %q: target_url %q has no host", name, svc.TargetURL)
		case u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1":
			warn("service %q: target_url host %q — StageHand expects container names on a shared Docker network; localhost only works with host networking",
				name, u.Hostname())
		}

		if !strings.HasPrefix(svc.HealthPath, "/") {
			fail("service %q: health_path %q must start with /", name, svc.HealthPath)
		}
		if svc.StartupTimeoutSeconds < 1 {
			fail("service %q: startup_timeout_seconds must be >= 1, got %d", name, svc.StartupTimeoutSeconds)
		}
		if svc.MaxQueueSize < 0 {
			fail("service %q: max_queue_size must be >= 1 (or omitted), got %d", name, svc.MaxQueueSize)
		}
		if svc.VRAMPool != nil {
			if _, ok := c.VRAMPools[*svc.VRAMPool]; !ok {
				fail("service %q: vram_pool %q is not defined", name, *svc.VRAMPool)
			}
		}
	}

	// --- pools ---
	for name, pool := range c.VRAMPools {
		if pool.GracePeriodSeconds < 0 {
			fail("vram_pool %q: grace_period_seconds must be >= 0", name)
		}
		if pool.CooldownSeconds < 0 {
			fail("vram_pool %q: cooldown_seconds must be >= 0", name)
		}
		if pool.DefaultService != nil {
			def := *pool.DefaultService
			svc, ok := c.Services[def]
			if !ok {
				fail("vram_pool %q: default_service %q is not a declared service", name, def)
			} else if svc.VRAMPool == nil || *svc.VRAMPool != name {
				fail("vram_pool %q: default_service %q does not belong to this pool", name, def)
			}
		}
	}

	// --- routes ---
	if len(c.Routes) == 0 {
		fail("at least one route must be declared")
	}
	for i, r := range c.Routes {
		if !strings.HasPrefix(r.PathPrefix, "/") {
			fail("route %d: path_prefix %q must start with /", i, r.PathPrefix)
		}
		if r.Service == "" {
			fail("route %d (%s): service is required (it is the fallback when models is set)", i, r.PathPrefix)
		} else if _, ok := c.Services[r.Service]; !ok {
			fail("route %d (%s): service %q is not declared", i, r.PathPrefix, r.Service)
		}
		for model, svc := range r.Models {
			if model == "" {
				fail("route %d (%s): empty model name in models map", i, r.PathPrefix)
			}
			if _, ok := c.Services[svc]; !ok {
				fail("route %d (%s): models[%q] targets undeclared service %q", i, r.PathPrefix, model, svc)
			}
		}
	}

	return warnings, errors.Join(errs...)
}
