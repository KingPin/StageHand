package dockerctl

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ValidateContainers pings the daemon and verifies every named container
// exists on the host (PRD §2.2). Used at boot (fail loudly) and during
// hot reload (reject the new config, keep the old).
func ValidateContainers(ctx context.Context, c Client, names []string) error {
	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("docker daemon unreachable: %w", err)
	}
	var missing []string
	for _, n := range names {
		_, err := c.InspectByName(ctx, n)
		switch {
		case errors.Is(err, ErrNotFound):
			missing = append(missing, n)
		case err != nil:
			return fmt.Errorf("inspecting %q: %w", n, err)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("containers not found on host: %s", strings.Join(missing, ", "))
	}
	return nil
}
