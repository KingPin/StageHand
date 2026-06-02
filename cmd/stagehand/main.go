// Command stagehand is the StageHand VRAM multiplexer & reverse proxy.
//
// This entrypoint is a placeholder; full wiring (config load, Docker boot
// validation, orchestrator, HTTP server) lands with the cmd wiring commit.
package main

import (
	"flag"
	"fmt"

	"github.com/KingPin/StageHand/internal/version"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("stagehand", version.Version)
		return
	}
	fmt.Println("stagehand", version.Version, "— not yet wired; see PRD.md")
}
