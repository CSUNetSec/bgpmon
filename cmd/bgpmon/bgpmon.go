// The bgpmon command is the RPC client for the bgpmon server. All subcommands
// and their documentation can be found in the cmd/ directory.
package main

import "github.com/CSUNetSec/bgpmon/cmd/bgpmon/cmd"

func main() {
	cmd.Execute()
}
