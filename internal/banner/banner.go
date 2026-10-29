package banner

import (
	"fmt"
	"runtime"
)

// ANSI color codes
const (
	colorCyan   = "\033[36m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

const rawBanner = `
  _   _ _   ___  ____  ______   _______     __
 | | | | \ | \ \/ /\ \/ /  _ \ | ____\ \   / /
 | | | |  \| |\  /  \  /| | | ||  _|  \ \ / / 
 | |_| | |\  | | |  /  \| |_| || |___  \ V /  
  \___/|_| \_| |_| /_/\_\____/ |_____|  \_/   
`

// Print prints a Spring Boot-style ASCII banner with service name, Go runtime, and version.
func Print(serviceName string, version string) {
	fmt.Print(colorCyan + rawBanner + colorReset)
	fmt.Printf("%s :: UNYXDEV ::  CONVERSATION ENGINE  %s(v%s)%s\n", colorBold+colorGreen, colorDim, version, colorReset)
	fmt.Printf("%s :: Service : %s%-20s%s :: Go Runtime: %s%s%s\n\n",
		colorYellow, colorReset, serviceName, colorYellow, colorReset, runtime.Version(), colorReset)
}
