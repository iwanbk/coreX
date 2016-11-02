package options

import (
	"flag"
	"fmt"
	"os"
)

type AppOptions struct {
	coreID        uint64
	redisSocket   string
	redisPassword string
	maxJobs       int
}

func (o *AppOptions) CoreID() uint64 {
	return o.coreID
}

func (o *AppOptions) RedisSocket() string {
	return o.redisSocket
}

func (o *AppOptions) RedisPassword() string {
	return o.redisPassword
}

func (o *AppOptions) MaxJobs() int {
	return o.maxJobs
}

func (o *AppOptions) Validate() []error {
	errors := make([]error, 0)
	if o.coreID == 0 {
		errors = append(errors, fmt.Errorf("-core-id is required"))
	}

	if o.redisSocket == "" {
		errors = append(errors, fmt.Errorf("-redis-socket is required"))
	}

	return errors
}

var Options AppOptions

func init() {
	help := false
	flag.BoolVar(&help, "h", false, "Print this help screen")
	flag.Uint64Var(&Options.coreID, "core-id", 0, "Core ID")
	flag.StringVar(&Options.redisSocket, "redis-socket", "", "Path to the redis socket")
	flag.StringVar(&Options.redisPassword, "redis-password", "", "Redis password [optional]")
	flag.IntVar(&Options.maxJobs, "max-jobs", 100, "Max number of jobs that can run concurrently")

	flag.Parse()

	printHelp := func() {
		fmt.Println("coreX [options]")
		flag.PrintDefaults()
	}

	if help {
		printHelp()
		os.Exit(0)
	}
}
