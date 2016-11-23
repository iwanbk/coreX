package process

import (
	"github.com/g8os/core.base/pm/core"
	"github.com/g8os/core.base/pm/stream"
	"github.com/op/go-logging"
	"syscall"
)

const (
	CommandSystem = "core.system"
)

var (
	log = logging.MustGetLogger("process")
)

type GetPID func() (int, error)

type PIDTable interface {
	//Register atomic registration of PID. MUST grantee that that no wait4 will happen
	//on any of the child process until the register operation is done.
	Register(g GetPID) error
	WaitPID(pid int) *syscall.WaitStatus
}

//ProcessStats holds process cpu and memory usage
type ProcessStats struct {
	Cmd   *core.Command `json:"cmd,omitempty"`
	CPU   float64       `json:"cpu"`
	RSS   uint64        `json:"rss"`
	VMS   uint64        `json:"vms"`
	Swap  uint64        `json:"swap"`
	Debug string        `json:"debug,ommitempty"`
}

//Process interface
type Process interface {
	Command() *core.Command
	Run() (<-chan *stream.Message, error)
	Kill()
	GetStats() *ProcessStats
}

type ProcessFactory func(PIDTable, *core.Command) Process
