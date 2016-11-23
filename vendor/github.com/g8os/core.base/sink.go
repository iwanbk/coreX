package core

import (
	"github.com/g8os/core.base/pm"
	"github.com/g8os/core.base/pm/core"
	"github.com/g8os/core.base/settings"
	"time"
)

const (
	ReconnectSleepTime = 10 * time.Second
)

type Sink interface {
	Run()
}

type SinkClient interface {
	GetNext(command *core.Command) error
	Respond(result *core.JobResult) error
}

type sinkImpl struct {
	key    string
	mgr    *pm.PM
	client SinkClient
}

func getKeys(m map[string]SinkClient) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	return keys
}

func NewSink(key string, mgr *pm.PM, client SinkClient) Sink {
	poll := &sinkImpl{
		key:    key,
		mgr:    mgr,
		client: client,
	}

	return poll
}

func (poll *sinkImpl) handler(cmd *core.Command, result *core.JobResult) {
	if err := poll.client.Respond(result); err != nil {
		log.Errorf("Failed to respond to command %s: %s", cmd, err)
	}
}

func (poll *sinkImpl) run() {
	lastError := time.Now()

	poll.mgr.AddRouteResultHandler(core.Route(poll.key), poll.handler)

	for {
		var command core.Command
		err := poll.client.GetNext(&command)
		if err != nil {
			log.Errorf("Failed to get next command from %s: %s", poll.client, err)
			if time.Now().Sub(lastError) < ReconnectSleepTime {
				time.Sleep(ReconnectSleepTime)
			}
			lastError = time.Now()

			continue
		}

		command.Route = core.Route(poll.key)

		log.Infof("Starting command %s", &command)

		if command.Queue == "" {
			poll.mgr.PushCmd(&command)
		} else {
			poll.mgr.PushCmdToQueue(&command)
		}
	}
}

func (poll *sinkImpl) Run() {
	go poll.run()
}

/*
StartSinks starts the long polling routines and feed the manager with received commands
*/
func StartSinks(mgr *pm.PM, sinks map[string]SinkClient) {
	var keys []string
	if len(settings.Settings.Channel.Cmds) > 0 {
		keys = settings.Settings.Channel.Cmds
	} else {
		keys = getKeys(sinks)
	}

	for _, key := range keys {
		sinkCl, ok := sinks[key]
		if !ok {
			log.Fatalf("No contoller with name '%s'", key)
		}

		poll := NewSink(key, mgr, sinkCl)
		poll.Run()
	}
}
