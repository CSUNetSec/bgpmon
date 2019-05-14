package modules

import (
	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/db"
	"github.com/CSUNetSec/bgpmon/util"

	"github.com/araddon/dateparse"
)

type hijackModule struct {
	*BaseTask
}

func (h *hijackModule) Run(args map[string]string) {
	reqKeys := []string{
		"entity",
		"start",
		"end",
		"session",
	}

	if !util.CheckForKeys(args, reqKeys...) {
		h.logger.Errorf("Need entity, start, and end keys")
		return
	}

	sessionName := args["session"]
	entityName := args["entity"]
	start, err := dateparse.ParseAny(args["start"])
	if err != nil {
		h.logger.Errorf("Error parsing start string: %s", args["start"])
		return
	}

	end, err := dateparse.ParseAny(args["end"])
	if err != nil {
		h.logger.Errorf("Error parsing end string: %s", args["end"])
		return
	}

	entity, err := h.readEntity(sessionName, entityName)
	if err != nil {
		h.logger.Errorf("Error reading entity name: %s %s", entityName, err)
		return
	}
	h.logger.Infof("Successfully found entity: %+v", entity)

	captureOptions := db.NewCaptureFilterOptions(db.AnyCollector, start.UTC(), end.UTC())
	captureOptions.AllowAdvPrefixes(entity.OwnedPrefixes...)
	capStream, err := h.server.OpenReadStream(sessionName, db.SessionReadCapture, captureOptions)
	if err != nil {
		h.logger.Errorf("Error opening capture stream: %s", err)
		return
	}
	defer capStream.Close()

	msgCt := 0
	events := 0
	for capStream.Read() {
		msgCt++
		cap := capStream.Data().(*db.Capture)

		if h.isEvent(entity, cap) {
			events++
		}
	}

	if err := capStream.Err(); err != nil {
		h.logger.Errorf("Capture stream error: %s", err)
		return
	}

	h.logger.Infof("Scanned %d messages, detected %d events!", msgCt, events)
}

// isEvent determines whether or not a capture qualifies as a hijack.
// Currently, a capture qualifies as a hijack if it contains a prefix owned
// by the entity (as filtered above) but did not originate from one of the
// origins owned by the entity.
func (h *hijackModule) isEvent(ent *db.Entity, cap *db.Capture) bool {
	for _, as := range ent.OwnedOrigins {
		if cap.Origin == as {
			return false
		}
	}

	return true
}

// readEntity opens a read entity stream on the server, and returns an entity
// with the provided name, or an error.
func (h *hijackModule) readEntity(session, entName string) (*db.Entity, error) {
	opts := db.NewEntityFilterOptions(entName)
	entityStream, err := h.server.OpenReadStream(session, db.SessionReadEntity, opts)
	if err != nil {
		return nil, err
	}
	defer entityStream.Close()

	if !entityStream.Read() {
		return nil, entityStream.Err()
	}

	entity := entityStream.Data().(*db.Entity)

	return entity, nil
}

// newHijackModule is the module maker for this module.
func newHijackModule(s core.BgpmondServer, l util.Logger) core.Module {
	return &hijackModule{NewBaseTask(s, l, "hijack")}
}

func init() {
	opts := "entity: the name of the entity to search for hijacks on\n" +
		"session: the name of the session to read captures from.\n" +
		"start: the timestamp to start reading from.\n" +
		"end: the timestamp to read to."

	hijackHandle := core.ModuleHandler{
		Info: core.ModuleInfo{
			Type:        "hijack",
			Description: "Scan for BGP hijacks",
			Opts:        opts,
		},
		Maker: newHijackModule,
	}
	core.RegisterModule(hijackHandle)
}
