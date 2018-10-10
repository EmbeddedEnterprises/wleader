package wleader

import (
	"context"
	"errors"
	"fmt"

	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
)

// BecameLeader is a callback function type which is used to inform the user
// that its session became the leader.
type BecameLeader func()

// LeaderElection is a function which performs a simple, but reliable leader election.
// It requires a WAMP router with shared registrations and the session meta API.
// It works by having a simple endpoint returning the callee's session ID and invocation_policy first.
// After the registration, the library calls the endpoint (for a single callee, i.e. the first instance)
// it returns its own session id.
// After the initial leader check, it registers on the MetaEventSessionOnLeave.
// When the leader leaves the WAMP realm, it queries for the new leader.
func LeaderElection(c *client.Client, groupIdentifier string, onBecameLeader BecameLeader) error {
	activeSession := wamp.ID(0)
	if err := c.Register(
		fmt.Sprintf("ee.leader.%s", groupIdentifier),
		func(_ context.Context, _ wamp.List, _, _ wamp.Dict) *client.InvokeResult {
			return &client.InvokeResult{
				Args: wamp.List{c.ID()},
			}
		},
		wamp.Dict{
			wamp.OptInvoke: wamp.InvokeFirst,
		},
	); err != nil {
		return err
	}
	if res, err := c.Call(context.Background(), fmt.Sprintf("ee.leader.%s", groupIdentifier), nil, nil, nil, ""); err != nil {
		return err
	} else if len(res.Arguments) == 0 {
		return errors.New("leader returned nothing")
	} else if id, ok := wamp.AsID(res.Arguments[0]); !ok {
		return errors.New("leader returned no session id")
	} else {
		activeSession = id
		if id == c.ID() {
			onBecameLeader()
		}
	}

	if err := c.Subscribe(string(wamp.MetaEventSessionOnLeave), func(args wamp.List, _, _ wamp.Dict) {
		// We have to use a goroutine here, since nexus calls topic handlers synchronously
		go func() {
			if len(args) == 0 {
				return
			}
			if id, ok := wamp.AsID(args[0]); !ok || id != activeSession {
				return
			} else if res, err := c.Call(context.Background(), fmt.Sprintf("ee.leader.%s", groupIdentifier), nil, nil, nil, ""); err != nil {
				return
			} else if len(res.Arguments) == 0 {
				return
			} else if id, ok := wamp.AsID(res.Arguments[0]); !ok {
				return
			} else {
				activeSession = id
				if id == c.ID() {
					onBecameLeader()
				}
			}
		}()
	}, nil); err != nil {
		return err
	}

	return nil
}
