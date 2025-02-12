// Copyright (c) 2019 Andy Pan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// +build freebsd dragonfly darwin

package gnet

import (
	"runtime"

	"github.com/panjf2000/gnet/internal/netpoll"
)

func (svr *server) activateMainReactor(lockOSThread bool) {
	if lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	defer svr.signalShutdown()

	err := svr.mainLoop.poller.Polling(func(fd int, filter int16) error { return svr.acceptNewConnection(fd) })
	svr.logger.Infof("Main reactor is exiting due to error: %v", err)
}

func (svr *server) activateSubReactor(el *eventloop, lockOSThread bool) {
	if lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	defer func() {
		el.closeAllConns()
		if el.idx == 0 && svr.opts.Ticker {
			close(svr.ticktock)
		}
		svr.signalShutdown()
	}()

	if el.idx == 0 && svr.opts.Ticker {
		go el.loopTicker()
	}

	err := el.poller.Polling(func(fd int, filter int16) error {
		if c, ack := el.connections[fd]; ack {
			if filter == netpoll.EVFilterSock {
				return el.loopCloseConn(c, nil)
			}

			switch c.outboundBuffer.IsEmpty() {
			// Don't change the ordering of processing EVFILT_WRITE | EVFILT_READ | EV_ERROR/EV_EOF unless you're 100%
			// sure what you're doing!
			// Re-ordering can easily introduce bugs and bad side-effects, as I found out painfully in the past.
			case false:
				if filter == netpoll.EVFilterWrite {
					return el.loopWrite(c)
				}
			case true:
				if filter == netpoll.EVFilterRead {
					return el.loopRead(c)
				}
			}
		}
		return nil
	})
	svr.logger.Infof("Event-loop(%d) is exiting normally on the signal error: %v", el.idx, err)
}
