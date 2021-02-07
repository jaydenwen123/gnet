// Copyright (c) 2019 Andy Pan
// Copyright (c) 2018 Joshua J Baker
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

// +build linux freebsd dragonfly darwin

package gnet

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/errors"
	"github.com/panjf2000/gnet/internal/logging"
	"github.com/panjf2000/gnet/internal/netpoll"
)

type server struct {
	ln           *listener          // the listener for accepting new connections
	// 这个复杂均衡里面存放的是subReactors。
	lb           loadBalancer       // event-loops for handling events
	wg           sync.WaitGroup     // event-loop close WaitGroup
	opts         *Options           // options with server
	once         sync.Once          // make sure only signalShutdown once
	cond         *sync.Cond         // shutdown signaler
	// 编解码器
	codec        ICodec             // codec for TCP stream
	logger       logging.Logger     // customized logger for logging info
	ticktock     chan time.Duration // ticker channel
	// mainReactor，主要负责接收客户端连接
	mainLoop     *eventloop         // main event-loop for accepting connections
	inShutdown   int32              // whether the server is in shutdown
	// 事件处理器
	eventHandler EventHandler       // user eventHandler
}

var serverFarm sync.Map

func (svr *server) isInShutdown() bool {
	return atomic.LoadInt32(&svr.inShutdown) == 1
}

// waitForShutdown waits for a signal to shutdown.
func (svr *server) waitForShutdown() {
	svr.cond.L.Lock()
	svr.cond.Wait()
	svr.cond.L.Unlock()
}

// signalShutdown signals the server to shut down.
func (svr *server) signalShutdown() {
	svr.once.Do(func() {
		svr.cond.L.Lock()
		svr.cond.Signal()
		svr.cond.L.Unlock()
	})
}



func (svr *server) closeEventLoops() {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		_ = el.poller.Close()
		return true
	})
}

func (svr *server) startSubReactors() {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			svr.activateSubReactor(el, svr.opts.LockOSThread)
			svr.wg.Done()
		}()
		return true
	})
}
// udp和端口复用的话会走到这个
func (svr *server) activateEventLoops(numEventLoop int) (err error) {
	// Create loops locally and bind the listeners.
	for i := 0; i < numEventLoop; i++ {
		l := svr.ln
		// udp
		if i > 0 && svr.opts.ReusePort {
			// 多个listener，监听同一个地址和端口
			if l, err = initListener(svr.ln.network, svr.ln.addr, svr.ln.reusePort); err != nil {
				return
			}
		}

		var p *netpoll.Poller
		if p, err = netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.ln = l
			el.svr = svr
			el.poller = p
			el.packet = make([]byte, 0x10000)
			el.connections = make(map[int]*conn)
			el.eventHandler = svr.eventHandler
			el.calibrateCallback = svr.lb.calibrate
			_ = el.poller.AddRead(el.ln.fd)
			svr.lb.register(el)
		} else {
			return
		}
	}

	// Start event-loops in background.
	svr.startEventLoops()

	return
}

func (svr *server) startEventLoops() {
	// 遍历所有的subReactor，然后监听读写事件
	svr.lb.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			// 开始监听事件
			el.loopRun(svr.opts.LockOSThread)
			svr.wg.Done()
		}()
		return true
	})
}

func (svr *server) activateReactors(numEventLoop int) error {
	for i := 0; i < numEventLoop; i++ {
		if p, err := netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			// 这儿listener是同一个，没啥关系，因为其他的actor不会监听客户端连接
			el.ln = svr.ln
			el.svr = svr
			el.poller = p
			el.packet = make([]byte, 0x10000)
			el.connections = make(map[int]*conn)
			el.eventHandler = svr.eventHandler
			el.calibrateCallback = svr.lb.calibrate
			svr.lb.register(el)
		} else {
			return err
		}
	}

	// 开始所有的subReactor
	// Start sub reactors in background.
	svr.startSubReactors()

	// epoll_create()
	if p, err := netpoll.OpenPoller(); err == nil {
		el := new(eventloop)
		el.ln = svr.ln
		el.idx = -1
		el.poller = p
		el.svr = svr
		// 注册读事件，接收客户端连接
		_ = el.poller.AddRead(el.ln.fd)
		svr.mainLoop = el

		// 开始mainReactor
		// Start main reactor in background.
		svr.wg.Add(1)
		go func() {
			svr.activateMainReactor(svr.opts.LockOSThread)
			svr.wg.Done()
		}()
	} else {
		return err
	}

	return nil
}

func (svr *server) start(numEventLoop int) error {
	// udp或者端口重用的的话直接activateEventLoops
	if svr.opts.ReusePort || svr.ln.network == "udp" {
		// 这个里面每个actor都可以接收客户端的连接，具体在loop_bsd的handleEvent中有体现loopAccept
		return svr.activateEventLoops(numEventLoop)
	}

	return svr.activateReactors(numEventLoop)
}

func (svr *server) stop(s Server) {
	// Wait on a signal for shutdown
	svr.waitForShutdown()

	svr.eventHandler.OnShutdown(s)

	// Notify all loops to close by closing all listeners
	svr.lb.iterate(func(i int, el *eventloop) bool {
		sniffErrorAndLog(el.poller.Trigger(func() error {
			return errors.ErrServerShutdown
		}))
		return true
	})

	if svr.mainLoop != nil {
		svr.ln.close()
		sniffErrorAndLog(svr.mainLoop.poller.Trigger(func() error {
			return errors.ErrServerShutdown
		}))
	}

	// Wait on all loops to complete reading events
	svr.wg.Wait()

	svr.closeEventLoops()

	if svr.mainLoop != nil {
		sniffErrorAndLog(svr.mainLoop.poller.Close())
	}

	atomic.StoreInt32(&svr.inShutdown, 1)
}

func serve(eventHandler EventHandler, listener *listener, options *Options, protoAddr string) error {
	// Figure out the correct number of loops/goroutines to use.
	numEventLoop := 1
	if options.Multicore {
		numEventLoop = runtime.NumCPU()
	}
	if options.NumEventLoop > 0 {
		numEventLoop = options.NumEventLoop
	}

	svr := new(server)
	svr.opts = options
	svr.eventHandler = eventHandler
	svr.ln = listener

	// 选择负责均衡策略
	switch options.LB {
	case RoundRobin:
		svr.lb = new(roundRobinLoadBalancer)
	case LeastConnections:
		svr.lb = new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		svr.lb = new(sourceAddrHashLoadBalancer)
	}

	svr.cond = sync.NewCond(&sync.Mutex{})
	svr.ticktock = make(chan time.Duration, channelBuffer(1))
	svr.logger = logging.DefaultLogger
	svr.codec = func() ICodec {
		if options.Codec == nil {
			return new(BuiltInFrameCodec)
		}
		return options.Codec
	}()

	server := Server{
		svr:          svr,
		Multicore:    options.Multicore,
		Addr:         listener.lnaddr,
		NumEventLoop: numEventLoop,
		ReusePort:    options.ReusePort,
		TCPKeepAlive: options.TCPKeepAlive,
	}
	switch svr.eventHandler.OnInitComplete(server) {
	case None:
	case Shutdown:
		return nil
	}

	// 调用start，开启所有的reactor
	if err := svr.start(numEventLoop); err != nil {
		svr.closeEventLoops()
		svr.logger.Errorf("gnet server is stopping with error: %v", err)
		return err
	}
	defer svr.stop(server)

	serverFarm.Store(protoAddr, svr)

	return nil
}
