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

// +build linux freebsd dragonfly darwin

package gnet

import (
	"os"

	"github.com/panjf2000/gnet/errors"
	"github.com/panjf2000/gnet/internal/netpoll"
	"golang.org/x/sys/unix"
)
// 接受新的客户端连接
func (svr *server) acceptNewConnection(fd int) error {
	// accept()
	nfd, sa, err := unix.Accept(fd)
	if err != nil {
		if err == unix.EAGAIN {
			return nil
		}
		return errors.ErrAcceptSocket
	}
	// 设置非阻塞nonblocking
	if err = os.NewSyscallError("fcntl nonblock",
		unix.SetNonblock(nfd, true)); err != nil {
		return err
	}

	netAddr := netpoll.SockaddrToTCPOrUnixAddr(sa)
	// 负载均衡，找到合适的subReactor
	el := svr.lb.next(netAddr)
	// 封装新的链接
	c := newTCPConn(nfd, el, sa, netAddr)

	// 最后将conn交给subReactor管理
	_ = el.poller.Trigger(func() (err error) {
		// 注册读事件
		if err = el.poller.AddRead(nfd); err != nil {
			return
		}
		// 添加到链接管理中
		el.connections[nfd] = c
		err = el.loopOpen(c)
		return
	})
	return nil
}
