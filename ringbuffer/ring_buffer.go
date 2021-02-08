// Copyright (c) 2019 Chao yuepan, Andy Pan, Allen Xu
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

package ringbuffer

import (
	"errors"

	"github.com/panjf2000/gnet/internal"
	"github.com/panjf2000/gnet/pool/bytebuffer"
)

const initSize = 1 << 12 // 4096 bytes for the first-time allocation on ring-buffer.

// ErrIsEmpty will be returned when trying to read a empty ring-buffer.
var ErrIsEmpty = errors.New("ring-buffer is empty")

// RingBuffer is a circular buffer that implement io.ReaderWriter interface.
type RingBuffer struct {
	buf     []byte
	size    int
	// 用来计算余数
	mask    int
	r       int // next position to read   ->rear
	w       int // next position to write  ->front
	isEmpty bool
}

// New returns a new RingBuffer whose buffer has the given size.
func New(size int) *RingBuffer {
	if size == 0 {
		return &RingBuffer{isEmpty: true}
	}
	// 1000->1024
	// 512->512
	size = internal.CeilToPowerOfTwo(size)
	return &RingBuffer{
		buf:     make([]byte, size),
		size:    size,
		mask:    size - 1,
		isEmpty: true,
	}
}

// LazyRead reads the bytes with given length but will not move the pointer of "read".
func (r *RingBuffer) LazyRead(len int) (head []byte, tail []byte) {
	if r.isEmpty {
		return
	}

	if len <= 0 {
		return
	}

	// r~w
	if r.w > r.r {
		n := r.w - r.r // Length
		if n > len {
			n = len
		}
		head = r.buf[r.r : r.r+n]
		return
	}

	// r~size,0~w
	n := r.size - r.r + r.w // Length
	if n > len {
		n = len
	}

	if r.r+n <= r.size {
		head = r.buf[r.r : r.r+n]
	} else {
		// 分散在两段，通过head装r~size，tail装0~w的数据
		c1 := r.size - r.r
		head = r.buf[r.r:]
		c2 := n - c1
		tail = r.buf[:c2]
	}

	return
}

// LazyReadAll reads the all bytes in this ring-buffer but will not move the pointer of "read".
func (r *RingBuffer) LazyReadAll() (head []byte, tail []byte) {
	if r.isEmpty {
		return
	}

	if r.w > r.r {
		head = r.buf[r.r:r.w]
		return
	}

	head = r.buf[r.r:]
	if r.w != 0 {
		tail = r.buf[:r.w]
	}

	return
}

// Shift shifts the "read" pointer.
func (r *RingBuffer) Shift(n int) {
	if n <= 0 {
		return
	}

	if n < r.Length() {
		r.r = (r.r + n) & r.mask
		if r.r == r.w {
			r.isEmpty = true
		}
	} else {
		r.Reset()
	}
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p)) and any error
// encountered.
// Even if Read returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally returns what is available instead of waiting
// for more.
// When Read encounters an error or end-of-file condition after successfully reading n > 0 bytes,
// it returns the number of bytes read. It may return the (non-nil) error from the same call or return the
// error (and n == 0) from a subsequent call.
// Callers should always process the n > 0 bytes returned before considering the error err.
// Doing so correctly handles I/O errors that happen after reading some bytes and also both of the allowed EOF
// behaviors.
// 对应出队操作
func (r *RingBuffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if r.isEmpty {
		return 0, ErrIsEmpty
	}

	// r~w
	if r.w > r.r {
		// 可读r~w
		n = r.w - r.r
		// 可读的数据大于buf
		if n > len(p) {
			n = len(p)
		}
		copy(p, r.buf[r.r:r.r+n])
		// 更新读的下标
		r.r += n
		// 当读到r==w时，为空
		if r.r == r.w {
			r.isEmpty = true
		}
		return
	}

	// w<=r
	// 可读数据r~size,0~w
	n = r.size - r.r + r.w
	if n > len(p) {
		n = len(p)
	}

	// 读的数据在r~size之间
	if r.r+n <= r.size {
		copy(p, r.buf[r.r:r.r+n])
	// 	读的数据分散在两段
	} else {
		c1 := r.size - r.r
		// 第一段
		copy(p, r.buf[r.r:])
		// 第二段，要读取的数据
		c2 := n - c1
		copy(p[c1:], r.buf[:c2])
	}
	// 最终更新r的信息，取余
	r.r = (r.r + n) & r.mask
	// 如果读完了，则设置为空
	if r.r == r.w {
		r.isEmpty = true
	}

	return n, err
}

// ReadByte reads and returns the next byte from the input or ErrIsEmpty.
func (r *RingBuffer) ReadByte() (b byte, err error) {
	if r.isEmpty {
		return 0, ErrIsEmpty
	}
	b = r.buf[r.r]
	r.r++
	if r.r == r.size {
		r.r = 0
	}
	if r.r == r.w {
		r.isEmpty = true
	}

	return b, err
}

// Write writes len(p) bytes from p to the underlying buf.
// It returns the number of bytes written from p (n == len(p) > 0) and any error encountered that caused the write to
// stop early.
// If the length of p is greater than the writable capacity of this ring-buffer, it will allocate more memory to
// this ring-buffer.
// Write must not modify the slice data, even temporarily.
func (r *RingBuffer) Write(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return 0, nil
	}

	// 扩容
	free := r.Free()
	if n > free {
		// 扩容
		r.malloc(n - free)
	}

	// w>r大时，c1=(w~size),(0~r)都可写
	if r.w >= r.r {

		c1 := r.size - r.w
		// 判断如果
		if c1 >= n {
			// 写到w开始的位置
			copy(r.buf[r.w:], p)
			// 增加w的偏移量
			r.w += n
		} else {
			copy(r.buf[r.w:], p[:c1])
			// 剩余要写的
			c2 := n - c1
			// 写入到0~r之间
			copy(r.buf, p[c1:])
			r.w = c2
		// 	走到这儿时，w<r
		}
	} else {
		// w<r,w~r之间可写
		copy(r.buf[r.w:], p)
		r.w += n
	}

	// 循环写
	if r.w == r.size {
		r.w = 0
	}

	r.isEmpty = false

	return n, err
}

// WriteByte writes one byte into buffer.
func (r *RingBuffer) WriteByte(c byte) error {
	if r.Free() < 1 {
		r.malloc(1)
	}
	r.buf[r.w] = c
	r.w++

	if r.w == r.size {
		r.w = 0
	}
	r.isEmpty = false

	return nil
}

// Length returns the length of available read bytes.
func (r *RingBuffer) Length() int {
	if r.r == r.w {
		if r.isEmpty {
			return 0
		}
		return r.size
	}
	// 可读r~w
	if r.w > r.r {
		return r.w - r.r
	}
	// r.w<r.r
	// 可读：r~size,0~w,
	return r.size - r.r + r.w
}

// Len returns the length of the underlying buffer.
func (r *RingBuffer) Len() int {
	return len(r.buf)
}

// Cap returns the size of the underlying buffer.
func (r *RingBuffer) Cap() int {
	return r.size
}

// Free returns the length of available bytes to write.
func (r *RingBuffer) Free() int {
	if r.r == r.w {
		// 可能是空，也可能是满
		// 下面进行判断
		if r.isEmpty {
			return r.size
		}
		return 0
	}

	if r.w < r.r {
		// 可写空间w~r
		return r.r - r.w
	}
	// r.w > r.r
	// 可写空间：0~r.r，r.size-r.w
	return r.size - r.w + r.r
}

// WriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
func (r *RingBuffer) WriteString(s string) (n int, err error) {
	return r.Write(internal.StringToBytes(s))
}

// ByteBuffer returns all available read bytes. It does not move the read pointer and only copy the available data.
func (r *RingBuffer) ByteBuffer() *bytebuffer.ByteBuffer {
	if r.isEmpty {
		return nil
	} else if r.w == r.r {
		// 满了，直接返回所有的数据
		bb := bytebuffer.Get()
		_, _ = bb.Write(r.buf[r.r:])
		_, _ = bb.Write(r.buf[:r.w])
		return bb
	}

	bb := bytebuffer.Get()
	// 返回r~w的数据
	if r.w > r.r {
		_, _ = bb.Write(r.buf[r.r:r.w])
		return bb
	}
	// 分两段，r~size,0~w
	_, _ = bb.Write(r.buf[r.r:])

	if r.w != 0 {
		_, _ = bb.Write(r.buf[:r.w])
	}

	return bb
}

// WithByteBuffer combines the available read bytes and the given bytes. It does not move the read pointer and
// only copy the available data.
func (r *RingBuffer) WithByteBuffer(b []byte) *bytebuffer.ByteBuffer {
	if r.isEmpty {
		return &bytebuffer.ByteBuffer{B: b}
	// 	满
	} else if r.w == r.r {
		bb := bytebuffer.Get()
		_, _ = bb.Write(r.buf[r.r:])
		_, _ = bb.Write(r.buf[:r.w])
		_, _ = bb.Write(b)
		return bb
	}

	bb := bytebuffer.Get()
	if r.w > r.r {
		_, _ = bb.Write(r.buf[r.r:r.w])
		_, _ = bb.Write(b)
		return bb
	}

	_, _ = bb.Write(r.buf[r.r:])

	if r.w != 0 {
		_, _ = bb.Write(r.buf[:r.w])
	}
	_, _ = bb.Write(b)

	return bb
}

// IsFull returns this ringbuffer is full.
func (r *RingBuffer) IsFull() bool {
	return r.r == r.w && !r.isEmpty
}

// IsEmpty returns this ringbuffer is empty.
func (r *RingBuffer) IsEmpty() bool {
	return r.isEmpty
}

// Reset the read pointer and writer pointer to zero.
func (r *RingBuffer) Reset() {
	r.r = 0
	r.w = 0
	r.isEmpty = true
}

// 扩容
func (r *RingBuffer) malloc(cap int) {
	var newCap int
	// size==0空的话，直接初始化为1<<12=2^12=2^10*2^2=4*1024=4096byte=4k
	if r.size == 0 && initSize >= cap {
		newCap = initSize
	} else {
		//  1000->1024
		newCap = internal.CeilToPowerOfTwo(r.size + cap)
	}
	newBuf := make([]byte, newCap)
	oldLen := r.Length()
	_, _ = r.Read(newBuf)
	r.r = 0
	r.w = oldLen
	r.size = newCap
	r.mask = newCap - 1
	r.buf = newBuf
}
