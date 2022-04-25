package dht

import (
	"bytes"
	"fmt"
	"strings"
)

// bitmap represents a bit array.
type bitmap struct {
	Size int		// bit 数目
	data []byte		//
}

// newBitmap returns a size-length bitmap pointer.
//
// size 是 bit 的数目，因为底层存储是 byte ，而每个 byte 存储 8 个 bit ，所以需要把 size 对 8 做一下 round up 。
//
// 假设 size == 13 ，需要存储 13 个 bits ，13 > 8 = 1 ，13 % 8 = 5 ，所以需要两个 byte 来存储。
//
// 存储是从低到高:
//   [x,x,x,0,0,0,0,0][0,0,0,0,0,0,0,0]
func newBitmap(size int) *bitmap {
	// `size>>3` is equal to `size/8`
	// `size&0x07` is equal to `size % 8`
	//
	// so, the result will be rounded up to to the nearest 8 bits.
	//
	div, mod := size>>3, size&0x07

	// 如果 size 是 8 的 n 次倍，且还有余数，那么就多算一个 byte
	if mod > 0 {
		div++
	}

	// 存储原始 size 和底层 bytes 数组
	return &bitmap{size, make([]byte, div)}
}

// newBitmapFrom returns a new copyed bitmap pointer which
// newBitmap.data = other.data[:size].
//
// 创建一个有 size 个 bits 的位图 this ，并将前 other 中 other.size 个 bit 拷贝过来。
func newBitmapFrom(other *bitmap, size int) *bitmap {
	// 构造一个能容纳 size 个 bits 的位图
	bitmap := newBitmap(size)

	// 位数对齐
	if size > other.Size {
		size = other.Size
	}

	// 假设 size == 13 ，需要存储 13 个 bits ，13 > 8 = 1 ，13 % 8 = 5 ，所以需要两个 byte 来存储。
	// 存储是从低到高:
	//  [x,x,x,0,0,0,0,0][0,0,0,0,0,0,0,0]


	// 所以，拷贝 bitmap 需要先拷贝低位的完整 bytes ，再拷贝高位的不完整 byte
	div := size >> 3
	for i := 0; i < div; i++ {
		bitmap.data[i] = other.data[i]
	}

	for i := div << 3; i < size; i++ {
		if other.Bit(i) == 1 {
			bitmap.Set(i)
		}
	}

	return bitmap
}

// newBitmapFromBytes returns a bitmap pointer created from a byte array.
func newBitmapFromBytes(data []byte) *bitmap {
	bitmap := newBitmap(len(data) << 3)
	copy(bitmap.data, data)
	return bitmap
}

// newBitmapFromString returns a bitmap pointer created from a string.
func newBitmapFromString(data string) *bitmap {
	return newBitmapFromBytes([]byte(data))
}

// Bit returns the bit at index.
func (bitmap *bitmap) Bit(index int) int {
	if index >= bitmap.Size {
		panic("index out of range")
	}

	// 先定位到是第几个 byte ，再定位是第几个 bit
	div, mod := index>>3, index&0x07
	return int((uint(bitmap.data[div]) & (1 << uint(7-mod))) >> uint(7-mod))
}

// set sets the bit at index `index`. If bit is true, set 1, otherwise set 0.
func (bitmap *bitmap) set(index int, bit int) {
	if index >= bitmap.Size {
		panic("index out of range")
	}

	div, mod := index>>3, index&0x07
	shift := byte(1 << uint(7-mod))

	bitmap.data[div] &= ^shift
	if bit > 0 {
		bitmap.data[div] |= shift
	}
}

// Set sets the bit at idnex to 1.
func (bitmap *bitmap) Set(index int) {
	bitmap.set(index, 1)
}

// Unset sets the bit at idnex to 0.
func (bitmap *bitmap) Unset(index int) {
	bitmap.set(index, 0)
}

// Compare compares the prefixLen-prefix of two bitmap.
//   - If bitmap.data[:prefixLen] < other.data[:prefixLen], return -1.
//   - If bitmap.data[:prefixLen] > other.data[:prefixLen], return 1.
//   - Otherwise return 0.
func (bitmap *bitmap) Compare(other *bitmap, prefixLen int) int {
	if prefixLen > bitmap.Size || prefixLen > other.Size {
		panic("index out of range")
	}

	div, mod := prefixLen>>3, prefixLen&0x07
	res := bytes.Compare(bitmap.data[:div], other.data[:div])
	if res != 0 {
		return res
	}

	for i := div << 3; i < (div<<3)+mod; i++ {
		bit1, bit2 := bitmap.Bit(i), other.Bit(i)
		if bit1 > bit2 {
			return 1
		} else if bit1 < bit2 {
			return -1
		}
	}

	return 0
}

// Xor returns the xor value of two bitmap.
func (bitmap *bitmap) Xor(other *bitmap) *bitmap {
	// 位数必须相同，否则无法执行逻辑运算
	if bitmap.Size != other.Size {
		panic("size not the same")
	}
	// 执行 XOR ，并保存结果
	distance := newBitmap(bitmap.Size)
	xor(distance.data, bitmap.data, other.data)
	// 返回 XOR 结果
	return distance
}

// String returns the bit sequence string of the bitmap.
func (bitmap *bitmap) String() string {
	div, mod := bitmap.Size>>3, bitmap.Size&0x07
	buff := make([]string, div+mod)

	for i := 0; i < div; i++ {
		buff[i] = fmt.Sprintf("%08b", bitmap.data[i])
	}

	for i := div; i < div+mod; i++ {
		buff[i] = fmt.Sprintf("%1b", bitmap.Bit(div*8+(i-div)))
	}

	return strings.Join(buff, "")
}

// RawString returns the string value of bitmap.data.
func (bitmap *bitmap) RawString() string {
	return string(bitmap.data)
}
