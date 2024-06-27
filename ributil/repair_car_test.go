package ributil

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/atboosty/ribs/carlog"
	"github.com/filecoin-project/lotus/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/multiformats/go-multihash"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

/*
echo "natmoiunoeutmi" | ipfs add --raw-leaves
# Output: added bafkreidqw5niodrmt5nza7on5f7mmjx4vymbiwmxamd4nihtoldyviioxu

echo "etominpcyomdrpycmx" | ipfs add --raw-leaves
# Output: added bafkreibn4ktninvehj4iwerimg45kyo2h3wj5tcn53wjwhfeez4wyih7xq

echo ".rpyhlxmbl.crxf." | ipfs add --raw-leaves
# Output: added bafkreibftlukrlh3iapgjmcemz4gom4s4fpli6tqjggor3pbhdrgfpipl4

echo "crp.ydrcbd.py" | ipfs add --raw-leaves
# Output: added bafkreifgh655vcevj45q7uqpftwzy7ubfqkpwvh77enfrnaxac5t4gro5q

# Commands to create DAG nodes
ipfs dag put --input-codec=dag-json <<< '[{"/": "bafkreidqw5niodrmt5nza7on5f7mmjx4vymbiwmxamd4nihtoldyviioxu"}, {"/": "bafkreibn4ktninvehj4iwerimg45kyo2h3wj5tcn53wjwhfeez4wyih7xq"}]'
# Output: bafyreielemrpjfp3tqlperrfcdtqp2kjranb34tpnwjc5ir5x7rjm7pevy

ipfs dag put --input-codec=dag-json <<< '[{"/": "bafkreibftlukrlh3iapgjmcemz4gom4s4fpli6tqjggor3pbhdrgfpipl4"}, {"/": "bafkreifgh655vcevj45q7uqpftwzy7ubfqkpwvh77enfrnaxac5t4gro5q"}]'
# Output: bafyreidzjgmslbs4fw45ymtagkxnffdeiag4w6477h6iwb6bz2kovjpcpm

ipfs dag put --input-codec=dag-json <<< '[{"/": "bafyreielemrpjfp3tqlperrfcdtqp2kjranb34tpnwjc5ir5x7rjm7pevy"}, {"/": "bafyreidzjgmslbs4fw45ymtagkxnffdeiag4w6477h6iwb6bz2kovjpcpm"}]'
# Output: bafyreig67dpkzct5dlv6bopobeti72tttybwtyg63xh25qoan3t7i7aj2a

# Command to export data from the DAG
ipfs dag export bafyreig67dpkzct5dlv6bopobeti72tttybwtyg63xh25qoan3t7i7aj2a
*/
var testCar = []byte{
	0x3a, 0xa2, 0x65, 0x72, 0x6f, 0x6f, 0x74, 0x73, 0x81, 0xd8, 0x2a, 0x58, 0x25, 0x00, 0x01, 0x71,
	0x12, 0x20, 0xde, 0xf8, 0xde, 0xac, 0x8a, 0x7d, 0x1a, 0xeb, 0xe0, 0xb9, 0xee, 0x09, 0x26, 0x8f,
	0xea, 0x73, 0x9e, 0x03, 0x69, 0xe0, 0xde, 0xdd, 0xcf, 0xae, 0xc1, 0xc0, 0x6e, 0xe7, 0xf4, 0x7c,
	0x09, 0xd0, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x01, 0x77, 0x01, 0x71, 0x12, 0x20,
	0xde, 0xf8, 0xde, 0xac, 0x8a, 0x7d, 0x1a, 0xeb, 0xe0, 0xb9, 0xee, 0x09, 0x26, 0x8f, 0xea, 0x73,
	0x9e, 0x03, 0x69, 0xe0, 0xde, 0xdd, 0xcf, 0xae, 0xc1, 0xc0, 0x6e, 0xe7, 0xf4, 0x7c, 0x09, 0xd0,
	0x82, 0xd8, 0x2a, 0x58, 0x25, 0x00, 0x01, 0x71, 0x12, 0x20, 0x8b, 0x23, 0x22, 0xf4, 0x95, 0xfb,
	0x9c, 0x16, 0xf2, 0x46, 0x25, 0x10, 0xe7, 0x07, 0xe9, 0x49, 0x88, 0x1a, 0x1d, 0xf2, 0x6f, 0x6d,
	0x92, 0x2e, 0xa2, 0x3d, 0xbf, 0xe2, 0x96, 0x7d, 0xe4, 0xae, 0xd8, 0x2a, 0x58, 0x25, 0x00, 0x01,
	0x71, 0x12, 0x20, 0x79, 0x49, 0x99, 0x25, 0x86, 0x5c, 0x2d, 0xb9, 0xdc, 0x32, 0x60, 0x32, 0xae,
	0xd2, 0x94, 0x64, 0x40, 0x0d, 0xcb, 0x7b, 0x9f, 0xf9, 0xfc, 0x8b, 0x07, 0xc1, 0xce, 0x94, 0xea,
	0xa5, 0xe2, 0x7b, 0x77, 0x01, 0x71, 0x12, 0x20, 0x8b, 0x23, 0x22, 0xf4, 0x95, 0xfb, 0x9c, 0x16,
	0xf2, 0x46, 0x25, 0x10, 0xe7, 0x07, 0xe9, 0x49, 0x88, 0x1a, 0x1d, 0xf2, 0x6f, 0x6d, 0x92, 0x2e,
	0xa2, 0x3d, 0xbf, 0xe2, 0x96, 0x7d, 0xe4, 0xae, 0x82, 0xd8, 0x2a, 0x58, 0x25, 0x00, 0x01, 0x55,
	0x12, 0x20, 0x70, 0xb7, 0x5a, 0x87, 0x0e, 0x2c, 0x9f, 0x5b, 0x90, 0x7d, 0xcd, 0xe9, 0x7e, 0xc6,
	0x26, 0xfc, 0xae, 0x18, 0x14, 0x59, 0x97, 0x03, 0x07, 0xc6, 0xa0, 0xf3, 0x72, 0xc7, 0x8a, 0xa1,
	0x0e, 0xbd, 0xd8, 0x2a, 0x58, 0x25, 0x00, 0x01, 0x55, 0x12, 0x20, 0x2d, 0xe2, 0xa6, 0xd4, 0x36,
	0xa4, 0x3a, 0x78, 0x8b, 0x12, 0x28, 0x61, 0xb9, 0xd5, 0x61, 0xda, 0x3e, 0xec, 0x9e, 0xcc, 0x4d,
	0xee, 0xec, 0x9b, 0x1c, 0xa4, 0x26, 0x79, 0x6c, 0x20, 0xff, 0xbc, 0x33, 0x01, 0x55, 0x12, 0x20,
	0x70, 0xb7, 0x5a, 0x87, 0x0e, 0x2c, 0x9f, 0x5b, 0x90, 0x7d, 0xcd, 0xe9, 0x7e, 0xc6, 0x26, 0xfc,
	0xae, 0x18, 0x14, 0x59, 0x97, 0x03, 0x07, 0xc6, 0xa0, 0xf3, 0x72, 0xc7, 0x8a, 0xa1, 0x0e, 0xbd,
	0x6e, 0x61, 0x74, 0x6d, 0x6f, 0x69, 0x75, 0x6e, 0x6f, 0x65, 0x75, 0x74, 0x6d, 0x69, 0x0a, 0x37,
	0x01, 0x55, 0x12, 0x20, 0x2d, 0xe2, 0xa6, 0xd4, 0x36, 0xa4, 0x3a, 0x78, 0x8b, 0x12, 0x28, 0x61,
	0xb9, 0xd5, 0x61, 0xda, 0x3e, 0xec, 0x9e, 0xcc, 0x4d, 0xee, 0xec, 0x9b, 0x1c, 0xa4, 0x26, 0x79,
	0x6c, 0x20, 0xff, 0xbc, 0x65, 0x74, 0x6f, 0x6d, 0x69, 0x6e, 0x70, 0x63, 0x79, 0x6f, 0x6d, 0x64,
	0x72, 0x70, 0x79, 0x63, 0x6d, 0x78, 0x0a, 0x77, 0x01, 0x71, 0x12, 0x20, 0x79, 0x49, 0x99, 0x25,
	0x86, 0x5c, 0x2d, 0xb9, 0xdc, 0x32, 0x60, 0x32, 0xae, 0xd2, 0x94, 0x64, 0x40, 0x0d, 0xcb, 0x7b,
	0x9f, 0xf9, 0xfc, 0x8b, 0x07, 0xc1, 0xce, 0x94, 0xea, 0xa5, 0xe2, 0x7b, 0x82, 0xd8, 0x2a, 0x58,
	0x25, 0x00, 0x01, 0x55, 0x12, 0x20, 0x25, 0x9a, 0xe8, 0xa8, 0xac, 0xfb, 0x40, 0x1e, 0x64, 0xb0,
	0x44, 0x66, 0x78, 0x67, 0x33, 0x92, 0xe1, 0x5e, 0xb4, 0x7a, 0x70, 0x49, 0x8c, 0xe8, 0xed, 0xe1,
	0x38, 0xe2, 0x62, 0xbd, 0x0f, 0x5f, 0xd8, 0x2a, 0x58, 0x25, 0x00, 0x01, 0x55, 0x12, 0x20, 0xa6,
	0x3f, 0xbb, 0xda, 0x88, 0x95, 0x4f, 0x3b, 0x0f, 0xd2, 0x0f, 0x2c, 0xed, 0x9c, 0x7e, 0x81, 0x2c,
	0x14, 0xfb, 0x54, 0xff, 0xf9, 0x1a, 0x58, 0xb4, 0x17, 0x00, 0xbb, 0x3e, 0x1a, 0x2e, 0xec, 0x35,
	0x01, 0x55, 0x12, 0x20, 0x25, 0x9a, 0xe8, 0xa8, 0xac, 0xfb, 0x40, 0x1e, 0x64, 0xb0, 0x44, 0x66,
	0x78, 0x67, 0x33, 0x92, 0xe1, 0x5e, 0xb4, 0x7a, 0x70, 0x49, 0x8c, 0xe8, 0xed, 0xe1, 0x38, 0xe2,
	0x62, 0xbd, 0x0f, 0x5f, 0x2e, 0x72, 0x70, 0x79, 0x68, 0x6c, 0x78, 0x6d, 0x62, 0x6c, 0x2e, 0x63,
	0x72, 0x78, 0x66, 0x2e, 0x0a, 0x32, 0x01, 0x55, 0x12, 0x20, 0xa6, 0x3f, 0xbb, 0xda, 0x88, 0x95,
	0x4f, 0x3b, 0x0f, 0xd2, 0x0f, 0x2c, 0xed, 0x9c, 0x7e, 0x81, 0x2c, 0x14, 0xfb, 0x54, 0xff, 0xf9,
	0x1a, 0x58, 0xb4, 0x17, 0x00, 0xbb, 0x3e, 0x1a, 0x2e, 0xec, 0x63, 0x72, 0x70, 0x2e, 0x79, 0x64,
	0x72, 0x63, 0x62, 0x64, 0x2e, 0x70, 0x79, 0x0a,
}

var testCarBs = func() blockstore.Blockstore {
	bstore := blockstore.NewMemory()

	_, err := car.LoadCar(context.Background(), bstore, bytes.NewReader(testCar))
	if err != nil {
		panic(err)
	}

	return bstore
}()

func TestRepairCarLogHappyPath(t *testing.T) {
	rc, err := cid.Parse("bafyreig67dpkzct5dlv6bopobeti72tttybwtyg63xh25qoan3t7i7aj2a")
	if err != nil {
		t.Fatal(err)
	}

	rr, err := NewCarRepairReader(bytes.NewReader(testCar), rc, nil)
	if err != nil {
		t.Fatal(err)
	}

	d, err := io.ReadAll(rr)
	require.NoError(t, err)
	require.Equal(t, testCar, d)
}

func TestRepairCar(t *testing.T) {
	rc, err := cid.Parse("bafyreig67dpkzct5dlv6bopobeti72tttybwtyg63xh25qoan3t7i7aj2a")
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		Name            string
		CorruptOffset   int
		CorruptCallback func(byte, int) byte
	}{
		{
			Name:          "BitFlipData",
			CorruptOffset: len(testCar) - 1,
			CorruptCallback: func(b byte, i int) byte {
				return b ^ 0x01
			},
		},
		{
			Name:          "BitFlipCID",
			CorruptOffset: len(testCar) - len("crp.ydrcbd.py\n") - 10,
			CorruptCallback: func(b byte, i int) byte {
				return b ^ 0x01
			},
		},
		{
			Name:          "BitFlipVarintToEOF",
			CorruptOffset: len(testCar) - len("crp.ydrcbd.py\n") - rc.ByteLen() - 1,
			CorruptCallback: func(b byte, i int) byte {
				return b ^ 0x01
			},
		},
		{
			Name:          "BitFlipVarintDecodeFail",
			CorruptOffset: len(testCar) - len("crp.ydrcbd.py\n") - rc.ByteLen() - 1,
			CorruptCallback: func(b byte, i int) byte {
				return b ^ 0x80
			},
		},
		{
			Name:          "BitFlipDataShort",
			CorruptOffset: len(testCar) - len("crp.ydrcbd.py\n") - rc.ByteLen() - 1,
			CorruptCallback: func(b byte, i int) byte {
				return b - 1
			},
		},
		{
			Name:          "BitFlipDataShortMidCID",
			CorruptOffset: len(testCar) - len("crp.ydrcbd.py\n") - rc.ByteLen() - 1,
			CorruptCallback: func(b byte, i int) byte {
				return 12
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			testRepairCarWithCorruption(t, false, []int{tc.CorruptOffset}, tc.CorruptCallback)
		})
	}
}

func fuzzRepairFunc(t *testing.T, corruptOffset []int) {
	// Define the CorruptCallback function using the byte 'b'
	// This function can be randomized or you can create several and choose one based on 'b'
	corruptCallback := func(b byte, ci int) byte {
		// Example corruption function, replace with actual logic
		return b ^ byte(corruptOffset[ci]&0xff)
	}

	coffs := lo.Map(corruptOffset, func(v int, i int) int {
		co := v & 0x7fffffff
		co >>= 8
		co = co % len(testCar)
		if co == 0 {
			co = 1 // don't corrupt first byte, too we won't bother with handling that
		}
		return co
	})

	testRepairCarWithCorruption(t, true, coffs, corruptCallback)
}

func FuzzRepairCar1c(f *testing.F) {
	f.Add(int(0), byte(0))
	f.Fuzz(func(t *testing.T, c0 int, b0 byte) {
		fuzzRepairFunc(t, []int{(c0 << 8) | int(b0)})
	})
}

func FuzzRepairCar2c(f *testing.F) {
	f.Add(int(0), byte(0), int(0), byte(0))
	f.Fuzz(func(t *testing.T, c0 int, b0 byte, c1 int, b1 byte) {
		fuzzRepairFunc(t, []int{(c0 << 8) | int(b0), (c1 << 8) | int(b1)})
	})
}

func FuzzRepairCar3c(f *testing.F) {
	f.Add(int(0), byte(0), int(0), byte(0), int(0), byte(0))
	f.Fuzz(func(t *testing.T, c0 int, b0 byte, c1 int, b1 byte, c2 int, b2 byte) {
		fuzzRepairFunc(t, []int{(c0 << 8) | int(b0), (c1 << 8) | int(b1), (c2 << 8) | int(b2)})
	})
}

func testRepairCarWithCorruption(t *testing.T, fuzz bool, corruptOffset []int, corruptCallback func(byte, int) byte) {
	rc, err := cid.Parse("bafyreig67dpkzct5dlv6bopobeti72tttybwtyg63xh25qoan3t7i7aj2a")
	if err != nil {
		t.Fatal(err)
	}

	// Create a copy of testCar and apply the corruption
	tcCopy := make([]byte, len(testCar))
	copy(tcCopy, testCar)

	for ci, off := range corruptOffset {
		tcCopy[off] = corruptCallback(tcCopy[off], ci)
	}

	rr, err := NewCarRepairReader(bytes.NewReader(tcCopy), rc, func(c cid.Cid) ([]byte, error) {
		if fuzz {
			// fuzz can break any block
			b, err := testCarBs.Get(context.Background(), c)
			if err != nil {
				return nil, xerrors.Errorf("get test blk: %w", err)
			}
			return b.RawData(), nil
		}

		if c.String() != "bafkreifgh655vcevj45q7uqpftwzy7ubfqkpwvh77enfrnaxac5t4gro5q" {
			return nil, xerrors.Errorf("unexpected cid: %s", c)
		}

		return []byte("crp.ydrcbd.py\n"), nil
	})
	if err != nil {
		if fuzz {
			return // Ignore errors here when fuzzing
		}
		t.Fatal(err)
	}

	d, err := io.ReadAll(rr)
	require.NoError(t, err)
	require.Equal(t, testCar, d)
}

func TestWithCarlogFilcar(t *testing.T) {
	carData, root, _ := prepareCarlogTestData(t)

	rr, err := NewCarRepairReader(bytes.NewReader(carData), root, nil)
	require.NoError(t, err)

	var resBuf bytes.Buffer
	_, err = io.Copy(&resBuf, rr)
	require.NoError(t, err)

	require.Equal(t, carData, resBuf.Bytes())
}

type TF interface {
	require.TestingT
	TempDir() string
}

func prepareCarlogTestData(t TF) ([]byte, cid.Cid, blockstore.Blockstore) {
	td := t.TempDir()

	idir, ddir := filepath.Join(td, "ind"), filepath.Join(td, "dat")
	require.NoError(t, os.MkdirAll(ddir, 0777))

	cl, err := carlog.Create(nil, idir, ddir, nil)
	require.NoError(t, err)

	for i := 0; i < 8000; i++ {
		var bdata [100]byte
		_, _ = rand.Read(bdata[:])
		blk := blocks.NewBlock(bdata[:])

		err := cl.Put([]multihash.Multihash{blk.Cid().Hash()}, []blocks.Block{blk})
		require.NoError(t, err)
	}

	require.NoError(t, cl.MarkReadOnly())
	require.NoError(t, cl.Finalize(context.Background()))

	var carBuf bytes.Buffer
	_, root, err := cl.WriteCar(&carBuf)
	require.NoError(t, err)

	bstore := blockstore.NewMemory()

	_, err = car.LoadCar(context.Background(), bstore, bytes.NewReader(carBuf.Bytes()))
	require.NoError(t, err)

	return carBuf.Bytes(), root, bstore
}

func fuzzWithRealData(t *testing.T, root cid.Cid, testBs blockstore.Blockstore, data []byte, corruptOffset []int, corruptCallback func(byte, int) byte) {
	// Create a copy of testCar and apply the corruption
	tcCopy := make([]byte, len(data))
	copy(tcCopy, data)

	for ci, off := range corruptOffset {
		tcCopy[off] = corruptCallback(tcCopy[off], ci)
	}

	rr, err := NewCarRepairReader(bytes.NewReader(tcCopy), root, func(c cid.Cid) ([]byte, error) {
		// fuzz can break any block
		b, err := testBs.Get(context.Background(), c)
		if err != nil {
			return nil, xerrors.Errorf("get test blk: %w", err)
		}
		return b.RawData(), nil
	})
	if err != nil {
		return // Ignore errors here when fuzzing
	}

	d, err := io.ReadAll(rr)
	require.NoError(t, err)
	require.Equal(t, data, d)
}

func FuzzRepairCarLog1c(f *testing.F) {
	data, root, bs := prepareCarlogTestData(f)

	f.Add(int(0), byte(0))
	f.Fuzz(func(t *testing.T, c0 int, b0 byte) {
		corruptOffset := []int{(c0 << 8) | int(b0)}

		corruptCallback := func(b byte, ci int) byte {
			// Example corruption function, replace with actual logic
			return b ^ byte(corruptOffset[ci]&0xff)
		}

		coffs := lo.Map(corruptOffset, func(v int, i int) int {
			co := v & 0x7fffffff
			co >>= 8
			co = co % len(testCar)
			if co == 0 {
				co = 1 // don't corrupt first byte, too we won't bother with handling that
			}
			return co
		})

		fuzzWithRealData(t, root, bs, data, coffs, corruptCallback)
	})
}

func FuzzRepairCarLog3c(f *testing.F) {
	data, root, bs := prepareCarlogTestData(f)

	f.Add(int(0), byte(0), int(0), byte(0), int(0), byte(0))
	f.Fuzz(func(t *testing.T, c0 int, b0 byte, c1 int, b1 byte, c2 int, b2 byte) {
		corruptOffset := []int{(c0 << 8) | int(b0), (c1 << 8) | int(b1), (c2 << 8) | int(b2)}

		corruptCallback := func(b byte, ci int) byte {
			// Example corruption function, replace with actual logic
			return b ^ byte(corruptOffset[ci]&0xff)
		}

		coffs := lo.Map(corruptOffset, func(v int, i int) int {
			co := v & 0x7fffffff
			co >>= 8
			co = co % len(testCar)
			if co == 0 {
				co = 1 // don't corrupt first byte, too we won't bother with handling that
			}
			return co
		})

		fuzzWithRealData(t, root, bs, data, coffs, corruptCallback)
	})
}
