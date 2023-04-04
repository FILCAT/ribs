package jbob

import (
	"bytes"
	"io"
	"os"

	"github.com/multiformats/go-multihash"
)

func SaveMHList(filePath string, list []multihash.Multihash) error {
	var buf bytes.Buffer

	for _, c := range list {
		buf.Write(c)
	}

	return os.WriteFile(filePath, buf.Bytes(), 0644)
}

func LoadMHList(filePath string) ([]multihash.Multihash, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	cidList := make([]multihash.Multihash, 0)
	buf := bytes.NewBuffer(data)
	mr := multihash.NewReader(buf)

	for {
		c, err := mr.ReadMultihash()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		cidList = append(cidList, c)
	}

	return cidList, nil
}
