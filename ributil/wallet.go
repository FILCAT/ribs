package ributil

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/filecoin-project/lotus/lib/sigs"
	_ "github.com/filecoin-project/lotus/lib/sigs/secp" // enable secp signatures
	"github.com/mitchellh/go-homedir"
	"github.com/whyrusleeping/base32"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	KNamePrefix  = "wallet-"
	KTrashPrefix = "trash-"
	KDefault     = "default"
)

func OpenWallet(path string) (*LocalWallet, error) {
	path, err := homedir.Expand(path)
	if err != nil {
		return nil, xerrors.Errorf("expand wallet path: %w", err)
	}

	kstore, err := OpenOrInitKeystore(path)
	if err != nil {
		return nil, err
	}

	return NewWallet(kstore)
}

type LocalWallet struct {
	keys     map[address.Address]*key.Key
	keystore types.KeyStore

	lk sync.Mutex
}

func NewWallet(keystore types.KeyStore) (*LocalWallet, error) {
	w := &LocalWallet{
		keys:     make(map[address.Address]*key.Key),
		keystore: keystore,
	}

	return w, nil
}

// below is same as in lotus; We copy to not depend on internal logic.
// Also we don't want bls sigs, because those depend on ffi

func (w *LocalWallet) WalletSign(ctx context.Context, addr address.Address, msg []byte, meta api.MsgMeta) (*crypto.Signature, error) {
	ki, err := w.findKey(addr)
	if err != nil {
		return nil, err
	}
	if ki == nil {
		return nil, xerrors.Errorf("signing using key '%s': %w", addr.String(), types.ErrKeyInfoNotFound)
	}

	return sigs.Sign(key.ActSigType(ki.Type), ki.PrivateKey, msg)
}

func (w *LocalWallet) WalletList(ctx context.Context) ([]address.Address, error) {
	all, err := w.keystore.List()
	if err != nil {
		return nil, xerrors.Errorf("listing keystore: %w", err)
	}

	sort.Strings(all)

	seen := map[address.Address]struct{}{}
	out := make([]address.Address, 0, len(all))
	for _, a := range all {
		if strings.HasPrefix(a, KNamePrefix) {
			name := strings.TrimPrefix(a, KNamePrefix)
			addr, err := address.NewFromString(name)
			if err != nil {
				return nil, xerrors.Errorf("converting name to address: %w", err)
			}
			if _, ok := seen[addr]; ok {
				continue // got duplicate with a different prefix
			}
			seen[addr] = struct{}{}

			out = append(out, addr)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})

	return out, nil
}

func (w *LocalWallet) GetDefault() (address.Address, error) {
	w.lk.Lock()
	defer w.lk.Unlock()

	ki, err := w.keystore.Get(KDefault)
	if err != nil {
		return address.Undef, xerrors.Errorf("failed to get default key: %w", err)
	}

	k, err := key.NewKey(ki)
	if err != nil {
		return address.Undef, xerrors.Errorf("failed to read default key from keystore: %w", err)
	}

	return k.Address, nil
}

func (w *LocalWallet) SetDefault(a address.Address) error {
	w.lk.Lock()
	defer w.lk.Unlock()

	ki, err := w.keystore.Get(KNamePrefix + a.String())
	if err != nil {
		return err
	}

	if err := w.keystore.Delete(KDefault); err != nil {
		if !xerrors.Is(err, types.ErrKeyInfoNotFound) {
			fmt.Println("failed to delete old default key: ", err)
		}
	}

	if err := w.keystore.Put(KDefault, ki); err != nil {
		return err
	}

	return nil
}

func (w *LocalWallet) WalletNew(ctx context.Context, typ types.KeyType) (address.Address, error) {
	w.lk.Lock()
	defer w.lk.Unlock()

	k, err := key.GenerateKey(typ)
	if err != nil {
		return address.Undef, err
	}

	if err := w.keystore.Put(KNamePrefix+k.Address.String(), k.KeyInfo); err != nil {
		return address.Undef, xerrors.Errorf("saving to keystore: %w", err)
	}
	w.keys[k.Address] = k

	_, err = w.keystore.Get(KDefault)
	if err != nil {
		if !xerrors.Is(err, types.ErrKeyInfoNotFound) {
			return address.Undef, err
		}

		if err := w.keystore.Put(KDefault, k.KeyInfo); err != nil {
			return address.Undef, xerrors.Errorf("failed to set new key as default: %w", err)
		}
	}

	return k.Address, nil
}

func (w *LocalWallet) findKey(addr address.Address) (*key.Key, error) {
	w.lk.Lock()
	defer w.lk.Unlock()

	k, ok := w.keys[addr]
	if ok {
		return k, nil
	}
	if w.keystore == nil {
		return nil, nil
	}

	ki, err := w.tryFind(addr)
	if err != nil {
		if xerrors.Is(err, types.ErrKeyInfoNotFound) {
			return nil, nil
		}
		return nil, xerrors.Errorf("getting from keystore: %w", err)
	}
	k, err = key.NewKey(ki)
	if err != nil {
		return nil, xerrors.Errorf("decoding from keystore: %w", err)
	}
	w.keys[k.Address] = k
	return k, nil
}

func (w *LocalWallet) tryFind(addr address.Address) (types.KeyInfo, error) {

	ki, err := w.keystore.Get(KNamePrefix + addr.String())
	if err == nil {
		return ki, err
	}

	if !xerrors.Is(err, types.ErrKeyInfoNotFound) {
		return types.KeyInfo{}, err
	}

	// We got an ErrKeyInfoNotFound error
	// Try again, this time with the testnet prefix

	tAddress, err := swapMainnetForTestnetPrefix(addr.String())
	if err != nil {
		return types.KeyInfo{}, err
	}

	ki, err = w.keystore.Get(KNamePrefix + tAddress)
	if err != nil {
		return types.KeyInfo{}, err
	}

	// We found it with the testnet prefix
	// Add this KeyInfo with the mainnet prefix address string
	err = w.keystore.Put(KNamePrefix+addr.String(), ki)
	if err != nil {
		return types.KeyInfo{}, err
	}

	return ki, nil
}

func swapMainnetForTestnetPrefix(addr string) (string, error) {
	aChars := []rune(addr)
	prefixRunes := []rune(address.TestnetPrefix)
	if len(prefixRunes) != 1 {
		return "", xerrors.Errorf("unexpected prefix length: %d", len(prefixRunes))
	}

	aChars[0] = prefixRunes[0]
	return string(aChars), nil
}

////

type DiskKeyStore struct {
	path string
}

func OpenOrInitKeystore(p string) (*DiskKeyStore, error) {
	if _, err := os.Stat(p); err == nil {
		return &DiskKeyStore{p}, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	if err := os.Mkdir(p, 0700); err != nil {
		return nil, err
	}

	return &DiskKeyStore{p}, nil
}

var kstrPermissionMsg = "permissions of key: '%s' are too relaxed, " +
	"required: 0600, got: %#o"

// List lists all the keys stored in the KeyStore
func (fsr *DiskKeyStore) List() ([]string, error) {

	dir, err := os.Open(fsr.path)
	if err != nil {
		return nil, fmt.Errorf("opening dir to list keystore: %w", err)
	}
	defer dir.Close() //nolint:errcheck
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, fmt.Errorf("reading keystore dir: %w", err)
	}
	keys := make([]string, 0, len(files))
	for _, f := range files {
		if f.Mode()&0077 != 0 {
			return nil, fmt.Errorf(kstrPermissionMsg, f.Name(), f.Mode())
		}
		name, err := base32.RawStdEncoding.DecodeString(f.Name())
		if err != nil {
			return nil, fmt.Errorf("decoding key: '%s': %w", f.Name(), err)
		}
		keys = append(keys, string(name))
	}
	return keys, nil
}

// Get gets a key out of keystore and returns types.KeyInfo coresponding to named key
func (fsr *DiskKeyStore) Get(name string) (types.KeyInfo, error) {

	encName := base32.RawStdEncoding.EncodeToString([]byte(name))
	keyPath := filepath.Join(fsr.path, encName)

	fstat, err := os.Stat(keyPath)
	if os.IsNotExist(err) {
		return types.KeyInfo{}, fmt.Errorf("opening key '%s': %w", name, types.ErrKeyInfoNotFound)
	} else if err != nil {
		return types.KeyInfo{}, fmt.Errorf("opening key '%s': %w", name, err)
	}

	if fstat.Mode()&0077 != 0 {
		return types.KeyInfo{}, fmt.Errorf(kstrPermissionMsg, name, fstat.Mode())
	}

	file, err := os.Open(keyPath)
	if err != nil {
		return types.KeyInfo{}, fmt.Errorf("opening key '%s': %w", name, err)
	}
	defer file.Close() //nolint: errcheck // read only op

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return types.KeyInfo{}, fmt.Errorf("reading key '%s': %w", name, err)
	}

	var res types.KeyInfo
	err = json.Unmarshal(data, &res)
	if err != nil {
		return types.KeyInfo{}, fmt.Errorf("decoding key '%s': %w", name, err)
	}

	return res, nil
}

// Put saves key info under given name
func (fsr *DiskKeyStore) Put(name string, info types.KeyInfo) error {

	encName := base32.RawStdEncoding.EncodeToString([]byte(name))
	keyPath := filepath.Join(fsr.path, encName)

	_, err := os.Stat(keyPath)
	if err == nil {
		return fmt.Errorf("checking key before put '%s': %w", name, types.ErrKeyExists)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("checking key before put '%s': %w", name, err)
	}

	keyData, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("encoding key '%s': %w", name, err)
	}

	err = ioutil.WriteFile(keyPath, keyData, 0600)
	if err != nil {
		return fmt.Errorf("writing key '%s': %w", name, err)
	}
	return nil
}

func (fsr *DiskKeyStore) Delete(name string) error {

	encName := base32.RawStdEncoding.EncodeToString([]byte(name))
	keyPath := filepath.Join(fsr.path, encName)

	_, err := os.Stat(keyPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("checking key before delete '%s': %w", name, types.ErrKeyInfoNotFound)
	} else if err != nil {
		return fmt.Errorf("checking key before delete '%s': %w", name, err)
	}

	err = os.Remove(keyPath)
	if err != nil {
		return fmt.Errorf("deleting key '%s': %w", name, err)
	}
	return nil
}
