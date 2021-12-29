package cacheleveldown

import (
	"os"
	"path"
	"sync"
	"context"

	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/cache"
	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var InMemoryDirectory = ":memory:"

type levelDownCache struct {
	ctx context.Context
	muCaches sync.Mutex
	caches   map[string]*wrappedCache
	logger   *zap.Logger
}

type wrappedCache struct {
	ctx context.Context
	wrappedCache datastore.Datastore
	manager      *levelDownCache
	id           string
	closed       bool
}

func (w *wrappedCache) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	return w.wrappedCache.Get(w.ctx, key)
}

func (w *wrappedCache) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	return w.wrappedCache.Has(w.ctx,key)
}

func (w *wrappedCache) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	return w.wrappedCache.GetSize(w.ctx,key)
}

func (w *wrappedCache) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return w.wrappedCache.Query(w.ctx,q)
}

func (w *wrappedCache) Put(ctx context.Context, key datastore.Key, value []byte) error {
	return w.wrappedCache.Put(w.ctx, key, value)
}

func (w *wrappedCache) Delete(	ctx context.Context, key datastore.Key) error {
	return w.wrappedCache.Delete(w.ctx, key)
}

func (w *wrappedCache) Sync(ctx context.Context, key datastore.Key) error {
	return w.wrappedCache.Sync(w.ctx, key)
}

func (w *wrappedCache) Close() error {
	if w.closed {
		return nil
	}

	w.manager.muCaches.Lock()

	w.closed = true
	err := w.wrappedCache.Close()
	delete(w.manager.caches, w.id)

	w.manager.muCaches.Unlock()

	return err
}

func (l *levelDownCache) Load(directory string, dbAddress address.Address) (ds datastore.Datastore, err error) {
	keyPath := datastoreKey(directory, dbAddress)

	l.muCaches.Lock()
	defer l.muCaches.Unlock()

	var ok bool
	var wc * wrappedCache
	
	if wc, ok = l.caches[keyPath]; ok {
		ds= wc.wrappedCache
		return
	}
	
	//ds = wc.wrappedCache
	
//	if ds, ok = l.caches[keyPath]; ok {
//		return
//	}

	l.logger.Debug("opening cache db", zap.String("path", keyPath))

	if directory == InMemoryDirectory {
		ds, err = leveldb.NewDatastore("", nil)
	} else {
		ds, err = leveldb.NewDatastore(keyPath, nil)
	}

	if err != nil {
		err = errors.Wrap(err, "unable to init leveldb datastore")
		return
	}

	l.caches[keyPath] = &wrappedCache{ctx:l.ctx, wrappedCache: ds, id: keyPath, manager: l}
	return
}

func (l *levelDownCache) Close() error {
	for _, c := range l.caches {
		_ = c.Close()
	}

	return nil
}

func datastoreKey(directory string, dbAddress address.Address) string {
	dbPath := path.Join(dbAddress.GetRoot().String(), dbAddress.GetPath())
	keyPath := path.Join(directory, dbPath)

	return keyPath
}

func (l *levelDownCache) Destroy(directory string, dbAddress address.Address) error {
	keyPath := datastoreKey(directory, dbAddress)
	l.muCaches.Lock()
	defer l.muCaches.Unlock()

	if wc, ok := l.caches[keyPath]; ok {
		wc.Close()
	}

	if directory != InMemoryDirectory {
		if err := os.RemoveAll(keyPath); err != nil {
			return errors.Wrap(err, "unable to delete datastore")
		}
	}

	return nil
}

// New Creates a new leveldb data store
func New(ctx context.Context, opts *cache.Options) cache.Interface {
	if opts == nil {
		opts = &cache.Options{}
	}

	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	return &levelDownCache{
		ctx: ctx,
		caches: map[string]*wrappedCache{},
		logger: logger,
	}
}

var _ cache.Interface = &levelDownCache{}
var _ datastore.Datastore = &wrappedCache{}
