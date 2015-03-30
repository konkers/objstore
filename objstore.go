package objstore

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/satori/go.uuid"
)

type Id uuid.UUID

type Storable interface {
	GetId() Id
	SetId(id Id)
}

type Store struct {
	db *bolt.DB
}

var OBJECT_BUCKET = []byte("objects")

func OpenStore(path string) (*Store, error) {
	boltDb, err := bolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}

	store := &Store{
		db: boltDb,
	}

	return store, nil
}

func (store *Store) Close() {
	store.db.Close()
}

func (store *Store) Create(obj Storable) error {
	// should we assert that ID is not set?
	obj.SetId(Id(uuid.NewV4()))

	// TODO: assert that id does not exist in database

	return store.Update(obj)
}

func (store *Store) Update(obj Storable) error {
	key := uuid.UUID(obj.GetId()).Bytes()
	if key == nil {
		return fmt.Errorf("can't update object with nil id")
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	// If we wanted a bucket per struct type, we could:
	// t := reflect.TypeOf(obj).Elem()
	// bucket := []byte(t.Name())

	err = store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(OBJECT_BUCKET)
		if err != nil {
			return err
		}

		err = bucket.Put(key, data)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (store *Store) Read(id Id, obj Storable) error {
	key := uuid.UUID(id).Bytes()
	if key == nil {
		return fmt.Errorf("can't update object with nil id")
	}

	var data []byte

	err := store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(OBJECT_BUCKET)
		if err != nil {
			return err
		}

		data = bucket.Get(key)
		if data == nil {
			return errors.New("object does not exist")
		}
		return nil
	})

	if err != nil {
		return err
	}

	return json.Unmarshal(data, obj)
}

func (store *Store) Delete(obj Storable) error {
	key := uuid.UUID(obj.GetId()).Bytes()
	if key == nil {
		return fmt.Errorf("can't update object with nil id")
	}

	err := store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(OBJECT_BUCKET)
		if err != nil {
			return err
		}

		err = bucket.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}
