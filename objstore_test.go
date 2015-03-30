package objstore

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

type testStruct struct {
	Id    Id
	Name  string
	Value int
}

func (t *testStruct) GetId() Id {
	return t.Id
}

func (t *testStruct) SetId(id Id) {
	t.Id = id
}

func TestCRUD(t *testing.T) {
	obj := &testStruct{
		Name:  "test name",
		Value: 42,
	}

	file, err := ioutil.TempFile("", "objstoretest")
	if err != nil {
		t.Error(err)
		return
	}

	store, err := OpenStore(file.Name())
	if err != nil {
		t.Error(err)
		return
	}

	err = store.Create(obj)
	if err != nil {
		t.Error(err)
		return
	}

	var obj2 testStruct

	err = store.Read(obj.GetId(), &obj2)
	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(obj, &obj2) {
		t.Errorf("loaded config does not match stored config\n%q\n%q",
			obj, &obj2)
		return
	}

	err = store.Delete(&obj2)
	if err != nil {
		t.Error(err)
		return
	}

	err = store.Read(obj.GetId(), &obj2)
	if err == nil {
		t.Errorf("reading a deleted object did not return an error")
		return
	}

	defer func() {
		store.Close()
		os.Remove(file.Name())
	}()

}
