package structure

import (
	"fmt"
	"testing"
)

func TestPush(t *testing.T) {
	l := NewList()
	arr := []int{1, 2, 3, 4, 5}
	for i := 0; i < len(arr); i++ {
		l.Push(arr[i], arr[i])
	}

	next := l.head
	j := len(arr) - 1
	for i := 0; i < len(arr); i++ {
		if arr[j] != next.key.(int) {
			t.Errorf("Exepected key to be %d but found %d", arr[i], next.key)
		}
		next = next.Next()
		j -= 1
	}
}

func TestPop(t *testing.T) {
	l := NewList()
	arr := []int{1, 2, 3, 4, 5}
	for i := 0; i < len(arr); i++ {
		l.Push(arr[i], arr[i])
	}

	elem := l.Pop()
	fmt.Printf("elem: %v arr[1]: %d\n", elem, arr[2])
	if elem.(int) != arr[len(arr)-1] {
		t.Errorf("Pop should get the first element")
	}
}

func TestGet(t *testing.T) {
	l := NewList()
	arr := []int{1, 2, 3, 4, 5}
	for i := 0; i < len(arr); i++ {
		l.Push(arr[i], arr[i])
	}

	key := 3
	elem := l.Get(key)
	if elem != key {
		t.Errorf("The specified key should be present in the list")
	}
}