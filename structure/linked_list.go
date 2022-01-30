package structure

type Value struct {
	key  interface{}
	data interface{}
	next *Value
}

func (v *Value) Next() *Value {
	if v.next != nil {
		return v.next
	}
	return nil
}

type List struct {
	size int
	head *Value
	next *Value
}

func NewList() *List {
	return new(List)
}

func (l *List) Push(_key interface{}, _data interface{}) {
	var current Value = Value{key: _key, data: _data}
	current.next = l.head
	l.head = &current
	l.size += 1
}

func (l *List) Pop() interface{} {
	ret := l.head
	l.size -= 1
	if l.next != nil {
		l.head = l.next
		l.next = l.next.next
	} else {
		l.head = nil
		l.next = nil
	}
	return ret.data
}

func (l *List) find(key interface{}) interface{} {
	next := l.head
	for next != nil {
		if next.key == key {
			return next.data
		}
		next = next.next
	}
	return nil
}

func (l *List) Get(key interface{}) interface{} {
	return l.find(key)
}

func (l *List) Remove(key interface{}) {
	next := l.head
	var prev *Value
	for next != nil {
		if next.key == key {
			prev.next = next.next
			l.size -= 1
			break
		}
		prev = next
		next = next.next
	}
}

func (l *List) Head() interface{} {
	return l.head
}
