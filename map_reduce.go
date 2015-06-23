package main

import ()

type ReduceFuncIterator struct{
	previousValue interface{}
	index uint64
	handler ReduceFunc
}

type ReduceFunc interface{
	Reduce(previous interface{}, current interface{}, index interface{}) interface{}
	Result(previous interface{}) interface{}
}


func (self *ReduceFuncIterator) ReduceNext(item interface{}){

	self.previousValue = self.handler.Reduce(self.previousValue, item, self.index)

	self.index++
}

func (self *ReduceFuncIterator) Result() interface{}{
	return self.handler.Result(self.previousValue)
}

func NewReduceHandler(reduce ReduceFunc) ReduceFuncIterator{
	return ReduceFuncIterator{
		handler :reduce,
	}
}



//
// Counts a stream of items
//
type ReduceCount struct{}

func (self *ReduceCount) Reduce(previous interface{}, current interface{}, index interface{}) interface{}{

	if previous == nil{
		previous = 0
	}

	return previous.(int) + 1;
}

func (self *ReduceCount) Result(previous interface{}) interface{}{
	return previous.(int)
}

//
// Counts the distinct items in a stream
//
type ReduceCountDistinct struct{}

type distinctMap map[interface{}]bool

func (self ReduceCountDistinct) Reduce(previous interface{}, current interface{}, index interface{}) interface{}{

	if previous == nil{
		previous = make(distinctMap)
	}

	previous.(distinctMap)[current] = true

	return previous
}

func (self ReduceCountDistinct) Result(previous interface{}) interface{}{
	return len(previous.(distinctMap))
}


//
// Counts the distinct items in a stream
//
type ReduceMeanAvg struct{}

type meanAvgHolder struct {
	count uint64
	value float64
}

func (self ReduceMeanAvg) Reduce(previous interface{}, current interface{}, index interface{}) interface{}{

	if previous == nil {
		previous = meanAvgHolder{}
	}

	m := previous.(meanAvgHolder)

	m.value += current.(float64)
	m.count++

	return m
}

func (self ReduceMeanAvg) Result(previous interface{}) interface{}{

	t := previous.(meanAvgHolder)

	return t.value / float64(t.count)
}
