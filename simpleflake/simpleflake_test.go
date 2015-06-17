package simpleflake

import (
	"testing"
	"fmt"
)

func TestIdCreation(t *testing.T){
	id1 := NewId()

	id2 := NewId()

	if id1.Id != id2.Id{
		fmt.Printf("Subsequent generated IDs are different")
	}else{
		t.Error("Failed asserting", id1.Id, "is different to", id2.Id)
	}

}
