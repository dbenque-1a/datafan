package main

import (
	"fmt"
	"time"

	"github.com/dbenque/datafan/pkg/engine"
	"github.com/dbenque/datafan/pkg/simple"
	"github.com/dbenque/datafan/pkg/store"
)

func main() {
	S1 := store.NewMapStore("Store1")
	S2 := store.NewMapStore("Store2")
	S3 := store.NewMapStore("Store3")

	M1 := simple.NewMember("M1", S1)
	M2 := simple.NewMember("M2", S2)
	M3 := simple.NewMember("M3", S3)

	E1 := engine.NewEngine(M1)
	E2 := engine.NewEngine(M2)
	E3 := engine.NewEngine(M3)

	E1.AddMember(M2)
	E3.AddMember(M2)

	stop := make(chan struct{})
	go E1.Run(stop)
	go E2.Run(stop)
	go E3.Run(stop)

	fmt.Println("------------------------------------")

	M1.Write(simple.NewItem("david", "benque"))
	fmt.Printf(S1.Dump())
	fmt.Printf(S2.Dump())
	fmt.Printf(S3.Dump())
	fmt.Println("------------------------------------")

	time.Sleep(1 * time.Second)
	fmt.Printf(S1.Dump())
	fmt.Printf(S2.Dump())
	fmt.Printf(S3.Dump())
	fmt.Println("------------------------------------")

	M2.Write(simple.NewItem("eric", "mountain"))
	M3.Write(simple.NewItem("cedric", "lamoriniere"))
	time.Sleep(1 * time.Second)
	fmt.Printf(S1.Dump())
	fmt.Printf(S2.Dump())
	fmt.Printf(S3.Dump())
	fmt.Println("------------------------------------")

	time.Sleep(5 * time.Second)
	fmt.Printf(S1.Dump())
	fmt.Printf(S2.Dump())
	fmt.Printf(S3.Dump())
	fmt.Println("------------------------------------")

}
