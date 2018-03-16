package main

import (
	"fmt"
	"time"

	"github.com/dbenque/datafan/pkg/engine"
	"github.com/dbenque/datafan/pkg/simple"
	"github.com/dbenque/datafan/pkg/store"
)

func main() {
	S1 := store.NewMapStore()
	S2 := store.NewMapStore()
	S3 := store.NewMapStore()

	M1 := simple.NewMember("M1", S1)
	M2 := simple.NewMember("M2", S2)
	M3 := simple.NewMember("M3", S3)

	E1 := engine.NewEngine(M1, time.Second)
	E2 := engine.NewEngine(M2, time.Second)
	E3 := engine.NewEngine(M3, time.Second)

	E1.AddMember(M2)
	E3.AddMember(M2)

	stop := make(chan struct{})
	go E1.Run(stop)
	go E2.Run(stop)
	go E3.Run(stop)

	fmt.Println("------------------------------------")
	M1.Write(simple.NewItem("david", "benque"))
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

	time.Sleep(1 * time.Second)
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

	M2.Write(simple.NewItem("eric", "mountain"))
	M3.Write(simple.NewItem("cedric", "lamoriniere"))
	time.Sleep(1 * time.Second)
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

	time.Sleep(1 * time.Second)
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

	M2.Write(simple.NewItem("eric", "super mountain"))
	M1.Remove("david")
	time.Sleep(2 * time.Second)
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

}
