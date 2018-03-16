package main

import (
	"fmt"
	"sync"
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

	syncPeriod := 20 * time.Millisecond
	checkPeriod := 10 * time.Millisecond

	E1 := engine.NewEngine(M1, syncPeriod)
	E2 := engine.NewEngine(M2, syncPeriod)
	E3 := engine.NewEngine(M3, syncPeriod)

	E1.AddMember(M2)
	E3.AddMember(M2)

	stop := make(chan struct{})
	go E1.Run(stop)
	go E2.Run(stop)
	go E3.Run(stop)

	var wg sync.WaitGroup

	fmt.Println("------------------------------------")
	M1.Write(simple.NewItem("david", "benque"))
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------wait for 1 everywhere---")

	S1.UntilCount(&wg, 1, checkPeriod)
	S2.UntilCount(&wg, 1, checkPeriod)
	S3.UntilCount(&wg, 1, checkPeriod)
	wg.Wait()
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

	M2.Write(simple.NewItem("eric", "mountain"))
	M3.Write(simple.NewItem("cedric", "lamoriniere"))
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------wait for 3 everywhere---")

	S1.UntilCount(&wg, 3, checkPeriod)
	S2.UntilCount(&wg, 3, checkPeriod)
	S3.UntilCount(&wg, 3, checkPeriod)
	wg.Wait()

	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

	M2.Write(simple.NewItem("eric", "super mountain"))
	M1.Remove("david")
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------wait for new valJe peux te debrief Lundi si tu veux.ue and for 2 everywhere---")
	kp := engine.KeyIDPair{Key: "eric", ID: M2.ID()}
	fcheck := func(i engine.Item) bool {
		if i == nil {
			return false
		}
		is := i.(*simple.Item)
		return is.Value == "super mountain"
	}

	S1.UntilCheck(&wg, kp, fcheck, checkPeriod)
	S2.UntilCheck(&wg, kp, fcheck, checkPeriod)
	S3.UntilCheck(&wg, kp, fcheck, checkPeriod)
	S1.UntilCount(&wg, 2, checkPeriod)
	S2.UntilCount(&wg, 2, checkPeriod)
	S3.UntilCount(&wg, 2, checkPeriod)

	wg.Wait()
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())

	fmt.Println("------------wait for cleanup-------")
	M2.Remove("eric")
	M3.Remove("cedric")
	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

	S1.UntilCount(&wg, 0, checkPeriod)
	S2.UntilCount(&wg, 0, checkPeriod)
	S3.UntilCount(&wg, 0, checkPeriod)
	wg.Wait()

	fmt.Printf("S1:\n" + S1.Dump())
	fmt.Printf("S2:\n" + S2.Dump())
	fmt.Printf("S3:\n" + S3.Dump())
	fmt.Println("------------------------------------")

}
