package simple

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dbenque/datafan/pkg/engine"

	"github.com/dbenque/datafan/pkg/store"
)

func BenchmarkAddLine(b *testing.B) {
	syncPeriod := 20 * time.Millisecond
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	N := b.N

	members, engines := prepareTest(N, "line", true, syncPeriod)
	D := 3

	for i := range members {
		for d := 0; d < D; d++ {
			name := engine.Key(names[d])
			val := fmt.Sprintf("%d", r1.Intn(1000))
			members[i].Write(NewItem(name, val))
		}
	}

	b.ResetTimer()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		go engines[i].Run(stop)
		s := members[i].GetStore().(*store.MapStore)
		s.UntilCount(&wg, D*N, syncPeriod)
	}
	wg.Wait()
	close(stop)
}

func prepareTest(N int, meshType string, panicOnDelete bool, syncPeriod time.Duration) ([]*Member, []*engine.Engine) {
	members := make([]*Member, N)
	engines := make([]*engine.Engine, N)

	for i := range members {
		members[i] = NewMember(fmt.Sprintf("M%d", i), store.NewMapStore())
		engines[i] = engine.NewEngine(members[i], syncPeriod)
		if panicOnDelete {
			members[i].GetStore().(*store.MapStore).PanicOnDelete()
		}
	}

	switch meshType {
	case "full":
		for i := range members {
			if i > 0 {

				for j := range members {
					if j == i {
						continue
					}
					engines[i].AddMember(members[j])
				}
			}
		}
	case "line":
		for i := 1; i < len(members); i++ {
			engines[i].AddMember(members[i-1])
		}
	case "line2":
		for i := 2; i < len(members); i++ {
			engines[i].AddMember(members[i-1])
			engines[i].AddMember(members[i-2])
		}

	default: // line
		for i := 1; i < len(members); i++ {
			engines[i].AddMember(members[i-1])
		}
	}
	return members, engines
}

func TestFuzzyAddOnly(t *testing.T) {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	syncPeriod := 10 * time.Millisecond

	N := r1.Intn(50)
	fmt.Printf("N=%d\n", N)
	members := make([]*Member, N)
	engines := make([]*engine.Engine, N)

	for i := range members {
		members[i] = NewMember(fmt.Sprintf("M%d", i), store.NewMapStore())
		engines[i] = engine.NewEngine(members[i], syncPeriod)
		//members[i].GetStore().(*store.MapStore).PanicOnDelete()
	}

	edge := 0
	//fuzzy mesh
	for i := range members {
		if i > 0 {
			J := r1.Intn(4)
			for j := 0; j <= J; j++ {
				x := r1.Intn(i)
				if x == i {
					x--
				}
				fmt.Printf("%s - %s\n", members[i].ID(), members[x].ID())
				engines[i].AddMember(members[x])

				edge++
			}
		}
	}
	fmt.Printf("Edges=%d\n", edge)
	//fuzzy data
	allData := 0
	stop := make(chan struct{})
	for i := range members {
		D := r1.Intn(20) + 10
		allData += D
		for d := 0; d < D; d++ {
			name := engine.Key(names[d])
			val := fmt.Sprintf("%d", r1.Intn(1000))
			members[i].Write(NewItem(name, val))
		}
		go engines[i].Run(stop)
	}

	var wg sync.WaitGroup
	for i := range members {
		members[i].GetStore().(*store.MapStore).UntilCount(&wg, allData, syncPeriod)
	}
	wg.Wait()
	close(stop)

	for i := range members {
		for j := range members {
			storeI := members[i].GetStore().(*store.MapStore)
			storeJ := members[j].GetStore().(*store.MapStore)

			di := storeI.Dump()
			dj := storeJ.Dump()

			if di != dj {

				t.Fatalf("Boum (allData=%d):\n%d in %s\n%d in %s\ntopo: %s\n",
					allData,
					storeI.Count(),
					toTmpFile(t, "fuzzi", []byte(di)),
					storeJ.Count(),
					toTmpFile(t, "fuzzj", []byte(dj)),
					ToDot(members, os.TempDir()+"/members"),
				)

			}
		}
	}
}

func toTmpFile(t *testing.T, name string, content []byte) string {
	tmpfile, err := ioutil.TempFile("", "fuzzy_i")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}
	return tmpfile.Name()
}

var names = []string{
	"Random Names",
	"Gemma Cotton",
	"Louie Jackson",
	"Montana Adam",
	"Mac Mcgee",
	"Gurdeep Galvan",
	"Azra Boyd",
	"Jozef Chaney",
	"Lisa-Marie Andrews",
	"Ebrahim Contreras",
	"Oran Kendall",
	"Cari Cousins",
	"Romana Legge",
	"Taylah Kouma",
	"Bryson Chase",
	"Thelma Barron",
	"Alanah Mansell",
	"Tomas Dennis",
	"Miller Goodwin",
	"Donnie Battle",
	"Kacie Duncan",
	"Sara Maldonado",
	"Deacon Dunlap",
	"Macauley Ahmed",
	"Azeem Santana",
	"Armaan Bull",
	"Olivia-Grace Richards",
	"Sharmin Franklin",
	"Ayoub Zavala",
	"Ronald Morrow",
	"Paloma Fox",
	"Ronan Christian",
	"Izaan Barker",
	"Kye Stone",
	"Owais Lang",
	"Abdi Porter",
	"Neha Connor",
	"Penelope Firth",
	"Farhana Bostock",
	"Bruno Ortega",
	"Braiden Busby",
	"Nabiha Salter",
	"Mairead Walker",
	"Rajan Kay",
	"George Whitmore",
	"Nicky Crossley",
	"Kaylee Bauer",
	"Eshaan Vincent",
	"Nela Weeks",
	"Jayce Pratt",
	"Hilda Lott",
	"Brody Moses",
	"Daisy Connelly",
	"Sanna Rios",
	"Lorena Hull",
	"Poppie Gould",
	"Jacey Donaldson",
	"Hassan Zamora",
	"Anabel Padilla",
	"Bryony Lindsey",
	"Mack Raymond",
	"Alaya Plummer",
	"Vickie Mccormick",
	"Luis Sharp",
	"Phoebe Simpson",
	"Macey Mosley",
	"Nuala Wainwright",
	"Mayur Findlay",
	"Pearl Ireland",
	"Jensen Cope",
	"Lilith Klein",
	"Tiya Spencer",
	"Jarrod Roy",
	"Kush Horton",
	"Kiara Duarte",
	"Tai Floyd",
	"Kai O'Reilly",
	"Khia Fowler",
	"Kayan Stanley",
	"Noa Delacruz",
	"Lorenzo Doyle",
	"Kenny Rich",
	"Fenella Larson",
	"Mahamed Handley",
	"Abby Millar",
	"Ehsan Hubbard",
	"Bronte Holder",
	"Hamish Marquez",
	"Taryn Cline",
	"Aya Bowden",
	"Rima Santiago",
	"Hamzah Le",
	"Patrycja Haley",
	"Darnell Brook",
	"Isabel Whitfield",
	"Jaime Ewing",
	"Marvin Fry",
	"Connah Derrick",
	"Marina Jacobson",
	"Alivia Halliday",
	"Jamie Carr",
}
