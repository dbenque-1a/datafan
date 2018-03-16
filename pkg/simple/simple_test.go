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

var s1 = rand.NewSource(time.Now().UnixNano())
var r1 = rand.New(s1)
var NN = 15
var DD = 5

var syncPeriod = 20 * time.Millisecond
var checkPeriod = 20 * time.Millisecond

func BenchmarkAddLine(b *testing.B) {
	N := b.N
	D := 3

	members, engines := prepareTest(N, D, "line", true, syncPeriod)

	b.ResetTimer()
	stop := make(chan struct{})
	runEngines(stop, engines)
	waitForCount(D*N, members, checkPeriod)
	close(stop)
}

func runEngines(stop chan struct{}, engines []*engine.Engine) {
	for _, e := range engines {
		go e.Run(stop)
	}

}
func waitForCount(count int, members []*Member, checkPeriod time.Duration) {
	var wg sync.WaitGroup
	for i := 0; i < len(members); i++ {
		s := members[i].GetStore().(*store.MapStore)
		s.UntilCount(&wg, count, checkPeriod)
	}
	wg.Wait()
}
func waitForCheck(members []*Member, checkPeriod time.Duration, kp engine.KeyIDPair, check func(i engine.Item) bool) {
	var wg sync.WaitGroup
	for i := 0; i < len(members); i++ {
		s := members[i].GetStore().(*store.MapStore)
		s.UntilCheck(&wg, kp, check, checkPeriod)
	}
	wg.Wait()
}

func prepareTest(N int, D int, meshType string, panicOnDelete bool, syncPeriod time.Duration) ([]*Member, []*engine.Engine) {
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
		engines[0].AddMember(members[1])
	case "circle":
		for i := 1; i < len(members); i++ {
			engines[i].AddMember(members[i-1])
		}
		engines[0].AddMember(members[len(members)-1])
	case "circle2":
		for i := 2; i < len(members); i++ {
			engines[i].AddMember(members[i-1])
			engines[i].AddMember(members[i-2])
		}
		engines[1].AddMember(members[len(members)-1])
		engines[0].AddMember(members[len(members)-1])
		engines[0].AddMember(members[len(members)-2])
	case "random3":
		for i := range members {
			if i > 0 {
				J := r1.Intn(2)
				for j := 0; j <= J; j++ {
					x := r1.Intn(i)
					if x == i {
						x--
					}
					engines[i].AddMember(members[x])
				}
			}
		}
	case "random4":
		for i := range members {
			if i > 0 {
				J := r1.Intn(3)
				for j := 0; j <= J; j++ {
					x := r1.Intn(i)
					if x == i {
						x--
					}
					engines[i].AddMember(members[x])
				}
			}
		}
	default: // line
		for i := 1; i < len(members); i++ {
			engines[i].AddMember(members[i-1])
		}
	}

	for i := range members {
		for d := 0; d < D; d++ {
			name := engine.Key(names[d])
			val := fmt.Sprintf("%d", r1.Intn(1000))
			members[i].Write(NewItem(name, val))
		}
	}

	return members, engines
}

func addOnlySequence(t *testing.T, members []*Member, engines []*engine.Engine) {
	stop := make(chan struct{})
	runEngines(stop, engines)
	waitForCount(DD*NN, members, checkPeriod)
	validateSameStore(t, members)
	members[0].Write(NewItem("David", "Benque"))
	waitForCount(DD*NN+1, members, checkPeriod)
	validateSameStore(t, members)
	members[0].Write(NewItem("David", "dbenque"))
	waitForCheck(members, checkPeriod, engine.KeyIDPair{Key: "David", ID: members[0].id},
		func(i engine.Item) bool {
			if i == nil {
				return false
			}
			is := i.(*Item)
			return is.Value == "dbenque"
		})
	validateSameStore(t, members)
	close(stop)
}

func allSequence(t *testing.T, members []*Member, engines []*engine.Engine) {
	stop := make(chan struct{})
	runEngines(stop, engines)
	waitForCount(DD*NN, members, checkPeriod)
	validateSameStore(t, members)
	members[0].Write(NewItem("David", "Benque"))
	waitForCount(DD*NN+1, members, checkPeriod)
	validateSameStore(t, members)
	members[0].Write(NewItem("David", "dbenque"))
	waitForCheck(members, checkPeriod, engine.KeyIDPair{Key: "David", ID: members[0].id},
		func(i engine.Item) bool {
			if i == nil {
				return false
			}
			is := i.(*Item)
			return is.Value == "dbenque"
		})
	validateSameStore(t, members)
	members[0].Remove("David")
	waitForCount(DD*NN, members, checkPeriod)
	validateSameStore(t, members)
	close(stop)
}

func TestEngineUsingSimplePkg(t *testing.T) {
	tests := []struct {
		name          string
		topo          string
		nbMember      int
		nbData        int
		panicOnDelete bool
		syncPeriod    time.Duration
		scenario      func(*testing.T, []*Member, []*engine.Engine)
		dot           bool
	}{
		{
			name:          "line_All",
			topo:          "line",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: false,
			syncPeriod:    syncPeriod,
			scenario:      allSequence,
		},
		{
			name:          "line_AddOnly",
			topo:          "line",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: true,
			syncPeriod:    syncPeriod,
			scenario:      addOnlySequence,
		},
		{
			name:          "line2_All",
			topo:          "line2",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: false,
			syncPeriod:    syncPeriod,
			scenario:      allSequence,
		},
		{
			name:          "line2_AddOnly",
			topo:          "line2",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: true,
			syncPeriod:    syncPeriod,
			scenario:      addOnlySequence,
		},
		{
			name:          "circle_All",
			topo:          "circle",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: false,
			syncPeriod:    syncPeriod,
			scenario:      allSequence,
		},
		{
			name:          "circle_AddOnly",
			topo:          "circle",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: true,
			syncPeriod:    syncPeriod,
			scenario:      addOnlySequence,
		},
		{
			name:          "circle2_All",
			topo:          "circle2",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: false,
			syncPeriod:    syncPeriod,
			scenario:      allSequence,
		},
		{
			name:          "circle2_AddOnly",
			topo:          "circle2",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: true,
			syncPeriod:    syncPeriod,
			scenario:      addOnlySequence,
		},
		{
			name:          "full_All",
			topo:          "full",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: false,
			syncPeriod:    syncPeriod,
			scenario:      allSequence,
		},
		{
			name:          "full_AddOnly",
			topo:          "full",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: true,
			syncPeriod:    syncPeriod,
			scenario:      addOnlySequence,
		},
		{
			name:          "random3_All",
			topo:          "random3",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: false,
			syncPeriod:    syncPeriod,
			scenario:      allSequence,
		},
		{
			name:          "random3_AddOnly",
			topo:          "random3",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: true,
			syncPeriod:    syncPeriod,
			scenario:      addOnlySequence,
		},
		{
			name:          "random4_All",
			topo:          "random4",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: false,
			syncPeriod:    syncPeriod,
			scenario:      allSequence,
		},
		{
			name:          "random4_AddOnly",
			topo:          "random4",
			nbMember:      NN,
			nbData:        DD,
			panicOnDelete: true,
			syncPeriod:    syncPeriod,
			scenario:      addOnlySequence,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members, engines := prepareTest(tt.nbMember, tt.nbData, tt.topo, tt.panicOnDelete, tt.syncPeriod)
			tt.scenario(t, members, engines)
			if tt.dot {
				fmt.Println(ToDot(members, os.TempDir()+tt.name))
			}
		})
	}
}

func TestEngine(t *testing.T) {

}

func TestFuzzyAddOnly(t *testing.T) {
	N := r1.Intn(30) + 10
	members := make([]*Member, N)
	engines := make([]*engine.Engine, N)

	for i := range members {
		members[i] = NewMember(fmt.Sprintf("M%d", i), store.NewMapStore())
		engines[i] = engine.NewEngine(members[i], syncPeriod)
		members[i].GetStore().(*store.MapStore).PanicOnDelete()
	}

	//fuzzy mesh
	for i := range members {
		if i > 0 {
			J := r1.Intn(4)
			for j := 0; j <= J; j++ {
				x := r1.Intn(i)
				if x == i {
					x--
				}
				engines[i].AddMember(members[x])
			}
		}
	}
	//fuzzy data
	allData := 0
	stop := make(chan struct{})
	for i := range members {
		D := r1.Intn(20) + 1
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

	validateSameStore(t, members)
}

func validateSameStore(t *testing.T, members []*Member) (ok bool) {
	for i := range members {
		for j := range members {
			storeI := members[i].GetStore().(*store.MapStore)
			storeJ := members[j].GetStore().(*store.MapStore)

			di := storeI.Dump()
			dj := storeJ.Dump()

			if di != dj {
				t.Fatalf("Boum:\n%d in %s\n%d in %s\ntopo: %s\n",
					storeI.Count(),
					toTmpFile(t, "fuzzi", []byte(di)),
					storeJ.Count(),
					toTmpFile(t, "fuzzj", []byte(dj)),
					ToDot(members, os.TempDir()+"/members"))
				return false
			}
		}
	}
	return true
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
	"Random Names", "Gemma Cotton", "Louie Jackson",
	"Montana Adam", "Mac Mcgee", "Gurdeep Galvan",
	"Azra Boyd", "Jozef Chaney", "Lisa-Marie Andrews",
	"Ebrahim Contreras", "Oran Kendall", "Cari Cousins",
	"Romana Legge", "Taylah Kouma", "Bryson Chase",
	"Thelma Barron", "Alanah Mansell", "Tomas Dennis",
	"Miller Goodwin", "Donnie Battle", "Kacie Duncan",
	"Sara Maldonado", "Deacon Dunlap", "Macauley Ahmed",
	"Azeem Santana", "Armaan Bull", "Olivia-Grace Richards",
	"Sharmin Franklin", "Ayoub Zavala", "Ronald Morrow",
	"Paloma Fox", "Ronan Christian", "Izaan Barker",
	"Kye Stone", "Owais Lang", "Abdi Porter",
	"Neha Connor", "Penelope Firth", "Farhana Bostock",
	"Bruno Ortega", "Braiden Busby", "Nabiha Salter",
	"Mairead Walker", "Rajan Kay", "George Whitmore",
	"Nicky Crossley", "Kaylee Bauer", "Eshaan Vincent",
	"Nela Weeks", "Jayce Pratt", "Hilda Lott",
	"Brody Moses", "Daisy Connelly", "Sanna Rios",
	"Lorena Hull", "Poppie Gould", "Jacey Donaldson",
	"Hassan Zamora", "Anabel Padilla", "Bryony Lindsey",
	"Mack Raymond", "Alaya Plummer", "Vickie Mccormick",
	"Luis Sharp", "Phoebe Simpson", "Macey Mosley",
	"Nuala Wainwright", "Mayur Findlay", "Pearl Ireland",
	"Jensen Cope", "Lilith Klein", "Tiya Spencer",
	"Jarrod Roy", "Kush Horton", "Kiara Duarte",
	"Tai Floyd", "Kai O'Reilly", "Khia Fowler",
	"Kayan Stanley", "Noa Delacruz", "Lorenzo Doyle",
	"Kenny Rich", "Fenella Larson", "Mahamed Handley",
	"Abby Millar", "Ehsan Hubbard", "Bronte Holder",
	"Hamish Marquez", "Taryn Cline", "Aya Bowden",
	"Rima Santiago", "Hamzah Le", "Patrycja Haley",
	"Darnell Brook", "Isabel Whitfield", "Jaime Ewing",
	"Marvin Fry", "Connah Derrick", "Marina Jacobson",
	"Alivia Halliday", "Jamie Carr"}
