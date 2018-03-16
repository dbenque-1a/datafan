package simple

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dbenque/datafan/pkg/engine"

	"github.com/dbenque/datafan/pkg/store"
)

func TestFuzzy(t *testing.T) {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	syncPeriod := 10 * time.Millisecond

	N := r1.Intn(100)
	fmt.Printf("N=%d\n", N)
	members := make([]*Member, N)
	engines := make([]*engine.Engine, N)

	for i := range members {
		members[i] = NewMember(fmt.Sprintf("M%d", i), store.NewMapStore())
		engines[i] = engine.NewEngine(members[i], syncPeriod)
	}

	edge := 0
	//fuzzy mesh
	for i := range members {
		if i > 0 {
			J := r1.Intn(5)
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

	stop := make(chan struct{})
	for i := range members {
		D := r1.Intn(20) + 10
		for d := 0; d < D; d++ {
			name := engine.Key(names[r1.Intn(len(names))])
			val := fmt.Sprintf("%d", r1.Intn(1000))
			members[i].Write(NewItem(name, val))
		}
		go engines[i].Run(stop)
	}

	time.Sleep(time.Duration(20*N) * syncPeriod)
	//time.Sleep(5 * time.Second)
	close(stop)
	for i := range members {
		for j := range members {
			storeI := members[i].GetStore().(*store.MapStore)
			storeJ := members[j].GetStore().(*store.MapStore)

			di := storeI.Dump()
			dj := storeJ.Dump()

			if di != dj {
				t.Fatalf("Boum:\n%s\n%s\n", di, dj)
			}
		}
	}
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
