package simple

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"

	"github.com/dbenque/datafan/pkg/store"
)

type edge struct {
	P1 *Member
	P2 *Member
}

func NewEdge(p1, p2 *Member) edge {
	if strings.Compare(string(p1.id), string(p2.id)) > 0 {
		p1, p2 = p2, p1
	}
	e := edge{P1: p1, P2: p2}
	return e
}

func (e *edge) Key() string {
	return fmt.Sprintf("%v-%v", e.P1.id, e.P2.id)
}

func ToDot(members []*Member, filepath string) string {

	links := map[string]edge{}
	for _, p := range members {
		for _, n := range p.connector.remoteMember {
			e := NewEdge(p, n)
			k := e.Key()
			if _, ok := links[k]; !ok {
				links[k] = e
			}
		}
	}

	str := "Graph G {\nrankdir=LR;\n"

	for _, p := range members {

		store := p.GetStore().(*store.MapStore)
		if store != nil {
			str += fmt.Sprintf("\"%v\" [label=\"%v: %d\"]\n", p.id, p.id, store.Count())
		} else {
			str += fmt.Sprintf("\"%v\"\n", p.id)
		}
	}

	for _, e := range links {
		str += fmt.Sprintf("\"%v\" -- \"%v\"\n", e.P1.id, e.P2.id)
	}
	str += "}\n"

	if filepath != "" {
		if err := ioutil.WriteFile(filepath+".dot", []byte(str), 0644); err != nil {
			fmt.Printf("%s", err)
			return str
		}
		if png, e := exec.Command("dot", "-Tpng", filepath+".dot").Output(); e != nil {
			fmt.Printf("%v\n", e)
		} else {
			if err := ioutil.WriteFile(filepath+".png", png, 0644); err != nil {
				fmt.Printf("%s", err)
				return str
			}
			return filepath + ".png"
		}
	}

	return str
}
