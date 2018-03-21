package utils

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
)

type edge struct {
	P1 VertexWithID
	P2 VertexWithID
}

type VertexWithID interface {
	ID() string
}

func NewEdge(p1, p2 VertexWithID) edge {
	if strings.Compare(string(p1.ID()), string(p2.ID())) > 0 {
		p1, p2 = p2, p1
	}
	e := edge{P1: p1, P2: p2}
	return e
}

func (e *edge) Key() string {
	return fmt.Sprintf("%v-%v", e.P1.ID(), e.P2.ID())
}

type DotCustomizerFunc func(VertexWithID) string

func ToDot(membersconnections map[VertexWithID][]VertexWithID, filepath string, customizer DotCustomizerFunc) string {

	links := map[string]edge{}
	for m, connected := range membersconnections {
		for _, n := range connected {
			e := NewEdge(m, n)
			k := e.Key()
			if _, ok := links[k]; !ok {
				links[k] = e
			}
		}
	}

	str := "Graph G {\nrankdir=LR;\n"

	for p := range membersconnections {

		if customizer != nil {
			str += fmt.Sprintf("\"%v\" %v\n", p.ID(), customizer(p))
		} else {
			str += fmt.Sprintf("\"%v\"\n", p.ID())
		}
	}

	for _, e := range links {
		str += fmt.Sprintf("\"%v\" -- \"%v\"\n", e.P1.ID(), e.P2.ID())
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
