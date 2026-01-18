package main
import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	ID int
	D time.Duration
	C int
}

type Node struct {
	ID, Cap, Used int
	Ch            chan Job
	mu            sync.Mutex
}

func NewNode(id, cap int) *Node {
	n := &Node{ID: id, Cap: cap, Ch: make(chan Job, 20)}
	go func(nn *Node) {
		for j := range nn.Ch {
			nn.mu.Lock()
			nn.Used += j.C
			nn.mu.Unlock()
			time.Sleep(j.D)
			nn.mu.Lock()
			nn.Used -= j.C
			nn.mu.Unlock()
		}
	}(n)
	return n
}

type Cluster struct {
	Nodes []*Node
	Q     chan Job
	mu    sync.Mutex
	rr    int
	up, down float64
	min, max int
}

func NewCluster(n, cap int) *Cluster {
	c := &Cluster{Q: make(chan Job, 200), up: 75, down: 30, min: 1, max: 20}
	for i := 0; i < n; i++ {
		c.Nodes = append(c.Nodes, NewNode(i+1, cap))
	}
	go c.dispatch()
	go c.monitor()
	return c
}

func (c *Cluster) add(cap int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Nodes) >= c.max {
		return
	}
	id := len(c.Nodes) + 1
	c.Nodes = append(c.Nodes, NewNode(id, cap))
}

func (c *Cluster) remove() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Nodes) <= c.min {
		return
	}
	n := c.Nodes[len(c.Nodes)-1]
	close(n.Ch)
	c.Nodes = c.Nodes[:len(c.Nodes)-1]
}

func (c *Cluster) dispatch() {
	for j := range c.Q {
		assigned := false
		c.mu.Lock()
		if len(c.Nodes) > 0 {
			start := c.rr % len(c.Nodes)
			for i := 0; i < len(c.Nodes); i++ {
				idx := (start + i) % len(c.Nodes)
				n := c.Nodes[idx]
				n.mu.Lock()
				if n.Used+j.C <= n.Cap {
					n.Ch <- j
					assigned = true
					n.mu.Unlock()
					c.rr = (idx + 1) % len(c.Nodes)
					break
				}
				n.mu.Unlock()
			}
		}
		c.mu.Unlock()
		if !assigned {
			time.Sleep(150 * time.Millisecond)
			go func(jj Job) { c.Q <- jj }(j)
		}
	}
}

func (c *Cluster) monitor() {
	t := time.NewTicker(2 * time.Second)
	for range t.C {
		totalU, totalC := 0, 0
		c.mu.Lock()
		for _, n := range c.Nodes {
			n.mu.Lock()
			totalU += n.Used
			totalC += n.Cap
			n.mu.Unlock()
		}
		cnt := len(c.Nodes)
		c.mu.Unlock()
		util := 0.0
		if totalC > 0 {
			util = float64(totalU) / float64(totalC) * 100
		}
		fmt.Printf("Nodes:%d Util:%.1f%%\n", cnt, util)
		if util > c.up {
			c.add(8)
		} else if util < c.down {
			c.remove()
		}
	}
}

func spawn(c *Cluster, cnt int) {
	for i := 0; i < cnt; i++ {
		c.Q <- Job{ID: i + 1, D: time.Duration(200+rand.Intn(600)) * time.Millisecond, C: 1 + rand.Intn(3)}
		time.Sleep(90 * time.Millisecond)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	c := NewCluster(3, 8)
	go spawn(c, 150)
	time.Sleep(25 * time.Second)
	close(c.Q)
	time.Sleep(1 * time.Second)
	c.mu.Lock()
	for _, n := range c.Nodes {
		close(n.Ch)
	}
	c.mu.Unlock()
}
