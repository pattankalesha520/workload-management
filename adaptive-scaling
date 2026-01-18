package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type Job struct{ ID int; D time.Duration; C int }

type Node struct {
	ID, Cap, Used int
	Ch            chan Job
	mu            sync.Mutex
}

func NewNode(id, cap int) *Node {
	n := &Node{ID: id, Cap: cap, Ch: make(chan Job, 50)}
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

type Predictor struct{ alpha float64; ema float64; init bool; mu sync.Mutex }

func NewPredictor(a float64) *Predictor { return &Predictor{alpha: a} }

func (p *Predictor) Observe(v int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.init {
		p.ema = float64(v)
		p.init = true
	} else {
		p.ema = p.alpha*float64(v) + (1-p.alpha)*p.ema
	}
}

func (p *Predictor) Forecast() float64 { p.mu.Lock(); v := p.ema; p.mu.Unlock(); return v }

type Cluster struct {
	Nodes []*Node
	Q     chan Job
	mu    sync.Mutex
	rr    int
	min   int
	max   int
	cap   int
}

func NewCluster(initN, cap, minN, maxN int) *Cluster {
	c := &Cluster{Q: make(chan Job, 1000), min: minN, max: maxN, cap: cap}
	for i := 0; i < initN; i++ {
		c.Nodes = append(c.Nodes, NewNode(i+1, cap))
	}
	go c.dispatcher()
	return c
}

func (c *Cluster) addNode() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Nodes) >= c.max { return }
	c.Nodes = append(c.Nodes, NewNode(len(c.Nodes)+1, c.cap))
}

func (c *Cluster) removeNode() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Nodes) <= c.min { return }
	n := c.Nodes[len(c.Nodes)-1]
	c.Nodes = c.Nodes[:len(c.Nodes)-1]
	close(n.Ch)
}

func (c *Cluster) dispatcher() {
	for j := range c.Q {
		assigned := false
		c.mu.Lock()
		if len(c.Nodes) > 0 {
			start := c.rr % len(c.Nodes)
			best := -1; bestAvail := -1
			for i := 0; i < len(c.Nodes); i++ {
				idx := (start + i) % len(c.Nodes)
				n := c.Nodes[idx]
				n.mu.Lock()
				avail := n.Cap - n.Used
				n.mu.Unlock()
				if avail >= j.C && avail > bestAvail { bestAvail = avail; best = idx }
			}
			if best >= 0 {
				c.Nodes[best].Ch <- j
				c.rr = (best + 1) % len(c.Nodes)
				assigned = true
			}
		}
		c.mu.Unlock()
		if !assigned {
			time.Sleep(100 * time.Millisecond)
			go func(jj Job){ c.Q <- jj }(j)
		}
	}
}

func (c *Cluster) totalLoad() int {
	c.mu.Lock(); defer c.mu.Unlock()
	sum := 0
	for _, n := range c.Nodes {
		n.mu.Lock()
		sum += n.Used
		n.mu.Unlock()
	}
	return sum
}

func (c *Cluster) estimateLat() float64 {
	c.mu.Lock(); defer c.mu.Unlock()
	if len(c.Nodes) == 0 { return 0 }
	tot := 0.0
	for _, n := range c.Nodes {
		n.mu.Lock()
		tot += float64(n.Used)/float64(n.Cap)
		n.mu.Unlock()
	}
	avg := tot / float64(len(c.Nodes))
	return 100 + avg*300
}

type Controller struct {
	cluster *Cluster
	pred    *Predictor
	last    time.Time
	cool    time.Duration
	target  float64
}

func NewController(c *Cluster, p *Predictor) *Controller {
	return &Controller{cluster: c, pred: p, cool: 1500 * time.Millisecond, target: 200}
}

func (ctl *Controller) Step() {
	if time.Since(ctl.last) < ctl.cool { return }
	f := ctl.pred.Forecast()
	lat := ctl.cluster.estimateLat()
	need := int(math.Ceil((f - float64(len(ctl.cluster.Nodes))*1.5)/2.0))
	if lat > ctl.target || need > 0 {
		if need < 1 { need = 1 }
		for i := 0; i < need; i++ { ctl.cluster.addNode() }
		ctl.last = time.Now()
		return
	}
	if lat < ctl.target*0.6 && len(ctl.cluster.Nodes) > ctl.cluster.min {
		ctl.cluster.removeNode()
		ctl.last = time.Now()
	}
}

func spawn(c *Cluster, rate int, dur time.Duration) {
	t := time.NewTicker(dur)
	id := 1
	for range t.C {
		for i := 0; i < rate; i++ {
			c.Q <- Job{ID: id, D: time.Duration(200+rand.Intn(600))*time.Millisecond, C: 1 + rand.Intn(3)}
			id++
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	cluster := NewCluster(3, 8, 1, 50)
	pred := NewPredictor(0.6)
	ctrl := NewController(cluster, pred)
	go spawn(cluster, 5, 1*time.Second)
	go func() {
		for range time.Tick(1 * time.Second) {
			pred.Observe(cluster.totalLoad())
		}
	}()
	go func() {
		for range time.Tick(1 * time.Second) {
			ctrl.Step()
		}
	}()
	time.Sleep(30 * time.Second)
	close(cluster.Q)
	cluster.mu.Lock()
	for _, n := range cluster.Nodes { close(n.Ch) }
	cluster.mu.Unlock()
	fmt.Println("finished")
}
