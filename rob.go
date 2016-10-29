package main

// Video: https://www.youtube.com/watch?v=cN_DpYBzKso
// Slides: https://talks.golang.org/2012/waza.slide#1

type Request struct {
  fn func() int
  out chan int
}

func requester(worker chan<- Request) {
  out := make(chan int)
  for {
    // Sleep(rand.Int63n(nWorker * 2 * time.Second))
    worker <- Request{workFn, out}
    result := <-out
    furtherPRocess(result)
  }
}

type Worker struct {
  requests chan Request
  pending int
  index int
}

func (w *Worker) work(done chan *Worker) {
  for {
    req := <-w.requests
    req.out <- req.fn()
    done <- w
  }
}

type Pool []*Worker
type Balancer struct {
  pool Pool
  done chan *Worker
}

func (b *Balancer) balance(work chan Request) {
  for {
    select {
    case req := <-work:
      b.dispatch(req)
    case w := <-b.done:
      b.completed(w)
    }
  }
} 

func (p Pool) Less(i, j int) bool {
  return p[i].pending < p[j].pending
}  

func (b *Balancer) dispatch(req Request) {
  w := heap.Pop(&b.pool).(*Worker)
  w.requests <- req
  w.pending++
  heap.Push(&b.pool, w)
} 

func (b *Balancer) completed(w *Worker) {
  w.pending--
  heap.Remove(&b.pool, w.index)
  heap.Push(&b.pool, w)
}  

// replicated search
// todo: need shutdown of `conns`
func Query(conns []Conn, query string) Result {
  ch := make(chan Result, len(conns))
  for _, conn := range conns {
    go func(c Conn) {
      ch <- c.DoQuery(query)
    }(conn)
  }
  return <-ch
}
