package main

// Taken from:
// http://talks.golang.org/2012/concurrency.slide#1

import "fmt"
import "time"
import "math/rand"

// from earlier
func boring(msg string) <-chan string {
	c := make(chan string)
	go func() {
		for i := 0; ; i++ {
			c <- fmt.Sprintf("%s %d", msg, i)
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		}
	}()
	return c
}

func fanIn(input1, input2 <- chan string) <-chan string {
	c := make(chan string)
	go func() { for { c <- <-input1 } }()
	go func() { for { c <- <-input2 } }()
	return c
}

// last example from youtube

type Result string
type Search func(query string) Result

var (
	Web1 = fakeSearch("web1")
        Web2 = fakeSearch("web2")
	Image1 = fakeSearch("image1")
	Image2 = fakeSearch("image2")
	Video1 = fakeSearch("web1")
	Video2 = fakeSearch("web2")
)

func Google(query string) (results []Result) {
	c := make(chan Result)
	go func() { c <- First(query, Web1, Web2) } ()
	go func() { c <- First(query, Image1, Image2) } ()
	go func() { c <- First(query, Video1, Video2) } ()
	timeout := time.After(80 * time.Millisecond)
	for i := 0; i < 3; i++ {
		select {
		case result := <-c:
			results = append(results, result)
		case <-timeout:
			fmt.Println("timed out")
			return
		}
	}
	return
}

func First(query string, replicas ...Search) Result {
	c := make(chan Result)
	searchReplica := func(i int) {
		c <- replicas[i](query)
	}
	for i := range replicas {
		go searchReplica(i)
	}
        return <-c
}

func fakeSearch(kind string) Search {
	return func(query string) Result {
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		return Result(fmt.Sprintf("%s result for %q", kind, query))
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()
	results := Google("golang")
	elapsed := time.Since(start)
	fmt.Println(results)
	fmt.Println(elapsed)
}
