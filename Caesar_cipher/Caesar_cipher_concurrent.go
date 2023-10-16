// Ramzi Yakan   300078504

package main

import(
	"fmt"
	"unicode"
	"strings"
	"bytes"
	"time"
	"sync"
)


func CaesarCipherList(messages []string, shift int, ch chan string){
	
	shift = shift % 26
	
	for _, m := range messages{
		
		var r []rune

		m = strings.ToUpper(m)

		// from string to slice of unicode
		for _,c := range m { 
			if unicode.IsLetter(c){
				r = append(r, c) // add character to slice of unicode
			}	
		}
		
		// from slice of unicode to string
		var buffer bytes.Buffer 
		for _,c := range r {
			s := int(c) + shift
			if s > 'Z' {
				c = rune(s - 26)
			} else if s < 'A' {
				c = rune(s + 26)
			} else{
				c = rune(s)
			}
			buffer.WriteRune(c)
		}

		e := buffer.String()
		e = strings.ToUpper(e)
		ch <- e
	}
}

func main() {
	
	var wg sync.WaitGroup
	
	// List of messages
	messages:= []string{"Csi2520", "Csi2120", "3 Paradigms", 
	"Go is 1st", "Prolog is 2nd", "Scheme is 3rd", 
   "uottawa.ca", "csi/elg/ceg/seg", "800 King Edward"}
	
    // Create channels???
	ch := make(chan string)

	// call go funtion
	wg.Add(1)
	go func() {
		defer wg.Done()
		CaesarCipherList(messages[:],2, ch) // send channels???
	}()

	// print results ???
	go func() {
		for {
			m, more := <-ch
			if more {
				fmt.Println(m)
				time.Sleep(time.Second) 
			} else{
				close(ch)
				wg.Done()
			}
		}
	}()
	
	// add synchronisation ???
	wg.Wait()
}