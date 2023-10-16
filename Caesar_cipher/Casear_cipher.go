// Ramzi Yakan   300078504

package main

import(
	"fmt"
	"unicode"
	"strings"
	"bytes"
)

func CaesarCipher(m string, shift int) (e string){
	
	shift = shift % 26
	
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
	
	e = buffer.String()
	e = strings.ToUpper(e)
	return e
}

func main(){
	fmt.Println(CaesarCipher("I love CS!", 5))
}