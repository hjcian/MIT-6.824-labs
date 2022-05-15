package main

import (
	"fmt"
	"strings"
	"unicode"
)

var content = `
The Project Gutenberg EBook of Grimms' Fairy Tales, by The Brothers Grimm

This eBook is for the use of anyone anywhere at no cost and with
almost no restrictions whatsoever.  You may copy it, give it away or
re-use it under the terms of the Project Gutenberg License included
with this eBook or online at www.gutenberg.org
`

func main() {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(content, ff)
	for _, w := range words {
		fmt.Println(w)
	}
}
