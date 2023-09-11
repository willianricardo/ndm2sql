package main

import (
	"github.com/willianricardo/ndm2sql/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		panic(err)
	}
}
