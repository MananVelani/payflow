package main

import (
	"fmt"
	"io"
	"net/http"
)

func main() {
	resp, err := http.Post("http://localhost:9999/v1/payments", "application/json", nil)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))
}
