package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Mohammad-y-abbass/moDB/internal/storage" // Update this to your module name
)

func main() {
	dbFile := "users.db"
	// Clean up from previous runs
	os.Remove(dbFile)

	// 1. Define the Rules (Schema)
	schema := storage.NewSchema([]storage.Column{
		{Name: "ID", Type: storage.TypeInt32},
		{Name: "Age", Type: storage.TypeInt32},
		{Name: "Username", Type: storage.TypeFixedText, Size: 16},
	})

	// 2. Initialize the Warehouse (Pager)
	pager, err := storage.NewPager(dbFile)
	if err != nil {
		log.Fatalf("Pager failed: %v", err)
	}
	defer pager.Close()

	// 3. Initialize the Manager (Table)
	table := storage.NewTable(pager, schema)

	// 4. Insert some data
	fmt.Println("Storing users...")
	users := [][]interface{}{
		{int32(1), int32(25), "Abbas"},
		{int32(2), int32(30), "Gemini"},
		{int32(3), int32(22), "GoDeveloper"},
	}

	for _, u := range users {
		if err := table.Insert(u); err != nil {
			log.Fatalf("Insert failed: %v", err)
		}
	}

	// 5. Read it back to verify
	fmt.Println("Reading data back from disk...")
	results, err := table.SelectAll()
	if err != nil {
		log.Fatalf("SelectAll failed: %v", err)
	}

	for _, row := range results {
		fmt.Printf("Row: ID=%d, Age=%d, Name=%s\n",
			row.Values[0], row.Values[1], row.Values[2])
	}
}
