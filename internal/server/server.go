package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/Mohammad-y-abbass/moDB/internal/executor"
	"github.com/Mohammad-y-abbass/moDB/internal/lexer"
	"github.com/Mohammad-y-abbass/moDB/internal/parser"
	"github.com/Mohammad-y-abbass/moDB/internal/planner"
	"github.com/Mohammad-y-abbass/moDB/internal/storage"
)

var (
	exec *executor.Executor
	plan *planner.Planner
)

func Start() {
	// Initialize Storage Engine
	engine := storage.NewEngine("./data")

	exec = executor.New(engine)
	err := exec.ReloadTables()
	if err != nil {
		fmt.Println("Warning: Could not reload tables:", err)
	}

	plan = planner.New()

	listener, err := net.Listen("tcp", ":3003")
	if err != nil {
		fmt.Println("Could not start server: ", err)
	}
	defer listener.Close()

	fmt.Println("Server is running on port 3003")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error: ", err)
			continue
		}

		go handleConnection(conn)
	}
}

const (
	colorRed   = "\033[31m"
	colorGreen = "\033[32m"
	colorReset = "\033[0m"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("--- New connection from %s ---\n", conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)
	var queryBuffer strings.Builder

	// Send an initial prompt
	prompt := "moDB> "
	conn.Write([]byte(prompt))

	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		if trimmedLine == "" && queryBuffer.Len() == 0 {
			conn.Write([]byte(prompt))
			continue
		}

		queryBuffer.WriteString(line + " ")

		// If the line ends with a semicolon, process the accumulated query
		if strings.HasSuffix(trimmedLine, ";") {
			fullQuery := strings.TrimSpace(queryBuffer.String())
			queryBuffer.Reset()
			prompt = "moDB> "

			fmt.Printf("Received query: %q\n", fullQuery)

			l := lexer.New(fullQuery)
			p := parser.New(l)
			program := p.ParseProgram()

			if len(p.Errors()) > 0 {
				errorMsg := p.GetErrorMessage()
				fmt.Printf("%sParsing error:%s %s\n", colorRed, colorReset, errorMsg)
				conn.Write([]byte(colorRed + errorMsg + colorReset + "\n"))
			} else {
				for _, stmt := range program.Statements {
					pNode := plan.GeneratePlan(stmt)
					results, err := exec.Execute(pNode)
					if err != nil {
						errMsg := "Execution error: " + err.Error()
						fmt.Printf("%s%s%s\n", colorRed, errMsg, colorReset)
						conn.Write([]byte(colorRed + errMsg + colorReset + "\n"))
						continue
					}

					if len(results.Columns) == 0 && len(results.Rows) == 0 {
						msg := "Success (Action completed)"
						if results.Message != "" {
							msg = results.Message
						}
						conn.Write([]byte(colorGreen + msg + colorReset + "\n"))
					} else {
						res := executor.FormatResultSet(results)
						conn.Write([]byte(res + "\n"))
					}
				}
			}
		} else {
			// Continue accumulating multi-line statement
			prompt = "   -> "
		}
		conn.Write([]byte(prompt))
	}
	fmt.Printf("--- Connection closed ---\n")
}
