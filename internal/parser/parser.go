package parser

type Parser struct {
	input  string
	cursor int
}

func New(input string) *Parser {
	return &Parser{
		input:  input,
		cursor: 0,
	}
}
