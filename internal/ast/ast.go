package ast

type Node interface {
	kind() string
}

type Expression interface {
	Node
	evaluate() any
}

type SelectStatement struct {
	expressions []Expression
}

func (s *SelectStatement) kind() string {
	return "select_statement"
}

type IntegerLiteral struct {
	value int
}

type BooleanLiteral struct {
	value bool
}

func (i *IntegerLiteral) kind() string {
	return "integer_literal"
}

func (i *IntegerLiteral) evaluate() any {
	return i.value
}

func (b *BooleanLiteral) kind() string {
	return "boolean_literal"
}

func (b *BooleanLiteral) evaluate() any {
	return b.value
}
