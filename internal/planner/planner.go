package planner

import (
	"github.com/Mohammad-y-abbass/moDB/internal/ast"
)

type PlanNode interface {
	PlanNode()
}

type ScanNode struct {
	TableName string
}

func (n *ScanNode) PlanNode() {}

type FilterNode struct {
	Child PlanNode
	Left  string
	Op    string
	Right string
}

func (n *FilterNode) PlanNode() {}

type ProjectNode struct {
	Child   PlanNode
	Columns []string
}

func (n *ProjectNode) PlanNode() {}

type InsertNode struct {
	TableName string
	Columns   []string
	Values    []string
}

func (n *InsertNode) PlanNode() {}

type UpdateNode struct {
	TableName string
	Sets      map[string]string
	Where     *ast.WhereClause
}

func (n *UpdateNode) PlanNode() {}

type DeleteNode struct {
	TableName string
	Where     *ast.WhereClause
}

func (n *DeleteNode) PlanNode() {}

type CreateTableNode struct {
	TableName string
	Columns   []ast.ColumnDefinition
}

func (n *CreateTableNode) PlanNode() {}

type CreateDatabaseNode struct {
	DatabaseName string
}

func (n *CreateDatabaseNode) PlanNode() {}

type UseDatabaseNode struct {
	DatabaseName string
}

func (n *UseDatabaseNode) PlanNode() {}

type Planner struct{}

func New() *Planner {
	return &Planner{}
}

func (p *Planner) GeneratePlan(stmt ast.Statement) PlanNode {
	switch s := stmt.(type) {
	case *ast.CreateDatabaseStatement:
		return &CreateDatabaseNode{
			DatabaseName: s.DatabaseName,
		}
	case *ast.UseDatabaseStatement:
		return &UseDatabaseNode{
			DatabaseName: s.DatabaseName,
		}
	case *ast.CreateTableStatement:
		return &CreateTableNode{
			TableName: s.Table,
			Columns:   s.Columns,
		}
	case *ast.SelectStatement:
		var node PlanNode = &ScanNode{TableName: s.Table}
		if s.Where != nil {
			node = &FilterNode{
				Child: node,
				Left:  s.Where.Left,
				Op:    s.Where.Op,
				Right: s.Where.Right,
			}
		}
		if len(s.Columns) > 0 && s.Columns[0] != "*" {
			node = &ProjectNode{
				Child:   node,
				Columns: s.Columns,
			}
		}
		return node
	case *ast.InsertStatement:
		return &InsertNode{
			TableName: s.Table,
			Columns:   s.Columns,
			Values:    s.Values,
		}
	case *ast.UpdateStatement:
		return &UpdateNode{
			TableName: s.Table,
			Sets:      s.Sets,
			Where:     s.Where,
		}
	case *ast.DeleteStatement:
		return &DeleteNode{
			TableName: s.Table,
			Where:     s.Where,
		}
	}
	return nil
}
