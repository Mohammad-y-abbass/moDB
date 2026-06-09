package planner

import (
	"testing"

	"github.com/Mohammad-y-abbass/moDB/internal/ast"
	"github.com/Mohammad-y-abbass/moDB/internal/lexer"
)

func TestPlanNodeImplementations(t *testing.T) {
	nodes := []PlanNode{
		&ScanNode{TableName: "users"},
		&FilterNode{Child: &ScanNode{}, Left: "id", Op: "=", Right: "1"},
		&ProjectNode{Child: &ScanNode{}, Columns: []string{"name"}},
		&InsertNode{TableName: "users", Columns: []string{"name"}, Values: []string{"john"}},
		&UpdateNode{TableName: "users", Sets: map[string]string{"name": "john"}},
		&DeleteNode{TableName: "users", Where: nil},
		&CreateTableNode{TableName: "users"},
		&ShowDatabasesNode{},
		&ShowTablesNode{},
		&DropTableNode{TableName: "users"},
		&DropDatabaseNode{DatabaseName: "mydb"},
		&CreateDatabaseNode{DatabaseName: "mydb"},
		&UseDatabaseNode{DatabaseName: "mydb"},
		&JoinNode{
			Left:  &ScanNode{TableName: "orders"},
			Right: &ScanNode{TableName: "users"},
		},
	}

	for _, n := range nodes {
		n.PlanNode() // Should not panic
	}
}

func TestNew(t *testing.T) {
	p := New()
	if p == nil {
		t.Fatal("expected non-nil planner")
	}
}

func TestGeneratePlanSelect(t *testing.T) {
	p := New()

	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "users",
	}

	plan := p.GeneratePlan(stmt)
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}

	scan, ok := plan.(*ScanNode)
	if !ok {
		t.Fatalf("expected *ScanNode, got %T", plan)
	}
	if scan.TableName != "users" {
		t.Errorf("expected users, got %s", scan.TableName)
	}
}

func TestGeneratePlanSelectWithWhere(t *testing.T) {
	p := New()

	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "users",
		Where: &ast.WhereClause{
			Token: lexer.Token{Type: lexer.WHERE_TOKEN, Value: "WHERE"},
			Left:  "id",
			Op:    "=",
			Right: "1",
		},
	}

	plan := p.GeneratePlan(stmt)
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}

	filter, ok := plan.(*FilterNode)
	if !ok {
		t.Fatalf("expected *FilterNode, got %T", plan)
	}
	if filter.Left != "id" || filter.Op != "=" || filter.Right != "1" {
		t.Errorf("filter mismatch: %+v", filter)
	}

	_, ok = filter.Child.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode as child of FilterNode, got %T", filter.Child)
	}
}

func TestGeneratePlanSelectWithProjection(t *testing.T) {
	p := New()

	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"id", "name"},
		Table:   "users",
	}

	plan := p.GeneratePlan(stmt)
	proj, ok := plan.(*ProjectNode)
	if !ok {
		t.Fatalf("expected *ProjectNode, got %T", plan)
	}
	if len(proj.Columns) != 2 || proj.Columns[0] != "id" {
		t.Errorf("project columns mismatch: %v", proj.Columns)
	}

	_, ok = proj.Child.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode as child of ProjectNode")
	}
}

func TestGeneratePlanSelectWithWhereAndProjection(t *testing.T) {
	p := New()

	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"name"},
		Table:   "users",
		Where: &ast.WhereClause{
			Token: lexer.Token{Type: lexer.WHERE_TOKEN, Value: "WHERE"},
			Left:  "id",
			Op:    "=",
			Right: "1",
		},
	}

	plan := p.GeneratePlan(stmt)
	proj, ok := plan.(*ProjectNode)
	if !ok {
		t.Fatalf("expected *ProjectNode, got %T", plan)
	}

	filter, ok := proj.Child.(*FilterNode)
	if !ok {
		t.Fatalf("expected FilterNode as child of ProjectNode, got %T", proj.Child)
	}

	_, ok = filter.Child.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode as child of FilterNode")
	}
}

func TestGeneratePlanInsert(t *testing.T) {
	p := New()
	stmt := &ast.InsertStatement{
		Token: lexer.Token{Type: lexer.INSERT_TOKEN, Value: "INSERT"},
		Table: "users",
		Values: []string{"1", "john"},
	}

	plan := p.GeneratePlan(stmt)
	ins, ok := plan.(*InsertNode)
	if !ok {
		t.Fatalf("expected *InsertNode, got %T", plan)
	}
	if ins.TableName != "users" || len(ins.Values) != 2 {
		t.Errorf("insert mismatch: %+v", ins)
	}
}

func TestGeneratePlanInsertWithColumns(t *testing.T) {
	p := New()
	stmt := &ast.InsertStatement{
		Token:   lexer.Token{Type: lexer.INSERT_TOKEN, Value: "INSERT"},
		Table:   "users",
		Columns: []string{"name", "age"},
		Values:  []string{"john", "30"},
	}

	plan := p.GeneratePlan(stmt)
	ins, ok := plan.(*InsertNode)
	if !ok {
		t.Fatalf("expected *InsertNode, got %T", plan)
	}
	if len(ins.Columns) != 2 || ins.Columns[0] != "name" {
		t.Errorf("insert columns mismatch: %v", ins.Columns)
	}
}

func TestGeneratePlanUpdate(t *testing.T) {
	p := New()
	stmt := &ast.UpdateStatement{
		Token: lexer.Token{Type: lexer.UPDATE_TOKEN, Value: "UPDATE"},
		Table: "users",
		Sets:  map[string]string{"name": "john"},
		Where: &ast.WhereClause{
			Token: lexer.Token{Type: lexer.WHERE_TOKEN, Value: "WHERE"},
			Left:  "id", Op: "=", Right: "1",
		},
	}

	plan := p.GeneratePlan(stmt)
	upd, ok := plan.(*UpdateNode)
	if !ok {
		t.Fatalf("expected *UpdateNode, got %T", plan)
	}
	if upd.TableName != "users" || upd.Sets["name"] != "john" {
		t.Errorf("update mismatch: %+v", upd)
	}
	if upd.Where == nil || upd.Where.Left != "id" {
		t.Errorf("update where mismatch")
	}
}

func TestGeneratePlanUpdateWithoutWhere(t *testing.T) {
	p := New()
	stmt := &ast.UpdateStatement{
		Token: lexer.Token{Type: lexer.UPDATE_TOKEN, Value: "UPDATE"},
		Table: "users",
		Sets:  map[string]string{"name": "john"},
		Where: nil,
	}

	plan := p.GeneratePlan(stmt)
	upd, ok := plan.(*UpdateNode)
	if !ok {
		t.Fatalf("expected *UpdateNode, got %T", plan)
	}
	if upd.Where != nil {
		t.Error("expected nil where")
	}
}

func TestGeneratePlanDelete(t *testing.T) {
	p := New()
	stmt := &ast.DeleteStatement{
		Token: lexer.Token{Type: lexer.DELETE_TOKEN, Value: "DELETE"},
		Table: "users",
		Where: &ast.WhereClause{
			Token: lexer.Token{Type: lexer.WHERE_TOKEN, Value: "WHERE"},
			Left:  "id", Op: "=", Right: "1",
		},
	}

	plan := p.GeneratePlan(stmt)
	del, ok := plan.(*DeleteNode)
	if !ok {
		t.Fatalf("expected *DeleteNode, got %T", plan)
	}
	if del.TableName != "users" || del.Where == nil {
		t.Errorf("delete mismatch: %+v", del)
	}
}

func TestGeneratePlanDeleteWithoutWhere(t *testing.T) {
	p := New()
	stmt := &ast.DeleteStatement{
		Token: lexer.Token{Type: lexer.DELETE_TOKEN, Value: "DELETE"},
		Table: "users",
		Where: nil,
	}

	plan := p.GeneratePlan(stmt)
	del, ok := plan.(*DeleteNode)
	if !ok {
		t.Fatalf("expected *DeleteNode, got %T", plan)
	}
	if del.Where != nil {
		t.Error("expected nil where for delete")
	}
}

func TestGeneratePlanShowDatabases(t *testing.T) {
	p := New()
	stmt := &ast.ShowDatabasesStatement{
		Token: lexer.Token{Type: lexer.SHOW_TOKEN, Value: "SHOW"},
	}

	plan := p.GeneratePlan(stmt)
	_, ok := plan.(*ShowDatabasesNode)
	if !ok {
		t.Fatalf("expected *ShowDatabasesNode, got %T", plan)
	}
}

func TestGeneratePlanShowTables(t *testing.T) {
	p := New()
	stmt := &ast.ShowTablesStatement{
		Token: lexer.Token{Type: lexer.SHOW_TOKEN, Value: "SHOW"},
	}

	plan := p.GeneratePlan(stmt)
	_, ok := plan.(*ShowTablesNode)
	if !ok {
		t.Fatalf("expected *ShowTablesNode, got %T", plan)
	}
}

func TestGeneratePlanDropTable(t *testing.T) {
	p := New()
	stmt := &ast.DropTableStatement{
		Token: lexer.Token{Type: lexer.DROP_TOKEN, Value: "DROP"},
		Table: "users",
	}

	plan := p.GeneratePlan(stmt)
	dropTbl, ok := plan.(*DropTableNode)
	if !ok {
		t.Fatalf("expected *DropTableNode, got %T", plan)
	}
	if dropTbl.TableName != "users" {
		t.Errorf("expected users, got %s", dropTbl.TableName)
	}
}

func TestGeneratePlanDropDatabase(t *testing.T) {
	p := New()
	stmt := &ast.DropDatabaseStatement{
		Token:        lexer.Token{Type: lexer.DROP_TOKEN, Value: "DROP"},
		DatabaseName: "mydb",
	}

	plan := p.GeneratePlan(stmt)
	dropDB, ok := plan.(*DropDatabaseNode)
	if !ok {
		t.Fatalf("expected *DropDatabaseNode, got %T", plan)
	}
	if dropDB.DatabaseName != "mydb" {
		t.Errorf("expected mydb, got %s", dropDB.DatabaseName)
	}
}

func TestGeneratePlanCreateDatabase(t *testing.T) {
	p := New()
	stmt := &ast.CreateDatabaseStatement{
		Token:        lexer.Token{Type: lexer.CREATE_TOKEN, Value: "CREATE"},
		DatabaseName: "mydb",
	}

	plan := p.GeneratePlan(stmt)
	createDB, ok := plan.(*CreateDatabaseNode)
	if !ok {
		t.Fatalf("expected *CreateDatabaseNode, got %T", plan)
	}
	if createDB.DatabaseName != "mydb" {
		t.Errorf("expected mydb, got %s", createDB.DatabaseName)
	}
}

func TestGeneratePlanUseDatabase(t *testing.T) {
	p := New()
	stmt := &ast.UseDatabaseStatement{
		Token:        lexer.Token{Type: lexer.USE_TOKEN, Value: "USE"},
		DatabaseName: "mydb",
	}

	plan := p.GeneratePlan(stmt)
	useDB, ok := plan.(*UseDatabaseNode)
	if !ok {
		t.Fatalf("expected *UseDatabaseNode, got %T", plan)
	}
	if useDB.DatabaseName != "mydb" {
		t.Errorf("expected mydb, got %s", useDB.DatabaseName)
	}
}

func TestGeneratePlanCreateTable(t *testing.T) {
	p := New()
	stmt := &ast.CreateTableStatement{
		Token: lexer.Token{Type: lexer.CREATE_TOKEN, Value: "CREATE"},
		Table: "users",
		Columns: []ast.ColumnDefinition{
			{Name: "id", DataType: "INT", IsNullable: false, IsPrimaryKey: true},
		},
	}

	plan := p.GeneratePlan(stmt)
	createTbl, ok := plan.(*CreateTableNode)
	if !ok {
		t.Fatalf("expected *CreateTableNode, got %T", plan)
	}
	if createTbl.TableName != "users" || len(createTbl.Columns) != 1 {
		t.Errorf("create table mismatch: %+v", createTbl)
	}
}

func TestGeneratePlanSelectWithJoin(t *testing.T) {
	p := New()
	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "orders",
		Join: &ast.JoinClause{
			Table:    "users",
			LeftKey:  "orders.user_id",
			RightKey: "users.id",
		},
	}

	plan := p.GeneratePlan(stmt)
	join, ok := plan.(*JoinNode)
	if !ok {
		t.Fatalf("expected *JoinNode, got %T", plan)
	}
	if join.Left.TableName != "orders" || join.Right.TableName != "users" {
		t.Errorf("join tables mismatch: left=%s, right=%s", join.Left.TableName, join.Right.TableName)
	}
	if join.LeftKey != "orders.user_id" || join.RightKey != "users.id" {
		t.Errorf("join keys mismatch: %s, %s", join.LeftKey, join.RightKey)
	}
}

func TestGeneratePlanSelectWithJoinAndWhere(t *testing.T) {
	p := New()
	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "orders",
		Join: &ast.JoinClause{
			Table:    "users",
			LeftKey:  "orders.user_id",
			RightKey: "users.id",
		},
		Where: &ast.WhereClause{
			Token: lexer.Token{Type: lexer.WHERE_TOKEN, Value: "WHERE"},
			Left:  "users.name",
			Op:    "=",
			Right: "john",
		},
	}

	plan := p.GeneratePlan(stmt)
	join, ok := plan.(*JoinNode)
	if !ok {
		t.Fatalf("expected *JoinNode, got %T", plan)
	}
	if join.Where == nil {
		t.Fatal("expected where clause in join node")
	}
	if join.Where.Left != "users.name" {
		t.Errorf("expected users.name, got %s", join.Where.Left)
	}
}

func TestGeneratePlanUnsupported(t *testing.T) {
	p := New()
	plan := p.GeneratePlan(nil)
	if plan != nil {
		t.Errorf("expected nil for unsupported statement, got %T", plan)
	}
}

func TestScanNodeFields(t *testing.T) {
	n := &ScanNode{TableName: "users"}
	if n.TableName != "users" {
		t.Errorf("expected users, got %s", n.TableName)
	}
}

func TestFilterNodeFields(t *testing.T) {
	child := &ScanNode{TableName: "users"}
	n := &FilterNode{Child: child, Left: "id", Op: "=", Right: "1"}
	if n.Child != child || n.Left != "id" || n.Op != "=" || n.Right != "1" {
		t.Errorf("FilterNode fields mismatch")
	}
}

func TestProjectNodeFields(t *testing.T) {
	child := &ScanNode{TableName: "users"}
	n := &ProjectNode{Child: child, Columns: []string{"id", "name"}}
	if n.Child != child || len(n.Columns) != 2 {
		t.Errorf("ProjectNode fields mismatch")
	}
}

func TestJoinNodeFields(t *testing.T) {
	n := &JoinNode{
		Left:     &ScanNode{TableName: "orders"},
		Right:    &ScanNode{TableName: "users"},
		LeftKey:  "orders.user_id",
		RightKey: "users.id",
		Columns:  []string{"*"},
		Where:    nil,
	}
	if n.Left.TableName != "orders" || n.Right.TableName != "users" {
		t.Errorf("JoinNode table fields mismatch")
	}
}

func TestGeneratePlanSelectWithEmptyColumns(t *testing.T) {
	p := New()
	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{},
		Table:   "users",
	}

	plan := p.GeneratePlan(stmt)
	_, ok := plan.(*ScanNode)
	if !ok {
		t.Fatalf("expected *ScanNode for empty columns, got %T", plan)
	}
}

func TestGeneratePlanSelectWithStar(t *testing.T) {
	p := New()
	stmt := &ast.SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "users",
	}

	plan := p.GeneratePlan(stmt)
	_, ok := plan.(*ScanNode)
	if !ok {
		t.Fatalf("expected *ScanNode for *, got %T", plan)
	}
}
