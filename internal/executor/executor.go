package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Mohammad-y-abbass/moDB/internal/planner"
	"github.com/Mohammad-y-abbass/moDB/internal/storage"
)

type ResultSet struct {
	Columns []string
	Rows    []storage.Row
}

type Executor struct {
	Engine *storage.Engine
	Tables map[string]*storage.Table
}

func New(engine *storage.Engine) *Executor {
	return &Executor{
		Engine: engine,
		Tables: make(map[string]*storage.Table),
	}
}

func (e *Executor) RegisterTable(name string, table *storage.Table) {
	e.Tables[name] = table
}

func (e *Executor) Execute(plan planner.PlanNode) (ResultSet, error) {
	switch n := plan.(type) {
	case *planner.ScanNode:
		table, ok := e.Tables[n.TableName]
		if !ok {
			return ResultSet{}, fmt.Errorf("table not found: %s", n.TableName)
		}
		rows, err := table.SelectAll()
		if err != nil {
			return ResultSet{}, err
		}

		cols := []string{}
		for _, c := range table.Schema.Columns {
			cols = append(cols, c.Name)
		}

		return ResultSet{Columns: cols, Rows: rows}, nil

	case *planner.FilterNode:
		res, err := e.Execute(n.Child)
		if err != nil {
			return ResultSet{}, err
		}
		table := e.getTableFromPlan(n.Child)
		if table == nil {
			return ResultSet{}, fmt.Errorf("could not determine table for filter")
		}

		var filtered []storage.Row
		for _, row := range res.Rows {
			match, err := e.evaluateFilter(row, table.Schema, n)
			if err != nil {
				return ResultSet{}, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		return ResultSet{Columns: res.Columns, Rows: filtered}, nil

	case *planner.ProjectNode:
		res, err := e.Execute(n.Child)
		if err != nil {
			return ResultSet{}, err
		}
		table := e.getTableFromPlan(n.Child)
		if table == nil {
			return ResultSet{}, fmt.Errorf("could not determine table for projection")
		}

		projectedRows, err := e.applyProjection(res.Rows, table.Schema, n.Columns)
		if err != nil {
			return ResultSet{}, err
		}

		return ResultSet{Columns: n.Columns, Rows: projectedRows}, nil

	case *planner.InsertNode:
		table, ok := e.Tables[n.TableName]
		if !ok {
			return ResultSet{}, fmt.Errorf("table not found: %s", n.TableName)
		}

		convertedValues, err := e.convertValues(n.Values, table.Schema)
		if err != nil {
			return ResultSet{}, err
		}

		// Basic UNIQUE/PK constraint check (expensive scan)
		existingRows, _ := table.SelectAll()
		for _, col := range table.Schema.Columns {
			if col.IsUnique || col.IsPrimaryKey {
				colIdx := -1
				for i, c := range table.Schema.Columns {
					if c.Name == col.Name {
						colIdx = i
						break
					}
				}
				newVal := convertedValues[colIdx]
				for _, row := range existingRows {
					if row.Values[colIdx] == newVal {
						return ResultSet{}, fmt.Errorf("UNIQUE constraint violation on column %s: value %v already exists", col.Name, newVal)
					}
				}
			}
		}

		err = table.Insert(convertedValues)
		if err != nil {
			return ResultSet{}, err
		}
		return ResultSet{}, nil

	case *planner.DeleteNode:
		if _, ok := e.Tables[n.TableName]; !ok {
			return ResultSet{}, fmt.Errorf("table not found: %s", n.TableName)
		}
		return ResultSet{}, fmt.Errorf("DELETE not fully implemented")

	case *planner.UpdateNode:
		if _, ok := e.Tables[n.TableName]; !ok {
			return ResultSet{}, fmt.Errorf("table not found: %s", n.TableName)
		}
		return ResultSet{}, fmt.Errorf("UPDATE not fully implemented")

	case *planner.CreateTableNode:
		if _, ok := e.Tables[n.TableName]; ok {
			return ResultSet{}, fmt.Errorf("table already exists: %s", n.TableName)
		}

		var storageCols []storage.Column
		for _, c := range n.Columns {
			var dataType storage.DataType
			var size uint32 = 32 // Default for text
			switch strings.ToUpper(c.DataType) {
			case "INT", "INTEGER":
				dataType = storage.TypeInt32
				size = 4
			case "TEXT", "VARCHAR":
				dataType = storage.TypeFixedText
				if c.Size > 0 {
					size = uint32(c.Size)
				} else {
					size = 32 // Default if not specified
				}
			default:
				return ResultSet{}, fmt.Errorf("unsupported type: %s", c.DataType)
			}
			storageCols = append(storageCols, storage.Column{
				Name:         c.Name,
				Type:         dataType,
				Size:         size,
				IsNullable:   c.IsNullable,
				IsUnique:     c.IsUnique,
				IsPrimaryKey: c.IsPrimaryKey,
			})
		}

		schema := storage.NewSchema(storageCols)
		// For now, we use a simple path. In a real DB, the engine would manage this.
		dbPath := fmt.Sprintf("./data/testdb/%s.db", n.TableName)
		pager, err := storage.NewPager(dbPath)
		if err != nil {
			return ResultSet{}, err
		}
		table := storage.NewTable(pager, schema)
		e.RegisterTable(n.TableName, table)

		return ResultSet{}, nil
	}

	return ResultSet{}, fmt.Errorf("unknown plan node type")
}

func (e *Executor) getTableFromPlan(plan planner.PlanNode) *storage.Table {
	switch n := plan.(type) {
	case *planner.ScanNode:
		return e.Tables[n.TableName]
	case *planner.FilterNode:
		return e.getTableFromPlan(n.Child)
	case *planner.ProjectNode:
		return e.getTableFromPlan(n.Child)
	}
	return nil
}

func (e *Executor) evaluateFilter(row storage.Row, schema *storage.Schema, filter *planner.FilterNode) (bool, error) {
	colIdx := -1
	for i, col := range schema.Columns {
		if col.Name == filter.Left {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return false, fmt.Errorf("column not found: %s", filter.Left)
	}

	val := row.Values[colIdx]

	switch v := val.(type) {
	case int32:
		target, err := strconv.Atoi(filter.Right)
		if err != nil {
			return false, fmt.Errorf("invalid value for int32: %s", filter.Right)
		}
		rhs := int32(target)
		switch filter.Op {
		case "=":
			return v == rhs, nil
		case "!=":
			return v != rhs, nil
		case ">":
			return v > rhs, nil
		case "<":
			return v < rhs, nil
		case ">=":
			return v >= rhs, nil
		case "<=":
			return v <= rhs, nil
		}
	case uint32:
		target, err := strconv.ParseUint(filter.Right, 10, 32)
		if err != nil {
			return false, fmt.Errorf("invalid value for uint32: %s", filter.Right)
		}
		rhs := uint32(target)
		switch filter.Op {
		case "=":
			return v == rhs, nil
		case "!=":
			return v != rhs, nil
		case ">":
			return v > rhs, nil
		case "<":
			return v < rhs, nil
		case ">=":
			return v >= rhs, nil
		case "<=":
			return v <= rhs, nil
		}
	case string:
		rhs := filter.Right
		switch filter.Op {
		case "=":
			return v == rhs, nil
		case "!=":
			return v != rhs, nil
		case ">":
			return v > rhs, nil
		case "<":
			return v < rhs, nil
		case ">=":
			return v >= rhs, nil
		case "<=":
			return v <= rhs, nil
		}
	}

	return false, nil
}

func (e *Executor) applyProjection(rows []storage.Row, schema *storage.Schema, columns []string) ([]storage.Row, error) {
	colIndices := []int{}
	for _, colName := range columns {
		found := false
		for i, col := range schema.Columns {
			if col.Name == colName {
				colIndices = append(colIndices, i)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column not found: %s", colName)
		}
	}

	projectedRows := []storage.Row{}
	for _, row := range rows {
		newValues := []interface{}{}
		for _, idx := range colIndices {
			newValues = append(newValues, row.Values[idx])
		}
		projectedRows = append(projectedRows, storage.Row{Values: newValues})
	}
	return projectedRows, nil
}

func (e *Executor) convertValues(values []string, schema *storage.Schema) ([]interface{}, error) {
	if len(values) != len(schema.Columns) {
		return nil, fmt.Errorf("value count mismatch: expected %d, got %d", len(schema.Columns), len(values))
	}

	converted := make([]interface{}, len(values))
	for i, col := range schema.Columns {
		val := values[i]

		// Handle NULL (if we had a NULL token in values, but for now we check string "NULL")
		if strings.ToUpper(val) == "NULL" {
			if !col.IsNullable {
				return nil, fmt.Errorf("column %s cannot be NULL", col.Name)
			}
			converted[i] = nil
			continue
		}

		switch col.Type {
		case storage.TypeInt32:
			v, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("invalid value for column %s (INT): %s", col.Name, val)
			}
			converted[i] = int32(v)
		case storage.TypeUint32:
			v, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid value for column %s (UINT): %s", col.Name, val)
			}
			converted[i] = uint32(v)
		case storage.TypeFixedText:
			converted[i] = val
		}
	}
	return converted, nil
}

func FormatResultSet(res ResultSet) string {
	if len(res.Columns) == 0 && len(res.Rows) == 0 {
		return "Success (Action completed)"
	}

	var sb strings.Builder

	// Header
	sb.WriteString("| ")
	for _, col := range res.Columns {
		sb.WriteString(fmt.Sprintf("%-10s | ", col))
	}
	sb.WriteString("\n")

	// Separator
	sb.WriteString("|")
	for range res.Columns {
		sb.WriteString("------------|")
	}
	sb.WriteString("\n")

	// Rows
	if len(res.Rows) == 0 {
		sb.WriteString(" (0 rows returned)\n")
	} else {
		for _, row := range res.Rows {
			sb.WriteString("| ")
			for _, val := range row.Values {
				sb.WriteString(fmt.Sprintf("%-10v | ", val))
			}
			sb.WriteString("\n")
		}
	}

	return sb.String()
}
