package executor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Mohammad-y-abbass/moDB/internal/planner"
	"github.com/Mohammad-y-abbass/moDB/internal/storage"
)

type ResultSet struct {
	Columns []string
	Rows    []storage.Row
	Message string
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

func (e *Executor) SaveTableSchema(name string, schema *storage.Schema) error {
	if e.Engine.ActiveDB == "" {
		return fmt.Errorf("no active database")
	}
	schemaPath := filepath.Join(e.Engine.BaseDir, e.Engine.ActiveDB, name+".json")
	file, err := os.Create(schemaPath)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(schema)
}

func (e *Executor) ReloadTables() error {
	if e.Engine.ActiveDB == "" {
		e.Tables = make(map[string]*storage.Table)
		return nil
	}

	dbDir := filepath.Join(e.Engine.BaseDir, e.Engine.ActiveDB)
	files, err := os.ReadDir(dbDir)
	if err != nil {
		return err
	}

	newTables := make(map[string]*storage.Table)
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".json" {
			tableName := strings.TrimSuffix(f.Name(), ".json")
			schemaPath := filepath.Join(dbDir, f.Name())
			dbPath := filepath.Join(dbDir, tableName+".db")

			// Check if .db file exists
			if _, err := os.Stat(dbPath); os.IsNotExist(err) {
				continue
			}

			// Load Schema
			schemaFile, err := os.Open(schemaPath)
			if err != nil {
				continue
			}
			var schema storage.Schema
			err = json.NewDecoder(schemaFile).Decode(&schema)
			schemaFile.Close()
			if err != nil {
				continue
			}

			// Initialize Table
			pager, err := storage.NewPager(dbPath)
			if err != nil {
				continue
			}
			table := storage.NewTable(pager, &schema)
			newTables[tableName] = table
		}
	}

	e.Tables = newTables
	return nil
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

		var convertedValues []interface{}
		var err error

		if len(n.Columns) > 0 {
			// Handle explicitly named columns: INSERT INTO table (c1, c2) VALUES (v1, v2)
			if len(n.Columns) != len(n.Values) {
				return ResultSet{}, fmt.Errorf("column count (%d) does not match value count (%d)", len(n.Columns), len(n.Values))
			}

			// Map values to columns
			valMap := make(map[string]string)
			for i, colName := range n.Columns {
				valMap[colName] = n.Values[i]
			}

			// Build the full row based on schema
			fullValues := make([]string, len(table.Schema.Columns))
			for i, col := range table.Schema.Columns {
				if val, exists := valMap[col.Name]; exists {
					fullValues[i] = val
				} else {
					fullValues[i] = "NULL"
				}
			}
			convertedValues, err = e.convertValues(fullValues, table.Schema)
		} else {
			// Handle positional insert: INSERT INTO table VALUES (v1, v2, v3)
			convertedValues, err = e.convertValues(n.Values, table.Schema)
		}

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
		table, ok := e.Tables[n.TableName]
		if !ok {
			return ResultSet{}, fmt.Errorf("table not found: %s", n.TableName)
		}

		rows, err := table.SelectAll()
		if err != nil {
			return ResultSet{}, err
		}

		deletedCount := 0
		for _, row := range rows {
			match := true
			if n.Where != nil {
				match, err = e.evaluateCondition(row, table.Schema, n.Where.Left, n.Where.Op, n.Where.Right)
				if err != nil {
					return ResultSet{}, err
				}
			}

			if match {
				err = table.Delete(row.PageID, row.SlotID)
				if err != nil {
					return ResultSet{}, err
				}
				deletedCount++
			}
		}
		return ResultSet{Message: fmt.Sprintf("Deleted %d rows", deletedCount)}, nil

	case *planner.UpdateNode:
		table, ok := e.Tables[n.TableName]
		if !ok {
			return ResultSet{}, fmt.Errorf("table not found: %s", n.TableName)
		}

		rows, err := table.SelectAll()
		if err != nil {
			return ResultSet{}, err
		}

		updatedCount := 0
		for _, row := range rows {
			match := true
			if n.Where != nil {
				match, err = e.evaluateCondition(row, table.Schema, n.Where.Left, n.Where.Op, n.Where.Right)
				if err != nil {
					return ResultSet{}, err
				}
			}

			if match {
				newValues := make([]interface{}, len(row.Values))
				copy(newValues, row.Values)

				for colName, newValStr := range n.Sets {
					colIdx := -1
					for i, col := range table.Schema.Columns {
						if col.Name == colName {
							colIdx = i
							break
						}
					}
					if colIdx == -1 {
						return ResultSet{}, fmt.Errorf("column not found in SET: %s", colName)
					}

					// Convert the string value from the query to the correct Go type for the column
					converted, err := e.convertSingleValue(newValStr, table.Schema.Columns[colIdx])
					if err != nil {
						return ResultSet{}, err
					}
					newValues[colIdx] = converted
				}

				err = table.Update(row.PageID, row.SlotID, newValues)
				if err != nil {
					return ResultSet{}, err
				}
				updatedCount++
			}
		}
		return ResultSet{Message: fmt.Sprintf("Updated %d rows", updatedCount)}, nil

	case *planner.CreateDatabaseNode:
		err := e.Engine.CreateDatabase(n.DatabaseName)
		if err != nil {
			return ResultSet{}, err
		}
		return ResultSet{}, nil

	case *planner.UseDatabaseNode:
		err := e.Engine.UseDatabase(n.DatabaseName)
		if err != nil {
			return ResultSet{}, err
		}
		// Refresh the table list for the new database
		err = e.ReloadTables()
		if err != nil {
			return ResultSet{}, fmt.Errorf("failed to reload tables: %w", err)
		}
		return ResultSet{}, nil

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

		if e.Engine.ActiveDB == "" {
			return ResultSet{}, fmt.Errorf("no database selected")
		}
		dbPath := fmt.Sprintf("%s/%s/%s.db", e.Engine.BaseDir, e.Engine.ActiveDB, n.TableName)
		pager, err := storage.NewPager(dbPath)
		if err != nil {
			return ResultSet{}, err
		}
		table := storage.NewTable(pager, schema)
		e.RegisterTable(n.TableName, table)

		// Persist Schema to disk
		err = e.SaveTableSchema(n.TableName, schema)
		if err != nil {
			return ResultSet{}, fmt.Errorf("failed to save schema: %w", err)
		}

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
	return e.evaluateCondition(row, schema, filter.Left, filter.Op, filter.Right)
}

func (e *Executor) evaluateCondition(row storage.Row, schema *storage.Schema, left, op, right string) (bool, error) {
	colIdx := -1
	for i, col := range schema.Columns {
		if col.Name == left {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return false, fmt.Errorf("column not found: %s", left)
	}

	val := row.Values[colIdx]

	if val == nil {
		if strings.ToUpper(right) == "NULL" {
			if op == "=" {
				return true, nil
			}
			if op == "!=" {
				return false, nil
			}
		}
		return false, nil
	}

	switch v := val.(type) {
	case int32:
		target, err := strconv.Atoi(right)
		if err != nil {
			return false, fmt.Errorf("invalid value for int32: %s", right)
		}
		rhs := int32(target)
		switch op {
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
		target, err := strconv.ParseUint(right, 10, 32)
		if err != nil {
			return false, fmt.Errorf("invalid value for uint32: %s", right)
		}
		rhs := uint32(target)
		switch op {
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
		rhs := right
		switch op {
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
		val, err := e.convertSingleValue(values[i], col)
		if err != nil {
			return nil, err
		}
		converted[i] = val
	}
	return converted, nil
}

func (e *Executor) convertSingleValue(val string, col storage.Column) (interface{}, error) {
	// Handle NULL (if we had a NULL token in values, but for now we check string "NULL")
	if strings.ToUpper(val) == "NULL" {
		if !col.IsNullable {
			return nil, fmt.Errorf("column %s cannot be NULL", col.Name)
		}
		return nil, nil
	}

	switch col.Type {
	case storage.TypeInt32:
		v, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("invalid value for column %s (INT): %s", col.Name, val)
		}
		return int32(v), nil
	case storage.TypeUint32:
		v, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid value for column %s (UINT): %s", col.Name, val)
		}
		return uint32(v), nil
	case storage.TypeFixedText:
		return val, nil
	}
	return nil, fmt.Errorf("unknown column type for conversion")
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
