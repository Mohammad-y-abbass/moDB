package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Mohammad-y-abbass/moDB/internal/planner"
	"github.com/Mohammad-y-abbass/moDB/internal/storage"
)

// checkReferencingChildren checks every other table in the current database for
// columns that REFERENCES this (parent) table. If any child row holds the
// parent's PK value, the delete is rejected.
func (e *Executor) checkReferencingChildren(parent *storage.Table, parentRow storage.Row) error {
	// Find the PK column index in the parent
	parentPKIdx := -1
	for i, col := range parent.Schema.Columns {
		if col.IsPrimaryKey {
			parentPKIdx = i
			break
		}
	}
	if parentPKIdx == -1 {
		// No PK defined – skip referential check
		return nil
	}
	parentPKVal := parentRow.Values[parentPKIdx]

	for _, childTable := range e.Tables {
		if childTable == parent {
			continue
		}
		for childColIdx, col := range childTable.Schema.Columns {
			if col.References == nil {
				continue
			}
			// Find which parent table this FK points to
			refParent, ok := e.Tables[col.References.Table]
			if !ok || refParent != parent {
				continue
			}
			// This child column references our parent – scan for matching rows
			childRows, err := childTable.SelectAll()
			if err != nil {
				return err
			}
			for _, cr := range childRows {
				if fmt.Sprintf("%v", cr.Values[childColIdx]) == fmt.Sprintf("%v", parentPKVal) {
					return fmt.Errorf("FK constraint violation: cannot delete parent row, child record exists in '%s.%s'",
						col.References.Table, col.Name)
				}
			}
		}
	}
	return nil
}

// ── Join execution ───────────────────────────────────────────────────────────

// resolveQualifiedCol splits "table.col" into (tableName, colName).
// If there is no dot the full string is returned as colName with an empty tableName.
func resolveQualifiedCol(qualified string) (table, col string) {
	parts := strings.SplitN(qualified, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", qualified
}

// executeJoin performs a Nested Loop inner join between two tables.
func (e *Executor) executeJoin(n *planner.JoinNode) (ResultSet, error) {
	leftTable, ok := e.Tables[n.Left.TableName]
	if !ok {
		return ResultSet{}, fmt.Errorf("table not found: %s", n.Left.TableName)
	}
	rightTable, ok := e.Tables[n.Right.TableName]
	if !ok {
		return ResultSet{}, fmt.Errorf("table not found: %s", n.Right.TableName)
	}

	leftRows, err := leftTable.SelectAll()
	if err != nil {
		return ResultSet{}, err
	}
	rightRows, err := rightTable.SelectAll()
	if err != nil {
		return ResultSet{}, err
	}

	// Resolve ON-clause column indices -----------------------------------------
	_, leftColName := resolveQualifiedCol(n.LeftKey)
	_, rightColName := resolveQualifiedCol(n.RightKey)

	leftKeyIdx := -1
	for i, c := range leftTable.Schema.Columns {
		if c.Name == leftColName {
			leftKeyIdx = i
			break
		}
	}
	rightKeyIdx := -1
	for i, c := range rightTable.Schema.Columns {
		if c.Name == rightColName {
			rightKeyIdx = i
			break
		}
	}
	if leftKeyIdx == -1 {
		return ResultSet{}, fmt.Errorf("join key column '%s' not found in table '%s'", leftColName, n.Left.TableName)
	}
	if rightKeyIdx == -1 {
		return ResultSet{}, fmt.Errorf("join key column '%s' not found in table '%s'", rightColName, n.Right.TableName)
	}

	// Build combined column names (qualified as "table.col") -------------------
	var combinedCols []string
	for _, c := range leftTable.Schema.Columns {
		combinedCols = append(combinedCols, n.Left.TableName+"."+c.Name)
	}
	for _, c := range rightTable.Schema.Columns {
		combinedCols = append(combinedCols, n.Right.TableName+"."+c.Name)
	}

	// Nested Loop Join ---------------------------------------------------------
	var joinedRows []storage.Row
	for _, lr := range leftRows {
		for _, rr := range rightRows {
			lv := fmt.Sprintf("%v", lr.Values[leftKeyIdx])
			rv := fmt.Sprintf("%v", rr.Values[rightKeyIdx])
			if lv != rv {
				continue
			}
			combined := storage.Row{
				Values: append(append([]interface{}{}, lr.Values...), rr.Values...),
			}
			joinedRows = append(joinedRows, combined)
		}
	}

	// Apply WHERE filter on combined rows if present ---------------------------
	if n.Where != nil {
		// Resolve the filter column in the combined schema
		filterColIdx := -1
		filterCol := n.Where.Left
		for i, cName := range combinedCols {
			// Match both "table.col" and bare "col"
			if cName == filterCol || strings.HasSuffix(cName, "."+filterCol) {
				filterColIdx = i
				break
			}
		}
		if filterColIdx == -1 {
			return ResultSet{}, fmt.Errorf("WHERE column '%s' not found in join result", filterCol)
		}

		// Build a one-column schema-compatible filter using a dummy schema
		var filtered []storage.Row
		for _, row := range joinedRows {
			val := row.Values[filterColIdx]
			match, err2 := e.compareValue(val, n.Where.Op, n.Where.Right)
			if err2 != nil {
				return ResultSet{}, err2
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		joinedRows = filtered
	}

	// Apply projection ---------------------------------------------------------
	if len(n.Columns) > 0 && n.Columns[0] != "*" {
		colIndices := make([]int, 0, len(n.Columns))
		for _, want := range n.Columns {
			found := false
			for i, cName := range combinedCols {
				if cName == want || strings.HasSuffix(cName, "."+want) {
					colIndices = append(colIndices, i)
					found = true
					break
				}
			}
			if !found {
				return ResultSet{}, fmt.Errorf("column '%s' not found in join result", want)
			}
		}
		var projected []storage.Row
		for _, row := range joinedRows {
			newVals := make([]interface{}, len(colIndices))
			for i, idx := range colIndices {
				newVals[i] = row.Values[idx]
			}
			projected = append(projected, storage.Row{Values: newVals})
		}
		return ResultSet{Columns: n.Columns, Rows: projected}, nil
	}

	return ResultSet{Columns: combinedCols, Rows: joinedRows}, nil
}

// compareValue compares an interface{} cell value against a string literal
// using the given operator. Used in the join WHERE path.
func (e *Executor) compareValue(val interface{}, op, right string) (bool, error) {
	if val == nil {
		return false, nil
	}
	switch v := val.(type) {
	case int32:
		target, err := strconv.Atoi(right)
		if err != nil {
			return false, fmt.Errorf("invalid int value in WHERE: %s", right)
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
	case string:
		switch op {
		case "=":
			return v == right, nil
		case "!=":
			return v != right, nil
		case ">":
			return v > right, nil
		case "<":
			return v < right, nil
		case ">=":
			return v >= right, nil
		case "<=":
			return v <= right, nil
		}
	}
	return false, nil
}
