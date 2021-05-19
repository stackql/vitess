package sqlparser

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
)

type SQLAstVisitor interface {
	Visit(SQLNode) error
}

type DRMAstVisitor struct {
	iDColumnName        string
	rewrittenQuery      string
	gcQueries           []string
	tablesToRewrite     map[*AliasedTableExpr]TableName
	shouldCollectTables bool
}

func NewDRMAstVisitor(iDColumnName string, shouldCollectTables bool) *DRMAstVisitor {
	return &DRMAstVisitor{
		iDColumnName:        iDColumnName,
		tablesToRewrite:     make(map[*AliasedTableExpr]TableName),
		shouldCollectTables: shouldCollectTables,
	}
}

func (v *DRMAstVisitor) GetRewrittenQuery() string {
	return v.rewrittenQuery
}

func (v *DRMAstVisitor) GetGCQueries() []string {
	return v.gcQueries
}

func (v *DRMAstVisitor) generateQIDComparison(tn TableName) *ComparisonExpr {
	return &ComparisonExpr{
		Left: &ColName{
			Name:      NewColIdent(v.iDColumnName),
			Qualifier: tn,
		},
		Right:    NewValArg([]byte(":" + v.iDColumnName)),
		Operator: EqualStr,
	}
}

func (v *DRMAstVisitor) computeQIDWhereSubTree() (Expr, error) {
	tblCount := len(v.tablesToRewrite)
	if tblCount == 0 {
		return nil, nil
	}
	if tblCount == 1 {
		for _, val := range v.tablesToRewrite {
			return v.generateQIDComparison(val), nil
		}
	}
	var retVal, curAndExpr *AndExpr
	i := 0
	for _, val := range v.tablesToRewrite {
		comparisonExpr := v.generateQIDComparison(val)
		if i == 0 {
			curAndExpr = &AndExpr{Left: comparisonExpr}
			retVal = curAndExpr
			i++
			continue
		}
		if i == tblCount {
			curAndExpr.Right = comparisonExpr
			break
		}
		newAndExpr := &AndExpr{Left: comparisonExpr}
		curAndExpr.Right = newAndExpr
		curAndExpr = newAndExpr
	}
	return retVal, nil
}

func (v *DRMAstVisitor) Visit(node SQLNode) error {
	buf := NewTrackedBuffer(nil)

	switch node := node.(type) {
	case *Select:
		var options string
		addIf := func(b bool, s string) {
			if b {
				options += s
			}
		}
		addIf(node.Distinct, DistinctStr)
		if node.Cache != nil {
			if *node.Cache {
				options += SQLCacheStr
			} else {
				options += SQLNoCacheStr
			}
		}
		addIf(node.StraightJoinHint, StraightJoinHint)
		addIf(node.SQLCalcFoundRows, SQLCalcFoundRowsStr)

		var commentStr, selectExprStr, fromStr, whereStr, groupByStr, havingStr, orderByStr, limitStr string
		if node.Comments != nil {
			node.Comments.Accept(v)
			commentStr = v.GetRewrittenQuery()
		}
		if node.SelectExprs != nil {
			node.SelectExprs.Accept(v)
			selectExprStr = v.GetRewrittenQuery()
		}
		fromVis := NewDRMAstVisitor(v.iDColumnName, true)
		if node.From != nil {
			node.From.Accept(fromVis)
			v.tablesToRewrite = fromVis.tablesToRewrite
			fromStr = fromVis.GetRewrittenQuery()
		}
		qIdSubtree, _ := fromVis.computeQIDWhereSubTree()
		if node.Where != nil {
			newWhereExpr := &AndExpr{
				Left:  node.Where.Expr,
				Right: qIdSubtree,
			}
			node.Where.Expr = newWhereExpr
		} else {
			node.Where = NewWhere(WhereStr, qIdSubtree)
		}
		node.Where.Accept(v)
		whereStr = v.GetRewrittenQuery()
		if node.GroupBy != nil {
			node.GroupBy.Accept(v)
			groupByStr = v.GetRewrittenQuery()
		}
		if node.Having != nil {
			node.Having.Accept(v)
			havingStr = v.GetRewrittenQuery()
		}
		if node.OrderBy != nil {
			node.OrderBy.Accept(v)
			orderByStr = v.GetRewrittenQuery()
		}
		if node.Limit != nil {
			node.Limit.Accept(v)
			orderByStr = v.GetRewrittenQuery()
		}
		rq := fmt.Sprintf("select %v%s%v from %v%v%v%v%v%v%s",
			commentStr, options, selectExprStr,
			fromStr, whereStr,
			groupByStr, havingStr, orderByStr,
			limitStr, node.Lock)
		v.rewrittenQuery = rq
		return nil

	case *ParenSelect:
		node.Accept(v)
		selStr := v.GetRewrittenQuery()
		rq := fmt.Sprintf("(%v)", selStr)
		v.rewrittenQuery = rq

	case *Auth:
		var infraql_opt string
		if node.SessionAuth {
			infraql_opt = "infraql "
		}
		rq := fmt.Sprintf("%sAUTH %v %s %v", infraql_opt, node.Provider, node.Type, node.KeyFilePath)
		v.rewrittenQuery = rq

	case *AuthRevoke:
		var infraql_opt string
		if node.SessionAuth {
			infraql_opt = "infraql "
		}
		rq := fmt.Sprintf("%sauth revoke %v", infraql_opt, node.Provider)
		v.rewrittenQuery = rq

	case *Sleep:
		rq := fmt.Sprintf("sleep %v", node.Duration)
		v.rewrittenQuery = rq

	case *Union:
		buf.astPrintf(node, "%v", node.FirstStatement)
		for _, us := range node.UnionSelects {
			buf.astPrintf(node, "%v", us)
		}
		buf.astPrintf(node, "%v%v%s", node.OrderBy, node.Limit, node.Lock)
		v.rewrittenQuery = buf.String()

	case *UnionSelect:
		buf.astPrintf(node, " %s %v", node.Type, node.Statement)
		v.rewrittenQuery = buf.String()

	case *Stream:
		buf.astPrintf(node, "stream %v%v from %v",
			node.Comments, node.SelectExpr, node.Table)
		v.rewrittenQuery = buf.String()

	case *Insert:
		buf.astPrintf(node, "%s %v%sinto %v%v%v %v%v",
			node.Action,
			node.Comments, node.Ignore,
			node.Table, node.Partitions, node.Columns, node.Rows, node.OnDup)
		v.rewrittenQuery = buf.String()

	case *Update:
		buf.astPrintf(node, "update %v%s%v set %v%v%v%v",
			node.Comments, node.Ignore, node.TableExprs,
			node.Exprs, node.Where, node.OrderBy, node.Limit)
		v.rewrittenQuery = buf.String()

	case *Delete:
		buf.astPrintf(node, "delete %v", node.Comments)
		if node.Targets != nil {
			buf.astPrintf(node, "%v ", node.Targets)
		}
		buf.astPrintf(node, "from %v%v%v%v%v", node.TableExprs, node.Partitions, node.Where, node.OrderBy, node.Limit)
		v.rewrittenQuery = buf.String()

	case *Set:
		buf.astPrintf(node, "set %v%v", node.Comments, node.Exprs)
		v.rewrittenQuery = buf.String()

	case *SetTransaction:
		if node.Scope == "" {
			buf.astPrintf(node, "set %vtransaction ", node.Comments)
		} else {
			buf.astPrintf(node, "set %v%s transaction ", node.Comments, node.Scope)
		}

		for i, char := range node.Characteristics {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.astPrintf(node, "%v", char)
		}
		v.rewrittenQuery = buf.String()

	case *DBDDL:
		switch node.Action {
		case CreateStr, AlterStr:
			buf.WriteString(fmt.Sprintf("%s database %s", node.Action, node.DBName))
		case DropStr:
			exists := ""
			if node.IfExists {
				exists = " if exists"
			}
			buf.WriteString(fmt.Sprintf("%s database%s %v", node.Action, exists, node.DBName))
		}
		v.rewrittenQuery = buf.String()

	case *DDL:
		switch node.Action {
		case CreateStr:
			if node.OptLike != nil {
				buf.astPrintf(node, "%s table %v %v", node.Action, node.Table, node.OptLike)
			} else if node.TableSpec != nil {
				buf.astPrintf(node, "%s table %v %v", node.Action, node.Table, node.TableSpec)
			} else {
				buf.astPrintf(node, "%s table %v", node.Action, node.Table)
			}
		case DropStr:
			exists := ""
			if node.IfExists {
				exists = " if exists"
			}
			buf.astPrintf(node, "%s table%s %v", node.Action, exists, node.FromTables)
		case RenameStr:
			buf.astPrintf(node, "%s table %v to %v", node.Action, node.FromTables[0], node.ToTables[0])
			for i := 1; i < len(node.FromTables); i++ {
				buf.astPrintf(node, ", %v to %v", node.FromTables[i], node.ToTables[i])
			}
		case AlterStr:
			if node.PartitionSpec != nil {
				buf.astPrintf(node, "%s table %v %v", node.Action, node.Table, node.PartitionSpec)
			} else {
				buf.astPrintf(node, "%s table %v", node.Action, node.Table)
			}
		case FlushStr:
			buf.astPrintf(node, "%s", node.Action)
		case CreateVindexStr:
			buf.astPrintf(node, "alter vschema create vindex %v %v", node.Table, node.VindexSpec)
		case DropVindexStr:
			buf.astPrintf(node, "alter vschema drop vindex %v", node.Table)
		case AddVschemaTableStr:
			buf.astPrintf(node, "alter vschema add table %v", node.Table)
		case DropVschemaTableStr:
			buf.astPrintf(node, "alter vschema drop table %v", node.Table)
		case AddColVindexStr:
			buf.astPrintf(node, "alter vschema on %v add vindex %v (", node.Table, node.VindexSpec.Name)
			for i, col := range node.VindexCols {
				if i != 0 {
					buf.astPrintf(node, ", %v", col)
				} else {
					buf.astPrintf(node, "%v", col)
				}
			}
			buf.astPrintf(node, ")")
			if node.VindexSpec.Type.String() != "" {
				buf.astPrintf(node, " %v", node.VindexSpec)
			}
		case DropColVindexStr:
			buf.astPrintf(node, "alter vschema on %v drop vindex %v", node.Table, node.VindexSpec.Name)
		case AddSequenceStr:
			buf.astPrintf(node, "alter vschema add sequence %v", node.Table)
		case AddAutoIncStr:
			buf.astPrintf(node, "alter vschema on %v add auto_increment %v", node.Table, node.AutoIncSpec)
		default:
			buf.astPrintf(node, "%s table %v", node.Action, node.Table)
		}
		v.rewrittenQuery = buf.String()

	case *OptLike:
		buf.astPrintf(node, "like %v", node.LikeTable)
		v.rewrittenQuery = buf.String()

	case *PartitionSpec:
		switch node.Action {
		case ReorganizeStr:
			buf.astPrintf(node, "%s %v into (", node.Action, node.Name)
			var prefix string
			for _, pd := range node.Definitions {
				buf.astPrintf(node, "%s%v", prefix, pd)
				prefix = ", "
			}
			buf.astPrintf(node, ")")
		default:
			panic("unimplemented")
		}
		v.rewrittenQuery = buf.String()

	case *PartitionDefinition:
		if !node.Maxvalue {
			buf.astPrintf(node, "partition %v values less than (%v)", node.Name, node.Limit)
		} else {
			buf.astPrintf(node, "partition %v values less than (maxvalue)", node.Name)
		}
		v.rewrittenQuery = buf.String()

	case *TableSpec:
		ts := node
		buf.astPrintf(ts, "(\n")
		for i, col := range ts.Columns {
			if i == 0 {
				buf.astPrintf(ts, "\t%v", col)
			} else {
				buf.astPrintf(ts, ",\n\t%v", col)
			}
		}
		for _, idx := range ts.Indexes {
			buf.astPrintf(ts, ",\n\t%v", idx)
		}
		for _, c := range ts.Constraints {
			buf.astPrintf(ts, ",\n\t%v", c)
		}

		buf.astPrintf(ts, "\n)%s", strings.Replace(ts.Options, ", ", ",\n  ", -1))
		v.rewrittenQuery = buf.String()

	case *ColumnDefinition:
		col := node
		buf.astPrintf(col, "%v %v", col.Name, &col.Type)
		v.rewrittenQuery = buf.String()

	// Format returns a canonical string representation of the type and all relevant options
	case *ColumnType:
		ct := node
		buf.astPrintf(ct, "%s", ct.Type)

		if ct.Length != nil && ct.Scale != nil {
			buf.astPrintf(ct, "(%v,%v)", ct.Length, ct.Scale)

		} else if ct.Length != nil {
			buf.astPrintf(ct, "(%v)", ct.Length)
		}

		if ct.EnumValues != nil {
			buf.astPrintf(ct, "(%s)", strings.Join(ct.EnumValues, ", "))
		}

		opts := make([]string, 0, 16)
		if ct.Unsigned {
			opts = append(opts, keywordStrings[UNSIGNED])
		}
		if ct.Zerofill {
			opts = append(opts, keywordStrings[ZEROFILL])
		}
		if ct.Charset != "" {
			opts = append(opts, keywordStrings[CHARACTER], keywordStrings[SET], ct.Charset)
		}
		if ct.Collate != "" {
			opts = append(opts, keywordStrings[COLLATE], ct.Collate)
		}
		if ct.NotNull {
			opts = append(opts, keywordStrings[NOT], keywordStrings[NULL])
		}
		if ct.Default != nil {
			opts = append(opts, keywordStrings[DEFAULT], String(ct.Default))
		}
		if ct.OnUpdate != nil {
			opts = append(opts, keywordStrings[ON], keywordStrings[UPDATE], String(ct.OnUpdate))
		}
		if ct.Autoincrement {
			opts = append(opts, keywordStrings[AUTO_INCREMENT])
		}
		if ct.Comment != nil {
			opts = append(opts, keywordStrings[COMMENT_KEYWORD], String(ct.Comment))
		}
		if ct.KeyOpt == colKeyPrimary {
			opts = append(opts, keywordStrings[PRIMARY], keywordStrings[KEY])
		}
		if ct.KeyOpt == colKeyUnique {
			opts = append(opts, keywordStrings[UNIQUE])
		}
		if ct.KeyOpt == colKeyUniqueKey {
			opts = append(opts, keywordStrings[UNIQUE], keywordStrings[KEY])
		}
		if ct.KeyOpt == colKeySpatialKey {
			opts = append(opts, keywordStrings[SPATIAL], keywordStrings[KEY])
		}
		if ct.KeyOpt == colKey {
			opts = append(opts, keywordStrings[KEY])
		}

		if len(opts) != 0 {
			buf.astPrintf(ct, " %s", strings.Join(opts, " "))
		}
		v.rewrittenQuery = buf.String()

	case *IndexDefinition:
		idx := node
		buf.astPrintf(idx, "%v (", idx.Info)
		for i, col := range idx.Columns {
			if i != 0 {
				buf.astPrintf(idx, ", %v", col.Column)
			} else {
				buf.astPrintf(idx, "%v", col.Column)
			}
			if col.Length != nil {
				buf.astPrintf(idx, "(%v)", col.Length)
			}
		}
		buf.astPrintf(idx, ")")

		for _, opt := range idx.Options {
			buf.astPrintf(idx, " %s", opt.Name)
			if opt.Using != "" {
				buf.astPrintf(idx, " %s", opt.Using)
			} else {
				buf.astPrintf(idx, " %v", opt.Value)
			}
		}
		v.rewrittenQuery = buf.String()

	case *IndexInfo:
		ii := node
		if ii.Primary {
			buf.astPrintf(ii, "%s", ii.Type)
		} else {
			buf.astPrintf(ii, "%s", ii.Type)
			if !ii.Name.IsEmpty() {
				buf.astPrintf(ii, " %v", ii.Name)
			}
		}
		v.rewrittenQuery = buf.String()

	case *AutoIncSpec:
		buf.astPrintf(node, "%v ", node.Column)
		buf.astPrintf(node, "using %v", node.Sequence)
		v.rewrittenQuery = buf.String()

	case *VindexSpec:
		buf.astPrintf(node, "using %v", node.Type)

		numParams := len(node.Params)
		if numParams != 0 {
			buf.astPrintf(node, " with ")
			for i, p := range node.Params {
				if i != 0 {
					buf.astPrintf(node, ", ")
				}
				buf.astPrintf(node, "%v", p)
			}
		}
		v.rewrittenQuery = buf.String()

	case VindexParam:
		buf.astPrintf(node, "%s=%s", node.Key.String(), node.Val)
		v.rewrittenQuery = buf.String()

	case *ConstraintDefinition:
		c := node
		if c.Name != "" {
			buf.astPrintf(c, "constraint %s ", c.Name)
		}
		c.Details.Format(buf)
		v.rewrittenQuery = buf.String()

	case ReferenceAction:
		a := node
		switch a {
		case Restrict:
			buf.WriteString("restrict")
		case Cascade:
			buf.WriteString("cascade")
		case NoAction:
			buf.WriteString("no action")
		case SetNull:
			buf.WriteString("set null")
		case SetDefault:
			buf.WriteString("set default")
		}
		v.rewrittenQuery = buf.String()

	case *ForeignKeyDefinition:
		f := node
		buf.astPrintf(f, "foreign key %v references %v %v", f.Source, f.ReferencedTable, f.ReferencedColumns)
		if f.OnDelete != DefaultAction {
			buf.astPrintf(f, " on delete %v", f.OnDelete)
		}
		if f.OnUpdate != DefaultAction {
			buf.astPrintf(f, " on update %v", f.OnUpdate)
		}
		v.rewrittenQuery = buf.String()

	case *Show:
		nodeType := strings.ToLower(node.Type)
		if (nodeType == "tables" || nodeType == "columns" || nodeType == "fields" || nodeType == "index" || nodeType == "keys" || nodeType == "indexes") && node.ShowTablesOpt != nil {
			opt := node.ShowTablesOpt
			if node.Extended != "" {
				buf.astPrintf(node, "show %s%s", node.Extended, nodeType)
			} else {
				buf.astPrintf(node, "show %s%s", opt.Full, nodeType)
			}
			if (nodeType == "columns" || nodeType == "fields") && node.HasOnTable() {
				buf.astPrintf(node, " from %v", node.OnTable)
			}
			if (nodeType == "index" || nodeType == "keys" || nodeType == "indexes") && node.HasOnTable() {
				buf.astPrintf(node, " from %v", node.OnTable)
			}
			if opt.DbName != "" {
				buf.astPrintf(node, " from %s", opt.DbName)
			}
			buf.astPrintf(node, "%v", opt.Filter)
			return nil
		}
		if node.Scope == "" {
			buf.astPrintf(node, "show %s", nodeType)
		} else {
			buf.astPrintf(node, "show %s %s", node.Scope, nodeType)
		}
		if node.HasOnTable() {
			buf.astPrintf(node, " on %v", node.OnTable)
		}
		if nodeType == "collation" && node.ShowCollationFilterOpt != nil {
			buf.astPrintf(node, " where %v", node.ShowCollationFilterOpt)
		}
		if nodeType == "charset" && node.ShowTablesOpt != nil {
			buf.astPrintf(node, "%v", node.ShowTablesOpt.Filter)
		}
		if node.HasTable() {
			buf.astPrintf(node, " %v", node.Table)
		}
		v.rewrittenQuery = buf.String()

	case *ShowFilter:
		if node == nil {
			return nil
		}
		if node.Like != "" {
			buf.astPrintf(node, " like '%s'", node.Like)
		} else {
			buf.astPrintf(node, " where %v", node.Filter)
		}
		v.rewrittenQuery = buf.String()

	case *Use:
		if node.DBName.v != "" {
			buf.astPrintf(node, "use %v", node.DBName)
		} else {
			buf.astPrintf(node, "use")
		}
		v.rewrittenQuery = buf.String()

	case *Commit:
		buf.WriteString("commit")
		v.rewrittenQuery = buf.String()

	case *Begin:
		buf.WriteString("begin")
		v.rewrittenQuery = buf.String()

	case *Rollback:
		buf.WriteString("rollback")
		v.rewrittenQuery = buf.String()

	case *SRollback:
		buf.astPrintf(node, "rollback to %v", node.Name)
		v.rewrittenQuery = buf.String()

	case *Savepoint:
		buf.astPrintf(node, "savepoint %v", node.Name)
		v.rewrittenQuery = buf.String()

	case *Release:
		buf.astPrintf(node, "release savepoint %v", node.Name)
		v.rewrittenQuery = buf.String()

	case *Explain:
		format := ""
		switch node.Type {
		case "": // do nothing
		case AnalyzeStr:
			format = AnalyzeStr + " "
		default:
			format = "format = " + node.Type + " "
		}
		buf.astPrintf(node, "explain %s%v", format, node.Statement)
		v.rewrittenQuery = buf.String()

	case *OtherRead:
		buf.WriteString("otherread")
		v.rewrittenQuery = buf.String()

	case *DescribeTable:
		buf.WriteString("describetable")
		v.rewrittenQuery = buf.String()

	case *OtherAdmin:
		buf.WriteString("otheradmin")
		v.rewrittenQuery = buf.String()

	case Comments:
		for _, c := range node {
			buf.astPrintf(node, "%s ", c)
		}
		v.rewrittenQuery = buf.String()

	case SelectExprs:
		var prefix string
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case *StarExpr:
		if !node.TableName.IsEmpty() {
			buf.astPrintf(node, "%v.", node.TableName)
		}
		buf.astPrintf(node, "*")
		v.rewrittenQuery = buf.String()

	case *AliasedExpr:
		buf.astPrintf(node, "%v", node.Expr)
		if !node.As.IsEmpty() {
			buf.astPrintf(node, " as %v", node.As)
		}
		v.rewrittenQuery = buf.String()

	case Nextval:
		buf.astPrintf(node, "next %v values", node.Expr)
		v.rewrittenQuery = buf.String()

	case Columns:
		if node == nil {
			return nil
		}
		prefix := "("
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		buf.WriteString(")")
		v.rewrittenQuery = buf.String()

	case Partitions:
		if node == nil {
			return nil
		}
		prefix := " partition ("
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		buf.WriteString(")")
		v.rewrittenQuery = buf.String()

	case TableExprs:
		var exprs []string
		for _, n := range node {
			n.Accept(v)
			s := v.GetRewrittenQuery()
			exprs = append(exprs, s)
		}
		v.rewrittenQuery = strings.Join(exprs, ", ")

	case *AliasedTableExpr:
		var exprStr, partitionStr string
		if node.Expr != nil {
			node.Expr.Accept(v)
			if v.shouldCollectTables {
				switch te := node.Expr.(type) {
				case TableName:
					v.tablesToRewrite[node] = te
				}
			}
			exprStr = v.GetRewrittenQuery()
		}
		if node.Partitions != nil {
			node.Partitions.Accept(v)
			partitionStr = v.GetRewrittenQuery()
		}
		q := fmt.Sprintf("%s%s", exprStr, partitionStr)
		if !node.As.IsEmpty() {
			node.As.Accept(v)
			asStr := v.GetRewrittenQuery()
			q = fmt.Sprintf("%s as %v", q, asStr)
		}
		if node.Hints != nil {
			node.Hints.Accept(v)
			// Hint node provides the space padding.
			hintStr := v.GetRewrittenQuery()
			q = fmt.Sprintf("%s%v", q, hintStr)
		}
		v.rewrittenQuery = q

	case TableNames:
		var prefix string
		for _, n := range node {
			n.Accept(v)
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case TableName:
		if node.IsEmpty() {
			return nil
		}
		buf.astPrintf(node, `"`)
		if !node.QualifierThird.IsEmpty() {
			buf.astPrintf(node, "%v.", node.QualifierThird)
		}
		if !node.QualifierSecond.IsEmpty() {
			buf.astPrintf(node, "%v.", node.QualifierSecond)
		}
		if !node.Qualifier.IsEmpty() {
			buf.astPrintf(node, "%v.", node.Qualifier)
		}
		buf.astPrintf(node, "%v", node.Name)
		buf.astPrintf(node, `"`)
		v.rewrittenQuery = buf.String()

	case *ParenTableExpr:
		buf.astPrintf(node, "(%v)", node.Exprs)
		v.rewrittenQuery = buf.String()

	case JoinCondition:
		if node.On != nil {
			buf.astPrintf(node, " on %v", node.On)
		}
		if node.Using != nil {
			buf.astPrintf(node, " using %v", node.Using)
		}
		v.rewrittenQuery = buf.String()

	case *JoinTableExpr:
		buf.astPrintf(node, "%v %s %v%v", node.LeftExpr, node.Join, node.RightExpr, node.Condition)
		v.rewrittenQuery = buf.String()

	case *IndexHints:
		buf.astPrintf(node, " %sindex ", node.Type)
		if len(node.Indexes) == 0 {
			buf.astPrintf(node, "()")
		} else {
			prefix := "("
			for _, n := range node.Indexes {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
			buf.astPrintf(node, ")")
		}
		v.rewrittenQuery = buf.String()

	case *Where:
		if node == nil || node.Expr == nil {
			return nil
		}
		buf.astPrintf(node, " %s %v", node.Type, node.Expr)
		v.rewrittenQuery = buf.String()

	case Exprs:
		var prefix string
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case *AndExpr:
		buf.astPrintf(node, "%v and %v", node.Left, node.Right)
		v.rewrittenQuery = buf.String()

	case *OrExpr:
		buf.astPrintf(node, "%v or %v", node.Left, node.Right)
		v.rewrittenQuery = buf.String()

	case *XorExpr:
		buf.astPrintf(node, "%v xor %v", node.Left, node.Right)
		v.rewrittenQuery = buf.String()

	case *NotExpr:
		buf.astPrintf(node, "not %v", node.Expr)
		v.rewrittenQuery = buf.String()

	case *ComparisonExpr:
		buf.astPrintf(node, "%v %s %v", node.Left, node.Operator, node.Right)
		if node.Escape != nil {
			buf.astPrintf(node, " escape %v", node.Escape)
		}
		v.rewrittenQuery = buf.String()

	case *RangeCond:
		buf.astPrintf(node, "%v %s %v and %v", node.Left, node.Operator, node.From, node.To)
		v.rewrittenQuery = buf.String()

	case *IsExpr:
		buf.astPrintf(node, "%v %s", node.Expr, node.Operator)
		v.rewrittenQuery = buf.String()

	case *ExistsExpr:
		buf.astPrintf(node, "exists %v", node.Subquery)
		v.rewrittenQuery = buf.String()

	case *SQLVal:
		switch node.Type {
		case StrVal:
			sqltypes.MakeTrusted(sqltypes.VarBinary, node.Val).EncodeSQL(buf)
		case IntVal, FloatVal, HexNum:
			buf.astPrintf(node, "%s", node.Val)
		case HexVal:
			buf.astPrintf(node, "X'%s'", node.Val)
		case BitVal:
			buf.astPrintf(node, "B'%s'", node.Val)
		case ValArg:
			buf.WriteArg(string(node.Val))
		default:
			panic("unexpected")
		}
		v.rewrittenQuery = buf.String()

	case *NullVal:
		buf.astPrintf(node, "null")
		v.rewrittenQuery = buf.String()

	case BoolVal:
		if node {
			buf.astPrintf(node, "true")
		} else {
			buf.astPrintf(node, "false")
		}
		v.rewrittenQuery = buf.String()

	case *ColName:
		if !node.Qualifier.IsEmpty() {
			buf.astPrintf(node, "%v.", node.Qualifier)
		}
		buf.astPrintf(node, "%v", node.Name)
		v.rewrittenQuery = buf.String()

	case ValTuple:
		buf.astPrintf(node, "(%v)", Exprs(node))
		v.rewrittenQuery = buf.String()

	case *Subquery:
		buf.astPrintf(node, "(%v)", node.Select)
		v.rewrittenQuery = buf.String()

	case ListArg:
		buf.WriteArg(string(node))
		v.rewrittenQuery = buf.String()

	case *BinaryExpr:
		buf.astPrintf(node, "%v %s %v", node.Left, node.Operator, node.Right)
		v.rewrittenQuery = buf.String()

	case *UnaryExpr:
		if _, unary := node.Expr.(*UnaryExpr); unary {
			// They have same precedence so parenthesis is not required.
			buf.astPrintf(node, "%s %v", node.Operator, node.Expr)
			return nil
		}
		buf.astPrintf(node, "%s%v", node.Operator, node.Expr)
		v.rewrittenQuery = buf.String()

	case *IntervalExpr:
		buf.astPrintf(node, "interval %v %s", node.Expr, node.Unit)
		v.rewrittenQuery = buf.String()

	case *TimestampFuncExpr:
		buf.astPrintf(node, "%s(%s, %v, %v)", node.Name, node.Unit, node.Expr1, node.Expr2)
		v.rewrittenQuery = buf.String()

	case *CurTimeFuncExpr:
		buf.astPrintf(node, "%s(%v)", node.Name.String(), node.Fsp)
		v.rewrittenQuery = buf.String()

	case *CollateExpr:
		buf.astPrintf(node, "%v collate %s", node.Expr, node.Charset)
		v.rewrittenQuery = buf.String()

	case *FuncExpr:
		var distinct string
		if node.Distinct {
			distinct = "distinct "
		}
		if !node.Qualifier.IsEmpty() {
			buf.astPrintf(node, "%v.", node.Qualifier)
		}
		// Function names should not be back-quoted even
		// if they match a reserved word, only if they contain illegal characters
		funcName := node.Name.String()

		if containEscapableChars(funcName, NoAt) {
			writeEscapedString(buf, funcName)
		} else {
			buf.WriteString(funcName)
		}
		buf.astPrintf(node, "(%s%v)", distinct, node.Exprs)
		v.rewrittenQuery = buf.String()

	case *GroupConcatExpr:
		buf.astPrintf(node, "group_concat(%s%v%v%s%v)", node.Distinct, node.Exprs, node.OrderBy, node.Separator, node.Limit)
		v.rewrittenQuery = buf.String()

	case *ValuesFuncExpr:
		buf.astPrintf(node, "values(%v)", node.Name)
		v.rewrittenQuery = buf.String()

	case *SubstrExpr:
		var val interface{}
		if node.Name != nil {
			val = node.Name
		} else {
			val = node.StrVal
		}

		if node.To == nil {
			buf.astPrintf(node, "substr(%v, %v)", val, node.From)
		} else {
			buf.astPrintf(node, "substr(%v, %v, %v)", val, node.From, node.To)
		}
		v.rewrittenQuery = buf.String()

	case *ConvertExpr:
		buf.astPrintf(node, "convert(%v, %v)", node.Expr, node.Type)
		v.rewrittenQuery = buf.String()

	case *ConvertUsingExpr:
		buf.astPrintf(node, "convert(%v using %s)", node.Expr, node.Type)
		v.rewrittenQuery = buf.String()

	case *ConvertType:
		buf.astPrintf(node, "%s", node.Type)
		if node.Length != nil {
			buf.astPrintf(node, "(%v", node.Length)
			if node.Scale != nil {
				buf.astPrintf(node, ", %v", node.Scale)
			}
			buf.astPrintf(node, ")")
		}
		if node.Charset != "" {
			buf.astPrintf(node, "%s %s", node.Operator, node.Charset)
		}
		v.rewrittenQuery = buf.String()

	case *MatchExpr:
		buf.astPrintf(node, "match(%v) against (%v%s)", node.Columns, node.Expr, node.Option)
		v.rewrittenQuery = buf.String()

	case *CaseExpr:
		buf.astPrintf(node, "case ")
		if node.Expr != nil {
			buf.astPrintf(node, "%v ", node.Expr)
		}
		for _, when := range node.Whens {
			buf.astPrintf(node, "%v ", when)
		}
		if node.Else != nil {
			buf.astPrintf(node, "else %v ", node.Else)
		}
		buf.astPrintf(node, "end")
		v.rewrittenQuery = buf.String()

	case *Default:
		buf.astPrintf(node, "default")
		if node.ColName != "" {
			buf.WriteString("(")
			formatID(buf, node.ColName, strings.ToLower(node.ColName), NoAt)
			buf.WriteString(")")
		}
		v.rewrittenQuery = buf.String()

	case *When:
		buf.astPrintf(node, "when %v then %v", node.Cond, node.Val)
		v.rewrittenQuery = buf.String()

	case GroupBy:
		prefix := " group by "
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case OrderBy:
		prefix := " order by "
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case *Order:
		if node, ok := node.Expr.(*NullVal); ok {
			buf.astPrintf(node, "%v", node)
			return nil
		}
		if node, ok := node.Expr.(*FuncExpr); ok {
			if node.Name.Lowered() == "rand" {
				buf.astPrintf(node, "%v", node)
				return nil
			}
		}

		buf.astPrintf(node, "%v %s", node.Expr, node.Direction)
		v.rewrittenQuery = buf.String()

	case *Limit:
		if node == nil {
			return nil
		}
		buf.astPrintf(node, " limit ")
		if node.Offset != nil {
			buf.astPrintf(node, "%v, ", node.Offset)
		}
		buf.astPrintf(node, "%v", node.Rowcount)
		v.rewrittenQuery = buf.String()

	case Values:
		prefix := "values "
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case UpdateExprs:
		var prefix string
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case *UpdateExpr:
		buf.astPrintf(node, "%v = %v", node.Name, node.Expr)
		v.rewrittenQuery = buf.String()

	case SetExprs:
		var prefix string
		for _, n := range node {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		v.rewrittenQuery = buf.String()

	case *SetExpr:
		if node.Scope != "" {
			buf.WriteString(node.Scope)
			buf.WriteString(" ")
		}
		// We don't have to backtick set variable names.
		switch {
		case node.Name.EqualString("charset") || node.Name.EqualString("names"):
			buf.astPrintf(node, "%s %v", node.Name.String(), node.Expr)
		case node.Name.EqualString(TransactionStr):
			sqlVal := node.Expr.(*SQLVal)
			buf.astPrintf(node, "%s %s", node.Name.String(), strings.ToLower(string(sqlVal.Val)))
		default:
			buf.astPrintf(node, "%v = %v", node.Name, node.Expr)
		}
		v.rewrittenQuery = buf.String()

	case OnDup:
		if node == nil {
			return nil
		}
		buf.astPrintf(node, " on duplicate key update %v", UpdateExprs(node))
		v.rewrittenQuery = buf.String()

	case ColIdent:
		for i := NoAt; i < node.at; i++ {
			buf.WriteByte('@')
		}
		formatID(buf, node.val, node.Lowered(), node.at)
		v.rewrittenQuery = buf.String()

	case TableIdent:
		formatID(buf, node.v, strings.ToLower(node.v), NoAt)
		v.rewrittenQuery = buf.String()

	case *IsolationLevel:
		buf.WriteString("isolation level " + node.Level)
		v.rewrittenQuery = buf.String()

	case *AccessMode:
		buf.WriteString(node.Mode)
		v.rewrittenQuery = buf.String()
	}
	return nil
}
