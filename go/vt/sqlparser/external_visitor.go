package sqlparser

type SQLAstVisitor interface {
	Visit(SQLNode) error
}

type DRMAstVisitor struct {
	rewrittenQuery string
	gcQuery        string
}

func (v *DRMAstVisitor) GetRewrittenQuery() string {
	return v.rewrittenQuery
}

func (v *DRMAstVisitor) GetGCQuery() string {
	return v.gcQuery
}

func (v *DRMAstVisitor) Visit(node SQLNode) error {
	switch node := node.(type) {
	case *Select:
		buf := NewTrackedBuffer(nil)
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

		buf.astPrintf(node, "select %v%s%v from %v%v%v%v%v%v%s",
			node.Comments, options, node.SelectExprs,
			node.From, node.Where,
			node.GroupBy, node.Having, node.OrderBy,
			node.Limit, node.Lock)
		v.rewrittenQuery = buf.String()
		v.gcQuery = ""
		return nil
	default:
		v.rewrittenQuery = String(node)
		v.gcQuery = ""
	}
	return nil
}

func NewDRMAstVisitor() *DRMAstVisitor {
	return &DRMAstVisitor{}
}
