package sqlparser

type SQLAstVisitor interface {
	Visit(SQLNode) error
}
