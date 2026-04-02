package grammar

// Validates returns true if the SQL string matches the FoldDB grammar.
func Validates(sql string) bool {
	_, err := Parse("", []byte(sql))
	return err == nil
}
