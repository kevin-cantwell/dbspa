package grammar

// Validates returns true if the SQL string matches the DBSPA grammar.
func Validates(sql string) bool {
	_, err := Parse("", []byte(sql))
	return err == nil
}
