package lexer

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isAlpha(ch byte) bool {
	return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ch == '_' || ch == '@' || ch == '$' || ch == '.'
}

func isIdentifierPart(ch byte) bool {
	return isAlpha(ch) || isDigit(ch) || ch == '.' || ch == '-'
}
