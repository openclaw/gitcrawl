package store

import (
	"fmt"
	"strings"
)

func portableTriggerDroppedDependency(triggerSQL string, dropped map[string]bool) (string, error) {
	tokens, err := tokenizePortableSQL(triggerSQL)
	if err != nil {
		return "", err
	}
	begin := -1
	start := -1
	for index, token := range tokens {
		if !token.quoted && strings.EqualFold(token.value, "BEGIN") {
			begin = index + 1
			break
		}
		if !token.quoted && strings.EqualFold(token.value, "WHEN") {
			start = index + 1
		}
	}
	if begin < 0 {
		return "", fmt.Errorf("unrecognized trigger body")
	}
	if start >= 0 {
		if dependency := portableDroppedTableDependency(tokens[start:begin-1], dropped); dependency != "" {
			return dependency, nil
		}
	}
	for _, statement := range portableSQLStatements(tokens[begin:]) {
		if dependency := portableDroppedTableDependency(statement, dropped); dependency != "" {
			return dependency, nil
		}
	}
	return "", nil
}

func portableDroppedTableDependency(tokens []portableSQLToken, dropped map[string]bool) string {
	// CTE resolution is deliberately conservative: a removed-table name in a
	// real table position is rejected even when a CTE might shadow it.
	return portableDroppedTableDependencyInList(tokens, dropped, false)
}

func portableDroppedTableDependencyInList(tokens []portableSQLToken, dropped map[string]bool, initialFromList bool) string {
	wantTable := initialFromList
	fromList := initialFromList
	deleteTarget := false
	for index := 0; index < len(tokens); index++ {
		token := tokens[index]
		if wantTable {
			if portableSQLKeyword(token, "OR") && index+1 < len(tokens) {
				index++
				continue
			}
			if token.value == "(" {
				end := portableSQLMatchingParenthesis(tokens, index)
				if dependency := portableDroppedTableDependencyInList(tokens[index+1:end], dropped, true); dependency != "" {
					return dependency
				}
				wantTable = false
				index = end
				continue
			}
			name, consumed := portableSQLTableIdentifier(tokens[index:])
			wantTable = false
			if consumed > 0 {
				index += consumed - 1
			}
			if dropped[strings.ToLower(name)] {
				return name
			}
			continue
		}
		if token.value == "(" {
			end := portableSQLMatchingParenthesis(tokens, index)
			if dependency := portableDroppedTableDependencyInList(tokens[index+1:end], dropped, false); dependency != "" {
				return dependency
			}
			index = end
			continue
		}
		switch {
		case portableSQLKeyword(token, "DELETE"):
			deleteTarget = true
			fromList = false
		case portableSQLKeyword(token, "FROM"):
			wantTable = true
			fromList = !deleteTarget
			deleteTarget = false
		case portableSQLKeyword(token, "JOIN"):
			wantTable = true
		case portableSQLKeyword(token, "UPDATE"),
			portableSQLKeyword(token, "INTO"),
			portableSQLKeyword(token, "REFERENCES"):
			wantTable = true
		case fromList && token.value == ",":
			wantTable = true
		case portableSQLKeyword(token, "WHERE"),
			portableSQLKeyword(token, "GROUP"),
			portableSQLKeyword(token, "HAVING"),
			portableSQLKeyword(token, "ORDER"),
			portableSQLKeyword(token, "LIMIT"),
			portableSQLKeyword(token, "RETURNING"),
			portableSQLKeyword(token, "SET"),
			portableSQLKeyword(token, "VALUES"),
			portableSQLKeyword(token, "UNION"),
			portableSQLKeyword(token, "EXCEPT"),
			portableSQLKeyword(token, "INTERSECT"):
			fromList = false
		}
	}
	return ""
}

func portableSQLMatchingParenthesis(tokens []portableSQLToken, start int) int {
	depth := 0
	for index := start; index < len(tokens); index++ {
		switch tokens[index].value {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return len(tokens) - 1
}

func portableSQLStatements(tokens []portableSQLToken) [][]portableSQLToken {
	var statements [][]portableSQLToken
	start := 0
	depth := 0
	for index, token := range tokens {
		switch token.value {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ";":
			if depth == 0 {
				if index > start {
					statements = append(statements, tokens[start:index])
				}
				start = index + 1
			}
		}
	}
	if start < len(tokens) {
		statements = append(statements, tokens[start:])
	}
	return statements
}

func portableSQLTableIdentifier(tokens []portableSQLToken) (string, int) {
	if len(tokens) == 0 || tokens[0].value == "(" {
		return "", 0
	}
	if len(tokens) >= 3 && tokens[1].value == "." {
		return tokens[2].value, 3
	}
	return tokens[0].value, 1
}

func portableSQLKeyword(token portableSQLToken, keyword string) bool {
	return !token.quoted && strings.EqualFold(token.value, keyword)
}

type portableSQLToken struct {
	value   string
	quoted  bool
	literal bool
}

func portableTriggerUpdatesThreads(triggerSQL string) (bool, error) {
	tokens, err := tokenizePortableSQL(triggerSQL)
	if err != nil {
		return false, err
	}
	headerEnd := len(tokens)
	for index, token := range tokens {
		if !token.quoted && strings.EqualFold(token.value, "BEGIN") {
			headerEnd = index
			break
		}
	}
	header := tokens[:headerEnd]
	for index, token := range header {
		if token.quoted || !strings.EqualFold(token.value, "ON") {
			continue
		}
		targetIndex := index + 1
		if targetIndex+2 < len(header) && header[targetIndex+1].value == "." {
			targetIndex += 2
		}
		if targetIndex >= len(header) || !strings.EqualFold(header[targetIndex].value, "threads") {
			continue
		}
		for _, prior := range header[:index] {
			if !prior.quoted && strings.EqualFold(prior.value, "UPDATE") {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("unrecognized trigger target")
}

func tokenizePortableSQL(value string) ([]portableSQLToken, error) {
	var tokens []portableSQLToken
	for index := 0; index < len(value); {
		switch {
		case isPortableSQLSpace(value[index]):
			index++
		case index+1 < len(value) && value[index:index+2] == "--":
			index += 2
			for index < len(value) && value[index] != '\n' {
				index++
			}
		case index+1 < len(value) && value[index:index+2] == "/*":
			end := strings.Index(value[index+2:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("unterminated SQL comment")
			}
			index += end + 4
		case value[index] == '\'' || value[index] == '"' || value[index] == '`':
			quote := value[index]
			start := index + 1
			index++
			var text strings.Builder
			for {
				if index >= len(value) {
					return nil, fmt.Errorf("unterminated SQL quote")
				}
				if value[index] == quote {
					if index+1 < len(value) && value[index+1] == quote {
						text.WriteString(value[start:index])
						text.WriteByte(quote)
						index += 2
						start = index
						continue
					}
					text.WriteString(value[start:index])
					index++
					break
				}
				index++
			}
			tokens = append(tokens, portableSQLToken{value: text.String(), quoted: true, literal: quote == '\''})
		case value[index] == '[':
			end := strings.IndexByte(value[index+1:], ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated SQL bracket identifier")
			}
			tokens = append(tokens, portableSQLToken{value: value[index+1 : index+1+end], quoted: true})
			index += end + 2
		case isPortableSQLWord(value[index]):
			start := index
			for index < len(value) && isPortableSQLWord(value[index]) {
				index++
			}
			tokens = append(tokens, portableSQLToken{value: value[start:index]})
		default:
			tokens = append(tokens, portableSQLToken{value: value[index : index+1]})
			index++
		}
	}
	return tokens, nil
}

func isPortableSQLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n' || value == '\f'
}

func isPortableSQLWord(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '$'
}
