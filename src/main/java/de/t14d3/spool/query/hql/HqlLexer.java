package de.t14d3.spool.query.hql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tokenizer for the HQL-lite dialect.
 * <p>
 * Recognizes keywords, identifiers, numeric and string literals, named
 * ({@code :name}) and positional ({@code ?1}) parameter markers and the
 * punctuation/operator set used by the grammar.
 */
final class HqlLexer {

    enum Kind {
        SELECT, FROM, WHERE, ORDER, BY, ASC, DESC, LIMIT,
        JOIN, FETCH, LEFT, INNER,
        AND, OR, NOT, IN, IS, NULL, LIKE, TRUE, FALSE,
        IDENT, NUMBER, STRING, NAMED_PARAM, POSITIONAL_PARAM,
        LPAREN, RPAREN, COMMA, DOT, STAR,
        EQ, NE, LT, LE, GT, GE,
        EOF
    }

    record Token(Kind kind, String text, int position) {
    }

    private static final Map<String, Kind> KEYWORDS = Map.ofEntries(
            Map.entry("SELECT", Kind.SELECT),
            Map.entry("FROM", Kind.FROM),
            Map.entry("WHERE", Kind.WHERE),
            Map.entry("ORDER", Kind.ORDER),
            Map.entry("BY", Kind.BY),
            Map.entry("ASC", Kind.ASC),
            Map.entry("DESC", Kind.DESC),
            Map.entry("LIMIT", Kind.LIMIT),
            Map.entry("JOIN", Kind.JOIN),
            Map.entry("FETCH", Kind.FETCH),
            Map.entry("LEFT", Kind.LEFT),
            Map.entry("INNER", Kind.INNER),
            Map.entry("AND", Kind.AND),
            Map.entry("OR", Kind.OR),
            Map.entry("NOT", Kind.NOT),
            Map.entry("IN", Kind.IN),
            Map.entry("IS", Kind.IS),
            Map.entry("NULL", Kind.NULL),
            Map.entry("LIKE", Kind.LIKE),
            Map.entry("TRUE", Kind.TRUE),
            Map.entry("FALSE", Kind.FALSE)
    );

    private final String source;
    private int pos;

    HqlLexer(String source) {
        this.source = source;
    }

    List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        while (true) {
            Token token = next();
            tokens.add(token);
            if (token.kind() == Kind.EOF) {
                return tokens;
            }
        }
    }

    private Token next() {
        skipWhitespace();
        if (pos >= source.length()) {
            return new Token(Kind.EOF, "", pos);
        }

        int start = pos;
        char c = source.charAt(pos);

        if (Character.isLetter(c) || c == '_') {
            return identifier(start);
        }
        if (Character.isDigit(c)) {
            return number(start);
        }
        if (c == '\'') {
            return string(start);
        }
        if (c == ':') {
            return namedParameter(start);
        }
        if (c == '?') {
            return positionalParameter(start);
        }

        pos++;
        switch (c) {
            case '(' -> { return new Token(Kind.LPAREN, "(", start); }
            case ')' -> { return new Token(Kind.RPAREN, ")", start); }
            case ',' -> { return new Token(Kind.COMMA, ",", start); }
            case '.' -> { return new Token(Kind.DOT, ".", start); }
            case '*' -> { return new Token(Kind.STAR, "*", start); }
            case '=' -> { return new Token(Kind.EQ, "=", start); }
            case '<' -> {
                if (pos < source.length() && source.charAt(pos) == '=') {
                    pos++;
                    return new Token(Kind.LE, "<=", start);
                }
                if (pos < source.length() && source.charAt(pos) == '>') {
                    pos++;
                    return new Token(Kind.NE, "<>", start);
                }
                return new Token(Kind.LT, "<", start);
            }
            case '>' -> {
                if (pos < source.length() && source.charAt(pos) == '=') {
                    pos++;
                    return new Token(Kind.GE, ">=", start);
                }
                return new Token(Kind.GT, ">", start);
            }
            case '!' -> {
                if (pos < source.length() && source.charAt(pos) == '=') {
                    pos++;
                    return new Token(Kind.NE, "!=", start);
                }
                throw new HqlException("Unexpected character '!'", start);
            }
            default -> throw new HqlException("Unexpected character '" + c + "'", start);
        }
    }

    private Token identifier(int start) {
        while (pos < source.length()) {
            char c = source.charAt(pos);
            if (!Character.isLetterOrDigit(c) && c != '_' && c != '$') {
                break;
            }
            pos++;
        }
        String text = source.substring(start, pos);
        Kind keyword = KEYWORDS.get(text.toUpperCase(Locale.ROOT));
        return new Token(keyword != null ? keyword : Kind.IDENT, text, start);
    }

    private Token number(int start) {
        while (pos < source.length() && Character.isDigit(source.charAt(pos))) {
            pos++;
        }
        if (pos < source.length() && source.charAt(pos) == '.'
                && pos + 1 < source.length() && Character.isDigit(source.charAt(pos + 1))) {
            pos++;
            while (pos < source.length() && Character.isDigit(source.charAt(pos))) {
                pos++;
            }
        }
        return new Token(Kind.NUMBER, source.substring(start, pos), start);
    }

    private Token string(int start) {
        pos++; // opening quote
        StringBuilder value = new StringBuilder();
        while (pos < source.length()) {
            char c = source.charAt(pos);
            if (c == '\'') {
                if (pos + 1 < source.length() && source.charAt(pos + 1) == '\'') {
                    value.append('\'');
                    pos += 2;
                    continue;
                }
                pos++;
                return new Token(Kind.STRING, value.toString(), start);
            }
            value.append(c);
            pos++;
        }
        throw new HqlException("Unterminated string literal", start);
    }

    private Token namedParameter(int start) {
        pos++; // ':'
        int nameStart = pos;
        while (pos < source.length()) {
            char c = source.charAt(pos);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                break;
            }
            pos++;
        }
        if (pos == nameStart) {
            throw new HqlException("Named parameter must have a name after ':'", start);
        }
        return new Token(Kind.NAMED_PARAM, ":" + source.substring(nameStart, pos), start);
    }

    private Token positionalParameter(int start) {
        pos++; // '?'
        int digitsStart = pos;
        while (pos < source.length() && Character.isDigit(source.charAt(pos))) {
            pos++;
        }
        if (pos == digitsStart) {
            throw new HqlException("Positional parameters must be written as ?1, ?2, ...", start);
        }
        return new Token(Kind.POSITIONAL_PARAM, "?" + source.substring(digitsStart, pos), start);
    }

    private void skipWhitespace() {
        while (pos < source.length() && Character.isWhitespace(source.charAt(pos))) {
            pos++;
        }
    }
}
