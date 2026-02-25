package AttributeInfo;

//    INTEGER — Standard integer (non-negative for parsing simplicity)
//• DOUBLE — Standard double-precision floating point (non-negative)
//• BOOLEAN — Accepts True or False. Must be stored as the language's boolean type, not
//    as strings.
//            • CHAR(N) — Fixed-length string of exactly N characters
//• VARCHAR(N) — Variable-length string with maximum N characters. Must be stored at its
//    actual size, not padded to maximum.

public enum AttributeTypeEnum {
    INTEGER,
    DOUBLE,
    CHAR,
    VARCHAR,
    BOOLEAN
}
