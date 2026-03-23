package Common.Where;

public enum MathOp {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE;

    public int calculate(int i1, int i2) {
        return switch (this) {
            case ADD -> i1 + i2;
            case SUBTRACT -> i1 - i2;
            case MULTIPLY -> i1 * i2;
            case DIVIDE ->  i1 / i2;
        };
    }


    public double calculate(double i1, double i2) {
        return switch (this) {
            case ADD -> i1 + i2;
            case SUBTRACT -> i1 - i2;
            case MULTIPLY -> i1 * i2;
            case DIVIDE ->  i1 / i2;
        };
    }

    public static MathOp getOp(String operator) throws BadOperatorException{
        return switch(operator){
            case "+" -> MathOp.ADD;
            case "-" -> MathOp.SUBTRACT;
            case "*" -> MathOp.MULTIPLY;
            case "/" -> MathOp.DIVIDE;
            default -> throw new BadOperatorException("Math Operation " + operator + " Not recognized!");
        };
    }
}
