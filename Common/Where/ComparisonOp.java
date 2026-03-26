package Common.Where;

public enum ComparisonOp {
    LESS_THAN,
    LESS_THAN_EQUAL,
    GREATER_THAN,
    GREATER_THAN_EQUAL,
    EQUAL,
    NOT_EQUAL,
    // for is NUll
    IS;


    public boolean compare(Integer i1, int i2) {
        return switch (this) {
            case LESS_THAN -> i1 < i2;
            case LESS_THAN_EQUAL -> i1 <= i2;
            case GREATER_THAN -> i1 > i2;
            case GREATER_THAN_EQUAL -> i1 >= i2;
            case EQUAL -> i1 == i2;
            case NOT_EQUAL -> i1 != i2;
            case IS -> i1 != null;
        };
    }

    public boolean compare(Double i1, double i2) {
        return switch (this) {
            case LESS_THAN -> i1 < i2;
            case LESS_THAN_EQUAL -> i1 <= i2;
            case GREATER_THAN -> i1 > i2;
            case GREATER_THAN_EQUAL -> i1 >= i2;
            case EQUAL -> i1 == i2;
            case NOT_EQUAL -> i1 != i2;
            case IS -> i1 != null;
        };
    }

    public boolean compare(Boolean i1, boolean i2) {
        return switch (this) {
            case EQUAL -> i1 == i2;
            case NOT_EQUAL -> i1 != i2;
            case LESS_THAN -> !i1 && i2;
            case LESS_THAN_EQUAL -> !i1 || i2;
            case GREATER_THAN -> i1 && !i2;
            case GREATER_THAN_EQUAL -> i1 || !i2;
            case IS -> i1 != null;
            default -> false;
            //is this one needed?
        };
    }

    public boolean compare(String i1, String i2) {
        return switch (this) {
            case LESS_THAN -> i1.compareTo(i2) <0;
            case LESS_THAN_EQUAL -> i1.compareTo(i2) <=0;
            case GREATER_THAN -> i1.compareTo(i2) >0;
            case GREATER_THAN_EQUAL ->i1.compareTo(i2) >=0;
            case EQUAL -> i1.equals(i2);
            case NOT_EQUAL -> ! i1.equals(i2);
            case IS -> i1 != null;
        };
    }

    public static ComparisonOp getOp(String operation) throws BadOperatorException{
        return switch(operation){
          case "<" -> ComparisonOp.LESS_THAN;
          case "<=" -> ComparisonOp.LESS_THAN_EQUAL;
          case ">" -> ComparisonOp.GREATER_THAN;
          case ">=" -> ComparisonOp.GREATER_THAN_EQUAL;
          case "=" -> ComparisonOp.EQUAL;
          case "<>" -> ComparisonOp.NOT_EQUAL;
          case "IS" -> ComparisonOp.IS;
            default -> throw new BadOperatorException("Relative operator " + operation + " Not Found!");
        };
    }

    }
