package Common.Where;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BuildTree {

    private static Integer precedence(String symbol) {
        if (symbol.contains(">") || symbol.contains("<") || symbol.equals("=") || symbol.equals("<>") || symbol.equals("IS")) {
            return 3;
        } else if (symbol.equals("AND")) {
            return 2;
        } else {
            return 1;
        }
    }

    public static IWhereOp buildTree(String whereSection) {
        String[] sections = whereSection.split(" ");
        Stack<String> operatorStack = new Stack<String>();
        Stack<String> operands = new Stack<String>();
        Stack<IWhereOp> whereNodes = new Stack<IWhereOp>();
        for (int i = 0; i < sections.length; i++) {
            if (!sections[i].contains(">") && !sections[i].contains("<") && !Objects.equals(sections[i], "=")
                    && !Objects.equals(sections[i], "<>") && !Objects.equals(sections[i], "AND")
                    && !Objects.equals(sections[i], "OR") && !sections[i].equals("IS")) {
                operands.add(sections[i]);
            } else {
                while(!operatorStack.isEmpty() && (precedence(operatorStack.peek()) >= precedence(sections[i]))) {
                    String currentOp = operatorStack.pop();
                    if (currentOp.contains(">") || currentOp.contains("<") || Objects.equals(currentOp, "=")
                            || Objects.equals(currentOp, "<>")) {
                        String firstOp = operands.pop();
                        String secOp = operands.pop();
                        Pattern pattern = Pattern.compile("[a-zA-Z]");
                        Matcher matcher = pattern.matcher(firstOp);
                        IOperandNode right;
                        IOperandNode left;

                        if (secOp.equals("+") || secOp.equals("-") || secOp.equals("*") || secOp.equals("/")) {
                            String rightOp = firstOp;
                            String mathOp = secOp;
                            String leftOp = operands.pop();
                            matcher = pattern.matcher(rightOp);
                            IOperandNode mathLeft;
                            IOperandNode mathRight;
                            if (matcher.find()) {
                                mathRight = new AttributeNode(rightOp);
                            } else {
                                mathRight = new ValueNode(rightOp);
                            }
                            matcher = pattern.matcher(leftOp);
                            if (matcher.find()) {
                                mathLeft = new AttributeNode(leftOp);
                            } else {
                                mathLeft = new ValueNode(leftOp);
                            }
                            right = new MathOpNode(mathLeft, mathRight, mathOp);
                        }
                        else if (matcher.find()) {
                            right = new AttributeNode(firstOp);
                        }else {
                            right = new ValueNode(firstOp);
                        }

                        if (secOp.equals("+") || secOp.equals("-") || secOp.equals("*") || secOp.equals("/")) {
                            secOp = operands.pop();
                        }
                        matcher = pattern.matcher(secOp);

                        if (operands.contains("+") || operands.contains("-") || operands.contains("*") || operands.contains("/")) {
                            String rightOp;
                            rightOp = secOp;
                            String mathOp = operands.pop();
                            String leftOp = operands.pop();
                            matcher = pattern.matcher(rightOp);
                            IOperandNode mathLeft;
                            IOperandNode mathRight;
                            if (matcher.find()) {
                                mathRight = new AttributeNode(rightOp);
                            } else {
                                mathRight = new ValueNode(rightOp);
                            }
                            matcher = pattern.matcher(leftOp);
                            if (matcher.find()) {
                                mathLeft = new AttributeNode(leftOp);
                            } else {
                                mathLeft = new ValueNode(leftOp);
                            }
                            left = new MathOpNode(mathLeft, mathRight, mathOp);
                        } else if (matcher.find()){
                            left = new AttributeNode(secOp);
                        } else {
                            left = new ValueNode(secOp);
                        }
                        whereNodes.push(new RelOpNode(left, right, currentOp));
                    } else if(currentOp.equals("IS")) {
                        String firstOp = operands.pop(); // if not null throw error
                        String secOp = operands.pop();
                        Pattern pattern = Pattern.compile("[a-zA-Z]");
                        IOperandNode right;
                        IOperandNode left;
                        right = new ValueNode(firstOp); // confirm this is valid based on finished valueNode code

                        Matcher matcher = pattern.matcher(secOp);
                        if (operands.contains("+") || operands.contains("-") || operands.contains("*") || operands.contains("/")) {
                            String rightOp;
                            rightOp = secOp;
                            String mathOp = operands.pop();
                            String leftOp = operands.pop();
                            matcher = pattern.matcher(rightOp);
                            IOperandNode mathLeft;
                            IOperandNode mathRight;
                            if (matcher.find()) {
                                mathRight = new AttributeNode(rightOp);
                            } else {
                                mathRight = new ValueNode(rightOp);
                            }
                            matcher = pattern.matcher(leftOp);
                            if (matcher.find()) {
                                mathLeft = new AttributeNode(leftOp);
                            } else {
                                mathLeft = new ValueNode(leftOp);
                            }
                            left = new MathOpNode(mathLeft, mathRight, mathOp);
                        } else if (matcher.find()) {
                            left = new AttributeNode(secOp);
                        } else {
                            left = new ValueNode(secOp);
                        }
                        whereNodes.push(new RelOpNode(left, right, "="));
                    }else if (currentOp.equals("AND")) {
                        IWhereOp right = whereNodes.pop();
                        IWhereOp left = whereNodes.pop();
                        whereNodes.push(new AndNode(left, right));
                    } else {
                        IWhereOp right = whereNodes.pop();
                        IWhereOp left = whereNodes.pop();
                        whereNodes.push(new OrNode(left, right));
                    }
                }
                operatorStack.push(sections[i]);
            }
        }
        while(!operatorStack.isEmpty()) {
            String currentOp = operatorStack.pop();
            if (currentOp.contains(">") || currentOp.contains("<") || Objects.equals(currentOp, "=")
                    || Objects.equals(currentOp, "<>")) {
                String firstOp = operands.pop();
                String secOp = operands.pop();
                Pattern pattern = Pattern.compile("[a-zA-Z]");
                Matcher matcher = pattern.matcher(firstOp);
                IOperandNode right;
                IOperandNode left;

                if (secOp.equals("+") || secOp.equals("-") || secOp.equals("*") || secOp.equals("/")) {
                    String rightOp = firstOp;
                    String mathOp = secOp;
                    String leftOp = operands.pop();
                    matcher = pattern.matcher(rightOp);
                    IOperandNode mathLeft;
                    IOperandNode mathRight;
                    if (matcher.find()) {
                        mathRight = new AttributeNode(rightOp);
                    } else {
                        mathRight = new ValueNode(rightOp);
                    }
                    matcher = pattern.matcher(leftOp);
                    if (matcher.find()) {
                        mathLeft = new AttributeNode(leftOp);
                    } else {
                        mathLeft = new ValueNode(leftOp);
                    }
                    right = new MathOpNode(mathLeft, mathRight, mathOp);
                }
                else if (matcher.find()) {
                    right = new AttributeNode(firstOp);
                }else {
                    right = new ValueNode(firstOp);
                }

                if (secOp.equals("+") || secOp.equals("-") || secOp.equals("*") || secOp.equals("/")) {
                    secOp = operands.pop();
                }
                matcher = pattern.matcher(secOp);

                if (operands.contains("+") || operands.contains("-") || operands.contains("*") || operands.contains("/")) {
                    String rightOp;

                    rightOp = secOp;
                    String mathOp = operands.pop();
                    String leftOp = operands.pop();
                    matcher = pattern.matcher(rightOp);
                    IOperandNode mathLeft;
                    IOperandNode mathRight;
                    if (matcher.find()) {
                        mathRight = new AttributeNode(rightOp);
                    } else {
                        mathRight = new ValueNode(rightOp);
                    }
                    matcher = pattern.matcher(leftOp);
                    if (matcher.find()) {
                        mathLeft = new AttributeNode(leftOp);
                    } else {
                        mathLeft = new ValueNode(leftOp);
                    }
                    left = new MathOpNode(mathLeft, mathRight, mathOp);
                } else if (matcher.find()){
                    left = new AttributeNode(secOp);
                } else {
                    left = new ValueNode(secOp);
                }
                whereNodes.push(new RelOpNode(left, right, currentOp));
            } else if(currentOp.equals("IS")) {
                String firstOp = operands.pop(); // if not null throw error
                String secOp = operands.pop();
                Pattern pattern = Pattern.compile("[a-zA-Z]");
                IOperandNode right;
                IOperandNode left;
                right = new ValueNode(firstOp); // confirm this is valid based on finished valueNode code

                Matcher matcher = pattern.matcher(secOp);
                if (operands.contains("+") || operands.contains("-") || operands.contains("*") || operands.contains("/")) {
                    String rightOp;
                    rightOp = secOp;
                    String mathOp = operands.pop();
                    String leftOp = operands.pop();
                    matcher = pattern.matcher(rightOp);
                    IOperandNode mathLeft;
                    IOperandNode mathRight;
                    if (matcher.find()) {
                        mathRight = new AttributeNode(rightOp);
                    } else {
                        mathRight = new ValueNode(rightOp);
                    }
                    matcher = pattern.matcher(leftOp);
                    if (matcher.find()) {
                        mathLeft = new AttributeNode(leftOp);
                    } else {
                        mathLeft = new ValueNode(leftOp);
                    }
                    left = new MathOpNode(mathLeft, mathRight, mathOp);
                } else if (matcher.find()) {
                    left = new AttributeNode(secOp);
                } else {
                    left = new ValueNode(secOp);
                }
                whereNodes.push(new RelOpNode(left, right, "="));
            }else if (currentOp.equals("AND")) {
                IWhereOp right = whereNodes.pop();
                IWhereOp left = whereNodes.pop();
                whereNodes.push(new AndNode(left, right));
            } else {
                IWhereOp right = whereNodes.pop();
                IWhereOp left = whereNodes.pop();
                whereNodes.push(new OrNode(left, right));
            }
        }
        return whereNodes.pop();
    }

    public static void main(String[] args) {
        //buildTree("1438745 = 5");
        //buildTree("a = 5 AND b > 3");
        //buildTree("a = 5 OR c <= 6");
        //buildTree("x IS NULL");
        //buildTree("20 * x IS NULL");
        //buildTree("2 * x = 500 + y");
        //buildTree("2 * x = y + 500");
        //buildTree("hi = 43834 / h");
        //buildTree(" hi + j = 43294309");
        //buildTree("a > 2 AND b = 3 OR x IS NULL");
        //buildTree("a = 3 OR b = 3 AND x = 5");
        buildTree("a + b = 3 / 200 OR b + c = 3 AND x = 5 AND name IS NULL OR 7 > 10");
    }
}
