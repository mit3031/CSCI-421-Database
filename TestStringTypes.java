import StorageManager.StorageManager;
import StorageManager.BufferManager;
import Catalog.Catalog;
import Catalog.TableSchema;
import AttributeInfo.*;
import Common.Page;
import Common.Logger;
import java.util.*;

public class TestStringTypes {
    public static void main(String[] args) {
        String[] debugActive = new String[]{"--debug"};
        Logger.initDebug(debugActive);
        
        int passedTests = 0;
        int totalTests = 0;
        
        try {
            StorageManager.initDatabase("teststringdb", 400, 10);
            StorageManager store = StorageManager.getStorageManager();
            
            System.out.println("\n========================================");
            System.out.println("STRING DATA TYPES TEST SUITE");
            System.out.println("========================================\n");

            // ============================================================
            // TEST 1: VARCHAR fields
            // ============================================================
            totalTests++;
            System.out.println("TEST 1: VARCHAR Fields");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs1 = new ArrayList<>();
                attrs1.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs1.add(new Attribute("name", new VarCharDefinition(false, false, 50), null));
                attrs1.add(new Attribute("email", new VarCharDefinition(false, false, 100), null));
                
                TableSchema table1 = new TableSchema("VarCharTable", attrs1);
                store.CreateTable(table1);
                
                List<List<Object>> rows1 = new ArrayList<>();
                rows1.add(Arrays.asList(1, "Alice", "alice@example.com"));
                rows1.add(Arrays.asList(2, "Bob", "bob@example.com"));
                rows1.add(Arrays.asList(3, "Charlie", "charlie@example.com"));
                rows1.add(Arrays.asList(4, "Diana", "diana@example.com"));
                rows1.add(Arrays.asList(5, "Eve", "eve@example.com"));
                
                store.insert("VarCharTable", rows1);
                
                Page page1 = store.selectFirstPage("VarCharTable");
                boolean correct = true;
                String[] expectedNames = {"Alice", "Bob", "Charlie", "Diana", "Eve"};
                String[] expectedEmails = {"alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com", "eve@example.com"};
                
                int idx = 0;
                while (page1 != null && correct) {
                    for (int i = 0; i < page1.getNumRows(); i++) {
                        ArrayList<Object> rec = page1.getRecord(i);
                        if (rec.size() == 3) {
                            int id = (int) rec.get(0);
                            String name = (String) rec.get(1);
                            String email = (String) rec.get(2);
                            
                            System.out.println("  Row " + (idx + 1) + ": id=" + id + ", name=\"" + name + "\", email=\"" + email + "\"");
                            
                            if (!name.equals(expectedNames[idx]) || !email.equals(expectedEmails[idx])) {
                                correct = false;
                            }
                            idx++;
                        } else {
                            correct = false;
                        }
                    }
                    if (page1.getNextPage() != -1) {
                        page1 = store.select(page1.getNextPage(), "VarCharTable");
                    } else {
                        break;
                    }
                }
                
                if (correct && idx == 5) {
                    System.out.println("✓ PASSED: VARCHAR fields stored and retrieved correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: VARCHAR data mismatch");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 2: CHAR fields (fixed length)
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 2: CHAR Fields (Fixed Length)");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs2 = new ArrayList<>();
                attrs2.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs2.add(new Attribute("code", new CharDefinition(false, false, 5), null));
                attrs2.add(new Attribute("status", new CharDefinition(false, false, 1), null));
                
                TableSchema table2 = new TableSchema("CharTable", attrs2);
                store.CreateTable(table2);
                
                List<List<Object>> rows2 = new ArrayList<>();
                rows2.add(Arrays.asList(1, "ABC12", "A"));
                rows2.add(Arrays.asList(2, "DEF34", "B"));
                rows2.add(Arrays.asList(3, "GHI56", "C"));
                rows2.add(Arrays.asList(4, "JKL78", "D"));
                
                store.insert("CharTable", rows2);
                
                Page page2 = store.selectFirstPage("CharTable");
                boolean correct = true;
                String[] expectedCodes = {"ABC12", "DEF34", "GHI56", "JKL78"};
                String[] expectedStatuses = {"A", "B", "C", "D"};
                
                int idx = 0;
                while (page2 != null && correct) {
                    for (int i = 0; i < page2.getNumRows(); i++) {
                        ArrayList<Object> rec = page2.getRecord(i);
                        if (rec.size() == 3) {
                            int id = (int) rec.get(0);
                            String code = (String) rec.get(1);
                            String status = (String) rec.get(2);
                            
                            System.out.println("  Row " + (idx + 1) + ": id=" + id + ", code=\"" + code + "\", status=\"" + status + "\"");
                            
                            if (!code.equals(expectedCodes[idx]) || !status.equals(expectedStatuses[idx])) {
                                correct = false;
                            }
                            idx++;
                        } else {
                            correct = false;
                        }
                    }
                    if (page2.getNextPage() != -1) {
                        page2 = store.select(page2.getNextPage(), "CharTable");
                    } else {
                        break;
                    }
                }
                
                if (correct && idx == 4) {
                    System.out.println("✓ PASSED: CHAR fields stored and retrieved correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: CHAR data mismatch");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 3: Mixed data types including strings
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 3: Mixed Types (INT, DOUBLE, BOOLEAN, VARCHAR, CHAR)");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs3 = new ArrayList<>();
                attrs3.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs3.add(new Attribute("name", new VarCharDefinition(false, false, 30), null));
                attrs3.add(new Attribute("score", new DoubleDefinition(false, false), null));
                attrs3.add(new Attribute("active", new BooleanDefinition(false, false), null));
                attrs3.add(new Attribute("grade", new CharDefinition(false, false, 1), null));
                
                TableSchema table3 = new TableSchema("MixedTable", attrs3);
                store.CreateTable(table3);
                
                List<List<Object>> rows3 = new ArrayList<>();
                rows3.add(Arrays.asList(1, "John", 95.5, true, "A"));
                rows3.add(Arrays.asList(2, "Jane", 87.3, false, "B"));
                rows3.add(Arrays.asList(3, "Jack", 92.0, true, "A"));
                
                store.insert("MixedTable", rows3);
                
                Page page3 = store.selectFirstPage("MixedTable");
                boolean correct = true;
                
                if (page3.getNumRows() == 3) {
                    ArrayList<Object> rec1 = page3.getRecord(0);
                    ArrayList<Object> rec2 = page3.getRecord(1);
                    ArrayList<Object> rec3 = page3.getRecord(2);
                    
                    System.out.println("  Row 1: " + rec1);
                    System.out.println("  Row 2: " + rec2);
                    System.out.println("  Row 3: " + rec3);
                    
                    correct = rec1.get(0).equals(1) && rec1.get(1).equals("John") && 
                              rec1.get(2).equals(95.5) && rec1.get(3).equals(true) && rec1.get(4).equals("A") &&
                              rec2.get(1).equals("Jane") && rec2.get(4).equals("B") &&
                              rec3.get(1).equals("Jack") && rec3.get(3).equals(true);
                } else {
                    correct = false;
                }
                
                if (correct) {
                    System.out.println("✓ PASSED: Mixed types with strings work correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Mixed type data mismatch");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 4: Variable length VARCHAR strings
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 4: Variable Length VARCHAR Strings");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs4 = new ArrayList<>();
                attrs4.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs4.add(new Attribute("description", new VarCharDefinition(false, false, 200), null));
                
                TableSchema table4 = new TableSchema("VarLengthTable", attrs4);
                store.CreateTable(table4);
                
                List<List<Object>> rows4 = new ArrayList<>();
                rows4.add(Arrays.asList(1, "Short"));
                rows4.add(Arrays.asList(2, "Medium length string"));
                rows4.add(Arrays.asList(3, "This is a much longer string that contains more characters to test variable length storage"));
                rows4.add(Arrays.asList(4, "A"));
                rows4.add(Arrays.asList(5, "Another moderately sized description"));
                
                store.insert("VarLengthTable", rows4);
                
                Page page4 = store.selectFirstPage("VarLengthTable");
                boolean correct = true;
                String[] expected = {
                    "Short", 
                    "Medium length string",
                    "This is a much longer string that contains more characters to test variable length storage",
                    "A",
                    "Another moderately sized description"
                };
                
                int idx = 0;
                while (page4 != null && correct) {
                    for (int i = 0; i < page4.getNumRows(); i++) {
                        ArrayList<Object> rec = page4.getRecord(i);
                        if (rec.size() == 2) {
                            int id = (int) rec.get(0);
                            String desc = (String) rec.get(1);
                            
                            System.out.println("  Row " + (idx + 1) + ": id=" + id + ", length=" + desc.length() + 
                                             ", desc=\"" + (desc.length() > 50 ? desc.substring(0, 47) + "..." : desc) + "\"");
                            
                            if (!desc.equals(expected[idx])) {
                                System.out.println("    MISMATCH! Expected: \"" + expected[idx] + "\"");
                                correct = false;
                            }
                            idx++;
                        } else {
                            correct = false;
                        }
                    }
                    if (page4.getNextPage() != -1) {
                        page4 = store.select(page4.getNextPage(), "VarLengthTable");
                    } else {
                        break;
                    }
                }
                
                if (correct && idx == 5) {
                    System.out.println("✓ PASSED: Variable length VARCHAR strings handled correctly");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Variable length string mismatch");
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // TEST 5: Large batch with strings (50 rows)
            // ============================================================
            totalTests++;
            System.out.println("\nTEST 5: Large Batch with Strings (50 rows)");
            System.out.println("--------------------");
            try {
                List<Attribute> attrs5 = new ArrayList<>();
                attrs5.add(new Attribute("id", new IntegerDefinition(null, true, false), null));
                attrs5.add(new Attribute("username", new VarCharDefinition(false, false, 20), null));
                attrs5.add(new Attribute("code", new CharDefinition(false, false, 3), null));
                
                TableSchema table5 = new TableSchema("LargeBatchStrings", attrs5);
                store.CreateTable(table5);
                
                List<List<Object>> rows5 = new ArrayList<>();
                for (int i = 1; i <= 50; i++) {
                    String username = "user" + i;
                    String code = String.format("%03d", i);
                    rows5.add(Arrays.asList(i, username, code));
                }
                
                store.insert("LargeBatchStrings", rows5);
                
                Page page5 = store.selectFirstPage("LargeBatchStrings");
                int totalRows = 0;
                int pageCount = 0;
                boolean correct = true;
                
                while (page5 != null) {
                    pageCount++;
                    for (int i = 0; i < page5.getNumRows(); i++) {
                        totalRows++;
                        ArrayList<Object> rec = page5.getRecord(i);
                        if (rec.size() == 3) {
                            int id = (int) rec.get(0);
                            String username = (String) rec.get(1);
                            String code = (String) rec.get(2);
                            
                            String expectedUser = "user" + id;
                            String expectedCode = String.format("%03d", id);
                            
                            if (!username.equals(expectedUser) || !code.equals(expectedCode)) {
                                System.out.println("  MISMATCH at row " + totalRows + ": expected user" + id + "/" + expectedCode + 
                                                 ", got " + username + "/" + code);
                                correct = false;
                            }
                        } else {
                            correct = false;
                        }
                    }
                    if (page5.getNextPage() != -1) {
                        page5 = store.select(page5.getNextPage(), "LargeBatchStrings");
                    } else {
                        break;
                    }
                }
                
                System.out.println("  Retrieved " + totalRows + " rows across " + pageCount + " pages");
                
                if (correct && totalRows == 50) {
                    System.out.println("✓ PASSED: 50 string records stored correctly across multiple pages");
                    passedTests++;
                } else {
                    System.out.println("✗ FAILED: Expected 50 rows, got " + totalRows);
                }
            } catch (Exception e) {
                System.out.println("✗ FAILED: " + e.getMessage());
                e.printStackTrace();
            }

            // ============================================================
            // SUMMARY
            // ============================================================
            System.out.println("\n========================================");
            System.out.println("STRING TEST SUMMARY");
            System.out.println("========================================");
            System.out.println("Passed: " + passedTests + "/" + totalTests);
            System.out.println("Failed: " + (totalTests - passedTests) + "/" + totalTests);
            
            if (passedTests == totalTests) {
                System.out.println("\n✓✓✓ ALL STRING TESTS PASSED! ✓✓✓");
                System.out.println("VARCHAR and CHAR types working correctly!");
            } else {
                System.out.println("\n⚠ SOME TESTS FAILED");
            }
            System.out.println("========================================\n");

        } catch (Exception e) {
            System.out.println("\n✗ CRITICAL ERROR: Test suite failed to initialize");
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}
