# JottQL Database Management System 

## Project Team
| Name                 |
|:---------------------|
| **Alex Lee**         |
| **Antonio Bicknell** | 
| **Analucia Macias Marin**        | 
| **Marvynn Talusan**  | 
| **Logan Maleady**        |

---

## Project Overview
This project is a from scratch implementation of a simplified relational database system written in Java. Instead of using an existing database engine, we built the core pieces ourselves to understand how databases actually work under the hood

---

## Environment Requirements

### Required Software
- Java **21** (JDK 21)
- Git

Verify Java version:

```bash
java --version
javac --version
```

Both commands must report **Java 21**

If multiple Java versions are installed on the CS machine, load Java 21:

```bash
module load java/21
```



## Building the Project (Compilation)

This project does **not** use Maven or Gradle. It must be compiled manually

From the project root directory:

```bash
find . -name "*.java" > sources.txt
javac --release 21 @sources.txt
```

This:
- Locates all Java source files in subdirectories
- Compiles them using Java 21
- Generates `.class` files in their respective directories

If compilation succeeds, no errors will be displayed

---

## Running the Program

The main class is:

```
JottQL
```

Run the program from the project root directory:

```bash
java JottQL <dbLocation> <pageSize> <bufferSize> <indexing> [debug]
```

### Required Arguments

The program requires **exactly 4 or 5 arguments**:

| Argument | Description                                    |
|----------|------------------------------------------------|
| `dbLocation` | Directory where database files will be stored  |
| `pageSize` | Size of each page in bytes (integer)           |
| `bufferSize` | Number of pages the buffer can hold (integer)  |
| `indexing` | `true` or `false`                              |
| `debug` | (Optional - Enables logging) `true` or `false` |

If the number of arguments is incorrect, the program will display a usage message and exit

### Example

```bash
java JottQL myDatabase 4096 10 false
```

With debug mode enabled:

```bash
java JottQL myDatabase 4096 10 false true
```

---

## Using the Database

After launching the program, commands are entered through standard input

All commands:
- May span multiple lines
- Must end with a semicolon (`;`), except for ```<QUIT>```
- Keywords must match required capitalization

Example:

```
CREATE TABLE myTable ( x INTEGER PRIMARYKEY );
INSERT myTable VALUES ( 1, 2, 3 );
SELECT * FROM myTable;
<QUIT>;
```

---

## Shutting Down the Database

To properly shut down the database, enter:

```
<QUIT>
```

This ensures:
- Buffers are flushed
- Files are safely written
- Storage is properly closed

Do **NOT** terminate using `Ctrl+C`, as this may result in data loss