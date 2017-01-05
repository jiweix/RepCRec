# RepCRec

RepCRec is an emulator of a distributed database, 
complete with multiversion concurrency control, 
deadlock detection, replication, and failure recovery. 

## Getting Started

These instructions will get you a copy of the project up and 
running on your local machine for development and testing purposes. 

### Prerequisites

Since our project is in python, you need a python installation.
```
apt-get install python
```

### Installing

You just need to clone the project.
```
git clone https://github.com/jiweix/RepCRec
cd RepCRec
```

Then you might need to install dependent packages.
```
pip install -r requirements.txt
```

Now you can run the program through
```
python src/adb.py -vv
```

## Running the tests

To run the automated tests for this system, you just need to execute
```
nosetests
```

### Adding new tests

Adding a new test case is very simple for our system. 
You just need to create a query file and an answer file
```
vi test/data/your_own_test.txt
vi test/data/your_own_test.ans
```

After that, rerun the automated tests by
```
nosetests
```

## Versioning

We use MAJOR.MINOR.PATCH for versioning. 
For the versions available, see the [tags on this repository](https://gitlab.201a.pub/csci_ga_2434_001_fa16/RepCRec/tags).

## Authors

* Liang Zhuo
* Jiwei Xu

## Description

### Data
The data consists of 20 distinct variables x1, ..., x20 (the numbers between
1 and 20 will be referred to as indexes below). There are 10 sites
numbered 1 to 10. A copy is indicated by a dot. Thus, x6.2 is the copy of
variable x6 at site 2. The odd indexed variables are at one site each (i.e.
1 + index number mod 10 ). For example, x3 and x13 are both at site 4.
Even indexed variables are at all sites. Each variable xi is initialized to the
value 10i. Each site has an independent lock table. If that site fails, the
lock table is erased.

### Algorithms to use

We implement the available copies approach to replication using two
phase locking (using read and write locks) at each site and validation at
commit time. A transaction may read a variable and later write that same
variable as well as others.

We detect deadlocks using cycle detection and abort the youngest transaction
in the cycle.

We use multiversion read consitency for read-only transactions, 
which we store the historical value of each variable at each site.

### Test Specification

We support inputing instructions from a file or the
standard input. The output goes to standard out.

Input instructions occurring in one step
begin at a new line and end with a carriage return. There may be
several operations in each step.
Some of these operations may be blocked due to conflicting locks. If an
operation for one transaction is forced to wait (because of blocking), that
does not affect the operations of other transactions.

### Input Format

The input follows the following grammar:
```
<input>     ::= <stmtlist>*
<stmtlist>  ::= epsilon | <statement> | <statement> ";" <stmtlist>
<statement> ::= "QUIT"
              | "BEGIN" "(" <namelist> ")"
              | "BEGINRO" "(" <namelist> ")"
              | "END" "(" <exprlist> ")"
              | "FAIL" "(" <exprlist> ")"
              | "RECOVER" "(" <exprlist> ")"
              | "R" "(" <expression> "," <expression> ")"
              | "W" "(" <expression> "," <expression> "," <expression> ")"
              | "DUMP" "(" ")"
              | NAME "=" <expression>
              | <expression>
<namelist>  ::= NAME | NAME "," <namelist>
<exprlist>  ::= <expression> | <expression> "," <exprlist>
<expression>::= NUMBER | NAME | "(" <expression> ")"
```

### Design

Please refer to our design document or 
[wiki](https://gitlab.201a.pub/csci_ga_2434_001_fa16/RepCRec/wikis/home).

## Usage

```
python src/adb.py -h
usage: adb.py [-h] [-v] [infile]

positional arguments:
  infile         input file

optional arguments:
  -h, --help     show this help message and exit
  -v, --verbose  increase output verbosity (e.g., -vv is more than -v)
```

## FAQ

Q: How to I see the the site used and tick executed for the returned read value?

A: Pass at lease one `-v` to the arguments, such as
`python src/adb.py -v` or `python src/adb.py -vv`

Q: I do not like to see your debug information.

A: You could redirect them to `/dev/null`. For example,
```
python src/adb.py -vv 2>/dev/null
```
should work.
