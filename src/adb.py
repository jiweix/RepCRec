# -----------------------------------------------------------------------------
# http://www.dabeaz.com/ply/example.html
# adb.py
#
# A simple calculator with variables -- all in one file.
# -----------------------------------------------------------------------------

import argparse
import logging
from transaction import ReadWriteTransaction, ReadOnlyTransaction
from transaction_manager import TransactionManager
from data_item import DataItem
import site1 as site

reserved = {
    'begin': 'BEGIN',
    'beginro': 'BEGIN_READONLY',
    'end': 'END',
    'dump': 'DUMP',
    'fail': 'FAIL',
    'recover': 'RECOVER',
    'r': 'READ',
    'w': 'WRITE',
    'quit': 'QUIT',
}

tokens = [
    'NAME', 'NUMBER',
    'PLUS', 'MINUS', 'TIMES', 'DIVIDE', 'EQUALS',
    'LPAREN', 'RPAREN',
    'COMMA', 'SEMICOLON',
] + list(reserved.values())

# Tokens

t_PLUS = r'\+'
t_MINUS = r'-'
t_TIMES = r'\*'
t_DIVIDE = r'/'
t_EQUALS = r'='
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_COMMA = r','
t_SEMICOLON = r';'


def t_NAME(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    t.type = reserved.get(t.value.lower(), 'NAME')
    return t


def t_NUMBER(t):
    r'\d+'
    try:
        t.value = int(t.value)
    except ValueError:
        print("Integer value too large %d", t.value)
        t.value = 0
    return t


# Ignored characters
t_ignore = " \t"


def t_COMMENT(t):
    r'//.*'
    pass  # No return value. Token discarded


def t_newline(t):
    r'\n+'
    t.lexer.lineno += t.value.count("\n")


def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


# Build the lexer
import ply.lex as lex

lexer = lex.lex()

# Parsing rules

precedence = (
    ('left', 'PLUS', 'MINUS'),
    ('left', 'TIMES', 'DIVIDE'),
    ('right', 'UMINUS'),
)

# transaction manager
tm = TransactionManager()
# dictionary of names
names = dict()
for i in xrange(1, 21):
    data_item_name = 'x%d' % i
    names[data_item_name] = DataItem(tm, data_item_name)


def p_stmtlist_0(t):
    'stmtlist : '
    t[0] = []


def p_stmtlist_1(t):
    'stmtlist : statement'
    if t[1] is not None:
        t[0] = t[1]
    else:
        t[0] = []


def p_stmtlist_2(t):
    'stmtlist : statement SEMICOLON stmtlist'
    if t[1] is not None:
        t[0] = t[1] + t[3]
    else:
        t[0] = t[3]


def p_statement_quit(t):
    'statement : QUIT'
    raise SystemExit


def p_statement_begin_transaction(t):
    'statement : BEGIN LPAREN namelist RPAREN'
    for name in t[3]:
        if name in names:
            print('Error: transaction %s has started!!!' % name)
        assert name not in names
        names[name] = ReadWriteTransaction(tm, name)
        tm.new_transaction(names[name])
        logging.debug('command received: begin %s' % name)


def p_statement_begin_readonly_transaction(t):
    'statement : BEGIN_READONLY LPAREN namelist RPAREN'
    for name in t[3]:
        if name in names:
            print('Error: transaction %s has started!!!' % name)
        assert name not in names
        names[name] = ReadOnlyTransaction(tm, name)
        tm.new_transaction(names[name])
    logging.debug('command received: beginRO %s' % t[3])


def p_statement_end_transaction(t):
    'statement : END LPAREN exprlist RPAREN'
    for trans in t[3]:
        trans.append_operation(trans.commit)
        logging.debug('command received: end %s' % trans.name)


def p_statement_fail(t):
    'statement : FAIL LPAREN exprlist RPAREN'
    cmd_list = []
    for val in t[3]:
        cmd_list.append((tm.sites[val - 1].fail, ()))
    t[0] = cmd_list


def p_statement_recover(t):
    'statement : RECOVER LPAREN exprlist RPAREN'
    cmd_list = []
    for val in t[3]:
        cmd_list.append((tm.sites[val - 1].recover, ()))
    t[0] = cmd_list


def p_statement_read(t):
    'statement : READ LPAREN expression COMMA expression RPAREN'
    t[3].append_operation(t[3].read, t[5])
    logging.debug('command received: read %s (transaction %s)' % (
        t[5].name, t[3].name))


def p_statement_write(t):
    'statement : WRITE LPAREN expression COMMA expression COMMA expression RPAREN'
    t[3].append_operation(t[3].write, t[5], t[7])
    logging.debug('command received: writing %d to %s (transaction %s)' % (
        t[7], t[5].name, t[3].name))


def grouped_dump_print(lines):
    print('=' * 80)
    xasc = sorted(lines)
    i, j = 0, 0
    while i < len(xasc):
        while (j < len(xasc) and xasc[i][:-1] == xasc[j][:-1] and 
            xasc[i][-1] - xasc[j][-1] == i - j):
            j += 1
        print('%s: %s at site %s' % (
            xasc[i][0], str(xasc[i][1]), 
            str(xasc[i][-1]) if i + 1 == j else '%d-%d' % (
                xasc[i][-1], xasc[j - 1][-1])))
        i = j


def dump_print(key=None):
    buf = []
    if key is None:
        for k in names:
            x = names[k]
            if isinstance(x, DataItem):
                for s in x.sites:
                    buf.append((
                        x.name, s.historical_values[x.name][-1], s.idx))
    elif isinstance(key, DataItem):
        for s in key.sites:
            buf.append((
                key.name, s.historical_values[key.name][-1], s.idx))
    elif isinstance(key, int):
        for x in tm.sites[key - 1].historical_values:
            buf.append((x, tm.sites[key - 1].historical_values[x][-1], key))
    else:
        print('Error: not a data item or a site to dump')
        return
    grouped_dump_print(buf)


def p_statement_dump(t):
    'statement : DUMP LPAREN RPAREN'
    t[0] = [(dump_print, ())]


def p_statement_dump_spec(t):
    'statement : DUMP LPAREN expression RPAREN'
    t[0] = [(dump_print, (t[3], ))]


def p_statement_assign(t):
    'statement : NAME EQUALS expression'
    names[t[1]] = t[3]


def p_statement_expr(t):
    'statement : expression'
    print(t[1])


def p_namelist_1(t):
    'namelist : NAME'
    t[0] = [t[1]]


def p_namelist_2(t):
    'namelist : NAME COMMA namelist'
    t[0] = [t[1]] + t[3]


def p_exprlist_1(t):
    'exprlist : expression'
    t[0] = [t[1]]


def p_exprlist_2(t):
    'exprlist : expression COMMA exprlist'
    t[0] = [t[1]] + t[3]


def p_expression_binop(t):
    '''expression : expression PLUS expression
                  | expression MINUS expression
                  | expression TIMES expression
                  | expression DIVIDE expression'''
    if t[2] == '+':
        t[0] = t[1] + t[3]
    elif t[2] == '-':
        t[0] = t[1] - t[3]
    elif t[2] == '*':
        t[0] = t[1] * t[3]
    elif t[2] == '/':
        t[0] = t[1] / t[3]


def p_expression_uminus(t):
    'expression : MINUS expression %prec UMINUS'
    t[0] = -t[2]


def p_expression_group(t):
    'expression : LPAREN expression RPAREN'
    t[0] = t[2]


def p_expression_number(t):
    'expression : NUMBER'
    t[0] = t[1]


def p_expression_name(t):
    'expression : NAME'
    try:
        t[0] = names[t[1]]
    except LookupError:
        print("Undefined name '%s'" % t[1])
        t[0] = 0


def p_error(t):
    print("Syntax error at '%s'" % t.value)


import ply.yacc as yacc

parser = yacc.yacc()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '-v', '--verbose', action='count',
        help='increase output verbosity (e.g., -vv is more than -v)')
    arg_parser.add_argument('infile', nargs='?', type=argparse.FileType('rU'),
        help='input file')
    args = arg_parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            format='%(levelname)s: %(message)s', level=(3 - args.verbose) * 10)
        logging.info('verbosity set to be %d' % ((3 - args.verbose) * 10))
    else:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=100)
    # starts running
    if not args.infile:
        while True:
            try:
                s = raw_input('adb > ')  # Use raw_input on Python 2
            except EOFError:
                break
            tm.sleep()
            cmd_list = parser.parse(s) # run these commands later
            tm.next_tick()
            map(lambda (f, x): f(*x), cmd_list)
    else:
        for s in args.infile:
            tm.sleep()
            cmd_list = parser.parse(s) # run these commands later
            tm.next_tick()
            map(lambda (f, x): f(*x), cmd_list)


if __name__ == '__main__':
    main()
