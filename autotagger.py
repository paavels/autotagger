"""This application does simplified categorization of CSV rows based on defined rules."""
#!/usr/bin/env python

import csv
from collections import namedtuple
import time
import sys

#class Point(namedtuple('Point', 'x y')):
#     __slots__ = ()
#     @property
#     def hypot(self):
#         return (self.x ** 2 + self.y ** 2) ** 0.5
#    def __str__(self):
#        return 'Point: x=%6.3f  y=%6.3f  hypot=%6.3f' % (self.x, self.y, self.hypot)
#

class Expression:
    """Expression"""
    field = ''
    fieldIdx = -1
    value = None
    inclusive = False

Rule = namedtuple("Rule", "name expressions")

def parse_expression(expression):
    """Parses expression from string"""

    expr_parts = expression.lstrip().split("\t")

    if len(expr_parts) < 3:
        print("Invalid expression for line '{0}'".format(expression))
        print("Expected format: field keyword value, [value2, value3, ...]")

        return None

    keyword = expr_parts[1]

    expr = Expression()
    expr.field = expr_parts[0]
    #expr.value = "," + expr_parts[2] + ","
    expr.value = expr_parts[2].split(",")

    if keyword == "IS" or keyword == "EQUALS" or keyword == "IS ANY":
        expr.inclusive = True

    elif keyword == "ALL EXCEPT" or keyword == "IS NOT":
        expr.inclusive = False

    else:
        print("Invalid keyword for line '{0}'".format(expression))
        print()

        return None

    return expr

def read_rules(filename):
    """Reads rules from file"""

    with open(filename, "r", encoding="utf-8-sig") as file_pointer:

        print("Reading rules from " + filename)

        rules = []
        rule = None

        for line in file_pointer:
            line = line.rstrip()

            if not line:
                continue

            if line.startswith("\t"):
                if not rule:
                    print("Found expression without rule, skipping. Expression: " + line)
                    continue

                expr = parse_expression(line)

                if expr:
                    rule.expressions.append(expr)

            else:
                rule = Rule(line, [])
                rules.append(rule)

            #print(line)

        file_pointer.close()

    return [r for r in rules if r.expressions]

def check_rule(entry, rule):
    """Checks entry for rule"""

    for expr in rule.expressions:

        #match = "," + entry[expr.fieldIdx] + "," in expr.value
        match = entry[expr.fieldIdx] in expr.value

        if (not match and expr.inclusive) or (match and not expr.inclusive):
            return False

    return True

def search_field_indexes(header_row, rules):
    """Applies fields index for faster lookup"""

    for idx, val in enumerate(header_row):
        for rule in rules:
            for expr in rule.expressions:
                if expr.field == val:
                    expr.fieldIdx = idx

    for rule in rules:
        for expr in rule.expressions:
            if expr.fieldIdx == -1:
                print("Rule {0} failed to find index for {1}".format(rule.name, expr.field))

    return

def parse_file(filename, rules):
    """Parses incoming file and tags the rows according rules"""

    print("Reading data from " + filename)

    results = {}
    for rule in rules:
        results[rule.name] = 0

    with open(filename, "r", encoding="utf-8-sig") as file_pointer:

        header_row = file_pointer.readline()

        if not header_row:
            print("Failed to receive header row")
            return

        search_field_indexes(header_row.split(","), rules)

        lines = 0

        for row in file_pointer:
            entry = row.split(",")

            for rule in rules:
                if check_rule(entry, rule):
                    results[rule.name] += 1

            if lines % 100000 == 0:
                print("Processed {0} records".format(lines), end="\r")
            lines += 1

    print("Processed {0} records".format(lines))

    return results

def print_results(rules, results):
    """Prints results on screen"""

    print("----------------------------------------")
    print("\t\tRESULTS")
    print("----------------------------------------")

    i = 0
    for rule in rules:
        i += 1
        print("{0: >3}. {1: <48} {2: >8}".format(i, rule.name, results[rule.name]))

    return

def print_rules(rules):
    """Prints rules on screen"""

    print("----------------------------------------")
    print("\t\tRULES")
    print("----------------------------------------")

    for rule in rules:
        print(rule.name)
        for expr in rule.expressions:
            mode = "INCLUSIVE" if expr.inclusive else "EXCLUSIVE"
            print("\t{0: <16}\t{1: <10}\t{2}".format(expr.field, mode, expr.value))
        print()

    print("{0} rules".format(len(rules)))

    return

def main(argv):
    """Main application block"""

    start_time = time.process_time()
    print("Tagger started")

    rules = read_rules("rules.txt")
    print_rules(rules)

    results = parse_file("loan.csv", rules)
    print_results(rules, results)

    elapsed = time.process_time() - start_time
    print("Time elapsed: {0}".format(elapsed))

if __name__ == "__main__":
    main(sys.argv)

# dumb execution string = 24.04
