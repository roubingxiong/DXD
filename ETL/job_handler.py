#!/usr/bin/env python
# -*- coding: utf-8 -*-

import smtplib

import getopt, sys
import argparse



def usage():
    print 'usage'

def main():
    parser = argparse.ArgumentParser(description='Process')

    parser.add_argument('-d', help='specify a date for running with format "yyyy-mm-dd"')
    parser.add_argument('-t', help='type of operation, extract(e) or load(l)')
    parser.add_argument('-o', required=True, nargs='+', help='table object name you want to extract or load')

    args = parser.parse_args()
    print 'date=' + args.d

    # print args[1]
    # print args.accumulate(args.integers)

    print parser.print_help()


if __name__ == "__main__":
    main()

