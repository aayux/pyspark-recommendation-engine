#!/usr/bin/env python

import sys
import fire

from utils.preprocess import JSONReader as reader

def main():
    if len(sys.argv) != 3:
        raise Exception('Require 2 arguments: <input_uri> <output_uri>')
            
    input_uri = sys.argv[1]
    output_uri = sys.argv[2]

    reader.make_data_dump(input_uri, output_uri)

if __name__ == '__main__':
    fire.Fire(main)