

import os
from functional import seq


def main():

    path = os.path.join('.', 'output', '20171206-101946.txt')
    
    with open(path) as f:
        lines = (seq(f)
            .map(lambda l: l.rstrip('\n')) # remove newline at end of line
            
        )
        print(lines.first())

main()
