# Helpers for ArgParse

import argparse

#
class SmartFormatter(argparse.HelpFormatter):

    def _split_lines(self, text, width):
        if text.startswith('R|'):
            lines = []
            for line in text[2:].splitlines():
                lines.extend(argparse.HelpFormatter._split_lines(self, line, width))
            return lines
        # this is the RawTextHelpFormatter._split_lines
        return argparse.HelpFormatter._split_lines(self, text, width)
