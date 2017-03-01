import math

class stat:
    def __init(self):
        self.suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    
    def human_size(nbytes):
        rank = int((math.log10(nbytes)) / 3)
        rank = min(rank, len(self.suffixes) - 1)
        human = nbytes / (1024.0 ** rank)
        f = ('%.2f' % human).rstrip('0').rstrip('.')
        return '%s %s' % (f, self.suffixes[rank])