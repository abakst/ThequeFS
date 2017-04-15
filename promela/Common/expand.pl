#!/usr/bin/env perl
use strict; 
use warnings;

my $n = $ARGV[0];
print "#define N $n\n";

open(my $fh, '<', $ARGV[1]);

while (my $line = <$fh>) 
{
    my $i;
    if ($line =~ /__RECVLOOP\((.*),(CHAN\(.*\)),(.*)\);/){
        print "if\n";
        for ($i = 0; $i < $n; $i = $i + 1) {
            print ":: RECVLOOP(($1 + $i),$2,$3)\n";
        }
        print "fi\n";
    } else {
        print $line;
    }
    print "\n" if eof;
}
