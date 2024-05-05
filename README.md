# regex_findall
A rust implementation replacing pandas.Series.str.findall

Find all occurrences of pattern or regular expression in the Series.
Equivalent to applying re.findall() to all the elements in the Series.

Provides a major speed improvement over Pandas' implementation.
