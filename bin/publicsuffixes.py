#!/usr/bin/python
#
import re
import sys

def getTopmostAssignedSurtPrefixRegex(file=None):
    if file is None:
        file = sys.path[0] + '/effective_tld_names.dat'
    f = open(file)
    re = surtPrefixRegexFromPublishedFile(f)
    f.close()
    return re

def surtPrefixRegexFromPublishedFile(file):
    '''Reads a file of the format promulgated by publicsuffix.org and
    generates regular expression for matching SURT-prefixes. While
    implementation takes different approach than original Java version
    (org.archive.net.PublicSuffixes), generated regular expression shall
    be fully compatible.'''

    def fix(seg):
        # current list has a stray '?' in a .no domain
        #seg = seg.replace('?', '_')
        # replace '!' with '+' to indicate lookahead-for-exceptions
        # (gets those to sort before '*' at later build-step)
        #seg = seg.replace('!', '+')
        return seg
        
    tree = ['alt']
    for line in file:
        line = line.decode('utf-8')
        # discard whitespace, empty lines, comments, exceptions
        line = re.sub(r'\s*//.*', '', line.strip())
        if len(line) == 0:
            continue
        # discard utf8 notation after entry
        line = re.sub(r'\s.*', '', line).lower()
        # SURT-order domain segments
        segs = line.split('.')
        # first part of exclusion (line starting with '!') is
        # handled as a single token to generate zero-with positive look-ahead
        exclude = segs[0][0] == '!' and segs.pop(0)
        surt = ','.join(map(fix, reversed(segs))) + ','
        tokens = list(surt)
        if exclude:
            tokens.append(exclude + ',')
        tokens.append(u'') # end-of-sequence marker
    
        p = tree
        i = 1
        for c in tokens:
            while True:
                if p[0] == 'alt':
                    # scanning across branches
                    if i >= len(p):
                        # new branch
                        p.append([c])
                        p = p[i]
                        i = 1
                        break
                    elif p[i][0] == c:
                        # found a branch
                        p = p[i]
                        i = 1
                        break
                    elif p[i][0] == '*':
                        # keep wildcard at the end. wildcard pattern must come
                        # after look-ahead pattern for exclusions, or look-ahead
                        # would have no effect. this could be merged with the
                        # first case above, if we can assume there never be
                        # multiple lines of the identical wildcard pattern.
                        p[i:i] = ([c],)
                        p = p[i]
                        i = 1
                        break
                    else:
                        i += 1
                else:
                    # running on a branch
                    if i >= len(p):
                        p.append(c)
                        i += 1
                        break
                    elif type(p[i]) == unicode:
                        if p[i] == c:
                            # common straight path
                            i += 1
                            break
                        else:
                            # need new branching point
                            br1 = p[i:]
                            br2 = [c]
                            # keep wildcard at the end
                            if br1[0] == '*':
                                p[i:] = (['alt', br2, br1],)
                            else:
                                p[i:] = (['alt', br1, br2],)
                            p = br2
                            assert type(p) == list
                            i = 1
                            break
                    else:
                        # branch point - find a branch for c, or create anew
                        assert type(p[i]) == list, str(p)
                        assert p[i][0] == 'alt'
                        p = p[i]
                        i = 1
            assert type(p) == list, 'type(p)=%s' % type(p)
            assert p[0] != 'alt'
            assert 0 < i <= len(p)
        assert type(p) == list
        assert p[0] != 'alt'
        assert i == len(p)
        assert p[-1] == ''

    # dump tree as regular expression
    def dump_re(out, alt, lv=0, pretty=False):
        assert type(alt) == list
        assert alt[0] == 'alt'
        if len(alt) > 1:
            out.append('(?:')
        sep = ''
        if pretty:
            sep1 = '\n'+('  '*lv)+'|'
        else:
            sep1 = '|'
        for b in alt[1:]:
            out.append(sep)
            wc = False
            for c in b:
                if type(c) == unicode:
                    if c != '':
                        if c[0] == '*':
                            out.append(r'[-\w]+')
                        elif c[0] == '!':
                            # exclusion - suppress inclusion of next component
                            # with zero-width positive look-ahead pattern
                            out.append('(?=%s)' % c[1:])
                        else:
                            out.append(c)
                else:
                    dump_re(out, c, lv + 1)
            sep = sep1
                
        if len(alt) > 1:
            out.append(')')

    out = []
    dump_re(out, tree)
    return ''.join(out)
    
if __name__ == '__main__':
    input = None
    if len(sys.argv) > 1:
        input = sys.argv[1]
    print repr(getTopmostAssignedSurtPrefixRegex(input))#.encode('utf-8')
