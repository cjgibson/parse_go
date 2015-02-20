###
# AUTHORS: CHRISTIAN GIBSON, 
# PROJECT: GO EVOLVE
# UPDATED: FEBURARY 18, 2015
# USAGE:   from parse_obo import parse_obo ; o = parse_obo()
# EXPECTS: python 2.7.6
###

# https://github.com/uqfoundation/pathos.git
from pathos.multiprocessing import ProcessingPool as Pool
import itertools
import datetime
import psutil
import json
import math
import sys

class parse_obo():
    def __init__(self, obo_root=['GO:0003674', 'GO:0005575', 'GO:0008150'],
                       obo_relations=['is_a', 'part_of', 'regulates'],
                       obo_disjoint=['is_a'],
                       obo_path=None):
        # Note: the algorithms herein assume that those terms in
        #   self.obo_root are root nodes for the entire go graph,
        #   and that the relations listed in self.obo_relations
        #   are used to traverse the go ontology. self.obo_disjoint
        #   contains those relations that separate the go ontology
        #   into disjoint ontologies that link to a single root.
        #   It should be a proper subset of self.obo_relations.
        self.obo_root = obo_root
        self.obo_relations = frozenset(obo_relations)
        self.obo_disjoint = []
        for d in obo_disjoint:
            if d in self.obo_relations:
                self.obo_disjoint.append(d)
        self.obo_header = {}
        self.obo_detail = {}
        obo_handle = self._prepare_file(obo_path)
        try:
            self._parse_file(obo_handle)
        except:
            pass
        finally:
            obo_handle.close()

    def reduce_list(self, go_list=[], branch=10, weights={'is_a' : 1}):
        if (not isinstance(weights, dict)
            or len(weights) < 1):
            print 'Provided weights must be stored as a non-empty dictionary'
            print 'with the following format:'
            print '  {'
            for relation in self.obo_relations:
                print "    '" + str(relation) + "': #,"
            print '  }'
            print 'If a relation is left out of the weights dictionary, it'
            print 'will be ignored while reducing the go_list.'
            return None
        
        if not isinstance(go_list, list) or len(go_list) == 0:
            print 'Provided go_list must be a non-empty list of go terms.'
            return None
        
        for k in weights:
            if k not in self.obo_relations:
                weights.pop(k)
                print str(k) + ' is not a valid relation type. (!)'
        return []

    def find_lsca(self, go_1, go_2, weights):
        if set(weights.keys()) == set(self.obo_disjoint):
            if ((go_1 in self.obo_detail and go_2 in self.obo_detail)
                 and self.obo_detail[go_1]['root']
                 and self.obo_detail[go_2]['root']
                 and (self.obo_detail[go_1]['root'] ==
                      self.obo_detail[go_2]['root'])):
                pass
            else:
                return (None, float('inf'))

        terms = {go_1 : None, go_2 : None}
        for term in terms:
            remaining = [(term, 0)]
            options = {term : 0}
            while remaining:
                n_id, n_wt = remaining.pop()
                for path in weights:
                    if path in self.obo_detail[n_id]:
                        for p_id in self.obo_detail[n_id][path]:
                            options[p_id] = n_wt + weights[path]
                            remaining.append((p_id, n_wt + weights[path]))
            terms[term] = options
        
        res_id = None
        res_score = float('inf')
        for option in set(terms[go_1].keys()).intersection(terms[go_2].keys()):
            option_score = terms[go_1][option]**2 + terms[go_2][option]**2
            if option_score < res_score:
                res_id = option
                res_score = option_score
        
        return (res_id, res_score)

    def dump_obo_detail_to_file(self, filename='go-detail.json'):
        with open(filename, 'w') as fh:
            fh.write(json.dumps(self.obo_detail,
                                sort_keys=True,
                                indent=2,
                                separators=(',', ': '),
                                cls=SimpleSafeJSON))

    def dump_pseudotree_to_file(self, filename='go.json'):
        tree = {}
        for r in self.obo_root:
            tree[r] = {}
        for k, v in self.obo_detail.items():
            if v['root']:
                tree[v['root']].setdefault(v['level'], []).append(k)
        with open(filename, 'w') as fh:
            fh.write(json.dumps(tree,
                                sort_keys=True,
                                indent=2,
                                separators=(',', ': ')))
    
    def _lsca_unittest(self, weights={'is_a' : 1,
                                      'part_of' : 2,
                                      'regulates' : 5}):
        def cbrt(x):
            return math.pow(x, 1.0/3.0)

        def group(i, n):
            it = iter(i)
            while True:
                group = tuple(itertools.islice(it, n))
                if not group:
                    return
                yield list(group)
        
        def _find_lsca(go_1, go_2):
            return self.find_lsca(go_1, go_2, weights)

        # Will parallelize tomorrow.
        threads = []
        for _ in range(psutil.cpu_count()):
            threads.append(None)

        missing = {}
        options = frozenset(self.obo_detail.keys())
        tested = set()
        count = 0
        total = len(options)*(len(options)+1)*0.5
        mod = math.floor(cbrt(total))
        for _1 in options:
            for _2 in group(options - tested, psutil.cpu_count()):
                count += len(_2)
                tmp_in = [(_1, __2) for __2 in _2]
                # Submit task to thread here, collect results in res.
                if math.fmod(count, mod) == 0.0:
                    fprint('Processed %d of %d possible combinations. (%0.5f%%)',
                           (count, total, float(count)/total))
                for r_id, _ in res:
                    if not r_id:
                        missing.setdefault(_1, []).append(_2)
            tested.add(_1)
        return missing

    def _prepare_file(self, filepath=None):
        fh = None
        try:
            if filepath:
                fh = open(filepath, 'r')
            else:
                try:
                    fh = open('go.obo', 'r')
                except:
                    fh = open('go-basic.obo', 'r')
        except:
            try:
                fh.close()
            except:
                pass
            finally:
                raise IOError("Cannot locate gene ontology source file.")
        return fh

    def _parse_file(self, fh):
        in_file = False
        in_term = False
        cur_key = None
        cur_val = {}
        
        # Read the file, line-by-line, and filter the results into
        #   a dictionary.
        for line in fh:
            # All information read prior to the first occurrence of a term
            #   block is parsed in and recorded as header information. Once
            #   We encounter our first term block, we consider ourselves to
            #   have entered the body of the file, and the in_file flag is 
            #   set to True.
            if in_file:
                # We consider ourself to be in the in_term state once we've
                #   passed the OBO file header. 
                #   (Once we observe the string '[Term]'.)
                if in_term:
                    # When we observe a line with nothing but a newline
                    #   character, and are inside of a term block, we've
                    #   finished parsing the term, and can clean the
                    #   resulting dictionary.
                    if '\n' == line:
                        # We're no longer in a term block, so we set the
                        #   term flag to False.
                        in_term = False
                        # If we have synonyms, which appear in the form:
                        #     '"go_term" RELATION []'
                        #   we split the string into its parts, and
                        #   organize according to relation type.
                        #   As a side note, the go_term in a synonym
                        #   is encoded as an english phrase, rather than
                        #   as a unique GO id, and the relation is stored
                        #   as a completely capitalized string.
                        if 'synonym' in cur_val:
                            synonyms = cur_val.pop('synonym')
                        else:
                            synonyms = None
                        if synonyms:
                            cur_val['synonym'] = {}
                            for synonym in synonyms:
                                (plaintext,
                                 relation_type,
                                 _) = synonym.rsplit(' ', 2)
                                cur_val['synonym'].setdefault(
                                  relation_type.lower(), []).append(plaintext)

                        # If we have relationships, which appear in the form:
                        #     'relation go_id'
                        #   we split the string into its parts, and organize
                        #   according to the relation type. As a note, the
                        #   linked go_id is the unique GO id.
                        if 'relationship' in cur_val:
                            relationships = cur_val.pop('relationship')
                        else:
                            relationships = None
                        if relationships:
                            for relationship in relationships:
                                (relation_type,
                                 go_id) = relationship.rsplit(' ', 1)
                                cur_val.setdefault(
                                  relation_type, []).append(go_id)

                        # As a final step, we store the cleaned dictionary.
                        self.obo_detail[cur_key] = cur_val
                    # If the line contains anything besides a newline character,
                    #   then we're currently in a term block.
                    else:
                        # Term attributes are stored in the following form:
                        #     'attribute_type: attribute_value'
                        #   We split the string into its parts, and store.
                        try:
                            k, v = [p.strip() for p in line.split(':', 1)]
                        except:
                            print line
                            raise IOError("Error encountered in parsing OBO file.")

                        # Once we've found the id field, we know which go term
                        #   we're currently reading.
                        if k == 'id':
                            cur_key = v
                        # Occasionally, attributes have extra information:
                        #     'attribute_type: attribute_value ! information'
                        #   We remove this information during cleaning.
                        else:
                            v = v.split('!')[0].strip()
                            cur_val.setdefault(k, []).append(v)
                # If we aren't in a term block, but our line contains
                #   '[Term]', we know we've entered a term block, and
                #   set our boolean flags accordingly.
                elif '[Term]' in line:
                    in_term = True
                    cur_key = None
                    cur_val = {}
            # If we aren't yet in the body of the file, we parse information
            #   and record under the assumption that it is part of the file's
            #   header.
            else:
                # This is a bit of a lazy check. In accordance with OBO format,
                #   we know that each line will contain an attribute key value
                #   pair. Once this no longer occurs, ergo, once Python fails
                #   to split a line into two parts around a colon, we know we've
                #   exited the header block.
                try:
                    k, v = [p.strip() for p in line.split(':', 1)]
                    self.obo_header.setdefault(k, []).append(v)
                except ValueError:
                    in_file = True
                except:
                    print line
                    raise IOError("Encountered unexpected line in OBO header.")
        
        # As a cursory stage in the final cleaning process for the generated
        #   obo_detail dictionary, we create new fields 'root' and 'level',
        #   and generate a set of child terms using the 'is_a' relations from
        #   each go term. This is then stored under the field name 'contains'.
        for g_id in self.obo_detail:
            self.obo_detail[g_id]['root'] = None
            self.obo_detail[g_id]['level'] = None
            if 'contains' not in self.obo_detail[g_id]:
                self.obo_detail[g_id]['contains'] = set()
            if 'is_a' in self.obo_detail[g_id]:
                for p_id in self.obo_detail[g_id]['is_a']:
                    self.obo_detail[p_id].setdefault('contains', set()).add(g_id)
        
        # After the set objects in our dictionary are finalized, we cast them
        #   as frozensets to improve comparison time. We also reduce each list
        #   with one element to a single element. Typically the reduced fields
        #   are 'def', 'name', and 'namespace'.
        for g_id in self.obo_detail:
            for k, v in self.obo_detail[g_id].items():
                if isinstance(v, set) or k in self.obo_relations:
                    v = frozenset(v)
                elif isinstance(v, list) and len(v) == 1:
                    v = v[0]
                self.obo_detail[g_id][k] = v
        
        # Lastly, we iterate over the dictionary using depth-first-search,
        #   starting with each root node. In this way, we set the 'root'
        #   and 'level' fields of each go term in our dictionary. We note
        #   that we rely on the 'is_a' relations stored in the 'contains'
        #   field when constructing our 'root' and 'level' fields; due to
        #   this, we are ensured not to match multiple roots to the same
        #   go term. For further information, see:
        #     http://geneontology.org/page/ontology-structure#oneorthree
        #   If a term is left without a 'root' and 'level' field following
        #   this process, it is an obsolete term.
        for r_id in self.obo_root:
            visited = set()
            options = [(r_id, 0)]
            while options:
                n_id, n_ht = options.pop(0)
                if n_id not in visited:
                    visited.add(n_id)
                    self.obo_detail[n_id]['root'] = r_id
                    self.obo_detail[n_id]['level'] = n_ht
                    options.extend(
                      [(x, n_ht+1) for x in 
                        self.obo_detail[n_id]['contains'] - visited]
                    )

        # As a final step, we perform simple cleaning of our header data.
        for k, v in self.obo_header.items():
            if isinstance(v, list) and len(v) == 1:
                v = v[0]
            self.obo_header[k] = v
        
        if 'date' in self.obo_header:
            try:
                self.date = datetime.datetime.strptime(
                              self.obo_header['date'],
                              "%d:%m:%Y %H:%M")
            except:
                self.date = None
            
            if self.date:
                distance = (datetime.datetime.now() - self.date).total_seconds()
                print 'Parsed GO ontology dump is {} hours old. ({} days)'.format(
                        round(distance / 3600, 1), round(distance / 86400, 1))
            else:
                print 'Parsed GO ontology contained no date information. (!)'

def fprint(s, f=()):
    sys.stdout.write('\r')
    sys.stdout.write(s.replace('\t', '    ') % f)
    sys.stdout.flush()

class SimpleSafeJSON(json.JSONEncoder):
    def default(self, obj, safe_method=repr):
        if isinstance(obj, (set, frozenset)):
            return list(obj)
        else:
            try:
                return json.JSONEncoder.default(self, obj)
            except:
                return json.JSONEncoder.default(self, safe_method(obj))