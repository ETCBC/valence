
# coding: utf-8

# <img align="right" src="images/etcbc.png"/>
# 
# # Corrections and enrichment
# 
# In order to do
# [flowchart analysis](https://github.com/ETCBC/valence/blob/master/notebooks/flowchart.ipynb)
# on verbs, we need to correct some coding errors.
# 
# We also need to enrich constituents surrounding the 
# verb occurrences with higher level features, that can be used
# as input for the flow chart decisions.
# 
# Read more in the [wiki](https://github.com/ETCBC/valence/wiki/Workflows).

# # Pipeline
# See [operation](https://github.com/ETCBC/pipeline/blob/master/README.md#operation) 
# for how to run this script in the pipeline.

# This notebook processes excel sheets with manual corrections and enrichments.
# These have been entered against the `4b` version.
# However, the `4b` version in this repository has been regenerated from scratch,
# and in that process the node numbers have changed.
# As the sheets rely on node numbers to let the entered data flow back to the right nodes,
# these sheets no longer work on this version.
# It should be possible to identify the meterial in those sheets on the basis of
# book, chapter and verse info.
# But we leave that as an exercise to posterity.
# 
# 
# For all other versions, we keep the mechanism in place, but for now we work with zero manual input
# for those versions.
# 
# As far as *corrections* are concerned: we expect to see them turn up in the continuous version `c`
# of the core [BHSA](https://github.com/ETCBC/bhsa) data.
# 
# As far as *enrichments* are concerned: there are very few manual enrichments.
# Most of the cases are handled by the algorithm in the notebook.
# 
# We recommend to harvest exceptions in the notebook itself, it has already a mechanism to apply
# verb specific logic.

# In[1]:


if 'SCRIPT' not in locals():
    SCRIPT = False
    FORCE = True
    CORE_NAME = 'bhsa'
    NAME = 'valence'
    VERSION= 'c'

def stop(good=False):
    if SCRIPT: sys.exit(0 if good else 1)


# ## Authors
# 
# [Janet Dyk and Dirk Roorda](https://github.com/ETCBC/valence/wiki/Authors)
# 
# Last modified 2017-09-13.

# ## References
# 
# [References](https://github.com/ETCBC/valence/wiki/References)

# ## Data
# We have carried out the valence project against the Hebrew Text Database of the ETCBC, version 4b.
# See the description of the [sources](https://github.com/ETCBC/valence/wiki/Sources).
# 
# However, we can run our stuff also against the newer versions.

# ## Implementation
# 
# Start the engines. We use the Python package 
# [text-fabric](https://github.com/Dans-labs/text-fabric)
# to process the data of the Hebrew Text Database smoothly and efficiently.

# In[2]:


import sys, os, collections
from copy import deepcopy
import utils
from tf.fabric import Fabric


# # Setting up the context: source file and target directories
# 
# The conversion is executed in an environment of directories, so that sources, temp files and
# results are in convenient places and do not have to be shifted around.

# In[3]:


repoBase = os.path.expanduser('~/github/etcbc')
coreRepo = '{}/{}'.format(repoBase, CORE_NAME)
thisRepo = '{}/{}'.format(repoBase, NAME)

coreTf = '{}/tf/{}'.format(coreRepo, VERSION)

thisSource = '{}/source/{}'.format(thisRepo, VERSION)
thisTemp = '{}/_temp/{}'.format(thisRepo, VERSION)
thisTempTf = '{}/tf'.format(thisTemp)

thisTf = '{}/tf/{}'.format(thisRepo, VERSION)


# # Test
# 
# Check whether this conversion is needed in the first place.
# Only when run as a script.

# In[4]:


if SCRIPT:
    (good, work) = utils.mustRun(None, '{}/.tf/{}.tfx'.format(thisTf, 'valence'), force=FORCE)
    print(good, work)
    if not good: stop(good=False)
    if not work: stop(good=True)


# # Loading the feature data
# 
# We load the features we need from the BHSA core database.

# In[5]:


utils.caption(4, 'Load the existing TF dataset')
TF = Fabric(locations=coreTf, modules=[''])


# We instruct the API to load data.

# In[6]:


api = TF.load('''
    lex gloss lex_utf8
    sp vs lex uvf prs nametype ls
    function rela typ
    mother
''')
api.makeAvailableIn(globals())


# # Locations

# In[7]:


linkShebanq = 'https://shebanq.ancient-data.org/hebrew/text'
linkPassage = '?book={}&chapter={}&verse={}'
linkAppearance = '&version={}&mr=m&qw=n&tp=txt_tb1&tr=hb&wget=x&qget=v&nget=x'.format(VERSION)

resultDir = '{}/results'.format(thisTemp)
allResults = '{}/all.csv'.format(resultDir)
selectedResults = '{}/selected.csv'.format(resultDir)
kinds = ('corr_blank', 'corr_filled', 'enrich_blank', 'enrich_filled')
kdir = {}
for k in kinds:
    kd = '{}/{}'.format(thisSource, k)
    kdir[k] = kd
    if not os.path.exists(kd):
        os.makedirs(kd)
if not os.path.exists(resultDir):
    os.makedirs(resultDir)

def vfile(verb, kind):
    if kind not in kinds:
        utils.caption(0, 'ERROR: Unknown kind `{}`'.format(kind))
        return None
    baseName = verb.replace('>','a').replace('<', 'o')
    return (baseName, '{}/{}.csv'.format(kdir[kind], baseName))


# # Domain
# Here is a subset of verbs that interest us.
# In fact, we are interested in all verbs, but we have subjected the occurrences of these verbs to closer inspection, 
# together with the contexts they occur in.
# 
# Manual additions in the correction and enrichment workflow can only happen for selected verbs.

# In[8]:


verbs_initial = set('''
    CJT
    BR>
    QR>
'''.strip().split())

motion_verbs = set('''
    <BR
    <LH
    BW>
    CWB
    HLK
    JRD
    JY>
    NPL
    NWS
    SWR
'''.strip().split())

double_object_verbs = set('''
    NTN
    <FH
    FJM
'''.strip().split())

complex_qal_verbs = set('''
    NF>
    PQD
'''.strip().split())

verbs = verbs_initial | motion_verbs | double_object_verbs | complex_qal_verbs


# # 1. Correction workflow
# 
# ## 1.1 Phrase function
# 
# We need to correct some values of the phrase function.
# When we receive the corrections, we check whether they have legal values.
# Here we look up the possible values.

# In[9]:


predicate_functions = {
    'Pred', 'PreS', 'PreO', 'PreC', 'PtcO', 'PrcS',
}


# In[10]:


legal_values = dict(
    function={F.function.v(p) for p in F.otype.s('phrase')},
)


# We generate a list of occurrences of those verbs, organized by the lexeme of the verb.
# We need some extra values, to indicate other coding errors.

# In[11]:


error_values = dict(
    function=dict(
        BoundErr='this constituent is part of another constituent and does not merit its own function/type/rela value',
    ),
)


# We add the error_values to the legal values.

# In[12]:


for feature in set(legal_values.keys()) | set(error_values.keys()):
    ev = error_values.get(feature, {})
    if ev:
        lv = legal_values.setdefault(feature, set())
        lv |= set(ev.keys())
if not SCRIPT:
    utils.caption(0, '{}'.format(legal_values))


# In[13]:


utils.caption(4, 'Finding occurrences ...')
occs = collections.defaultdict(list)   # dictionary of verb occurrence nodes per verb lexeme
npoccs = collections.defaultdict(list) # same, but those not occurring in a "predicate"
clause_verb = collections.defaultdict(list)    # dictionary of verb occurrence nodes per clause node
sel_clause_verb = collections.defaultdict(list)    # dictionary of selected verb occurrence nodes per clause node
clause_verb_index = collections.defaultdict(set) # mapping from clauses to its main verb(s)
sel_clause_verb_index = collections.defaultdict(set) # mapping from clauses to its main verb(s), for selected verbs
verb_clause_index = collections.defaultdict(list) # mapping from verbs to the clauses of which it is main verb
sel_verb_clause_index = collections.defaultdict(list) # mapping from selected verbs to the clauses of which it is main verb

nw = 0
sel_nw = 0
for w in F.otype.s('word'):
    if F.sp.v(w) != 'verb': continue
    lex = F.lex.v(w).rstrip('[=')
    nw += 1
    pf = F.function.v(L.u(w, 'phrase')[0])
    if pf not in predicate_functions:
        npoccs[lex].append(w)
    occs[lex].append(w)
    cn = L.u(w, 'clause')[0]
    clause_verb[cn].append(w)
    clause_verb_index[cn].add(lex)
    verb_clause_index[lex].append(cn)
    if lex in verbs:
        sel_nw += 1
        sel_clause_verb[cn].append(w)
        sel_clause_verb_index[cn].add(lex)

sel_verb_clause_index = dict((lex, cns) for (lex, cns) in verb_clause_index.items() if lex in verbs)
sel_clause_verb

utils.caption(0, '\tDone')
utils.caption(0, '\tAll:      {:>4} verbs with {:>6} verb occurrences in {} clauses'.format(
    len(verb_clause_index), nw, len(clause_verb)))
utils.caption(0, '\tSelected: {:>4} verbs with {:>6} verb occurrences in {} clauses'.format(
    len(sel_verb_clause_index), sel_nw, len(sel_clause_verb)))

for verb in sorted(verbs):
    utils.caption(0, '\t{} {:>5} occurrences of which {:>4} outside a predicate phrase'.format(
        verb, 
        len(occs[verb]),
        len(npoccs[verb]),
        continuation=True,
    ))


# # 1.2 Blank sheet generation
# Generate correction sheets.
# They are CSV files. Every row corresponds to a verb occurrence.
# The fields per row are the node numbers of the clause in which the verb occurs, the node number of the verb occurrence, the text of the verb occurrence (in ETCBC transliteration, consonantal) a passage label (book, chapter, verse), and then 4 columns for each phrase in the clause:
# 
# * phrase node number
# * phrase text (ETCBC translit consonantal)
# * original value of the `function` feature
# * corrected value of the `function` feature (generated as empty)

# In[14]:


utils.caption(4, 'Generating blank correction sheets ...')
sheetKind = 'corr_blank'
utils.caption(0, '\tas {}'.format(vfile('{verb}', sheetKind)[1]))

phrases_seen = collections.Counter()

def gen_sheet(verb):
    rows = []
    fieldsep = ';'
    field_names = '''
        clause#
        word#
        passage
        link
        verb
        stem
    '''.strip().split()
    max_phrases = 0
    clauses_seen = set()
    for wn in occs[verb]:
        cln = L.u(wn, 'clause')[0]
        if cln in clauses_seen: continue
        clauses_seen.add(cln)
        vn = L.u(wn, 'verse')[0]
        bn = L.u(wn, 'book')[0]
        (bookName, ch, vs) = T.sectionFromNode(vn, lang='la')
        passage_label = '{} {}:{}'.format(*T.sectionFromNode(vn))
        ln = linkShebanq+(linkPassage.format(bookName, ch, vs))+linkAppearance
        lnx = '''"=HYPERLINK(""{}""; ""link"")"'''.format(ln)
        vt = T.text([wn], fmt='text-trans-plain')
        vstem = F.vs.v(wn)
        np = '* ' if wn in npoccs[verb] else ''
        row = [cln, wn, passage_label, lnx, np+vt, vstem]
        phrases = L.d(cln, 'phrase')
        n_phrases = len(phrases)
        if n_phrases > max_phrases: max_phrases = n_phrases
        for pn in phrases:
            phrases_seen[pn] += 1
            pt = T.text(L.d(pn, 'word'), fmt='text-trans-plain')
            pf = F.function.v(pn)
            pnp = np if pf in predicate_functions else ''
            row.extend((pn, pnp+pt, pf, ''))
        rows.append(row)
    for i in range(max_phrases):
        field_names.extend('''
            phr{i}#
            phr{i}_txt
            phr{i}_function
            phr{i}_corr
        '''.format(i=i+1).strip().split())
    location = vfile(verb, sheetKind)
    if location == None: return
    (baseName, fileName) = location
    row_file = open(fileName, 'w')
    row_file.write('{}\n'.format(fieldsep.join(field_names)))
    for row in rows:
        row_file.write('{}\n'.format(fieldsep.join(str(x) for x in row)))
    row_file.close()
    utils.caption(0, '\t\tfor verb {}'.format(baseName))
    
for verb in verbs: gen_sheet(verb)
    
stats = collections.Counter()
for (p, times) in phrases_seen.items(): stats[times] += 1
for (times, n) in sorted(stats.items(), key=lambda y: (-y[1], y[0])):
    utils.caption(0, '\t{:<6} phrases seen {:<2} time(s)'.format(n, times))
utils.caption(0, '\tTotal phrases seen: {}'.format(len(phrases_seen)))


# # 1.3 Processing corrections
# We read the filled-in correction sheets and extract the correction data out of it.
# We store the corrections in a dictionary keyed by the phrase node.
# We check whether we get multiple corrections for the same phrase.

# In[15]:


utils.caption(4, 'Processing filled correction sheets ...')
sheetKind = 'corr_filled'
utils.caption(0, '\tas {}'.format(vfile('{verb}', sheetKind)[1]))

phrases_seen = collections.Counter()
pf_corr = {}

def read_corr():
    function_values = legal_values['function']

    for verb in sorted(verbs):
        repeated = collections.defaultdict(list)
        non_phrase = set()
        illegal_fvalue = set()
        nodeNumberErrors = []

        location = vfile(verb, sheetKind)
        if location == None: continue
        (baseName, fileName) = location
        if not os.path.exists(fileName):
            utils.caption(0, '\t\tNO file for {}'.format(baseName))
            continue
        else:
            utils.caption(0, '\t\tverb {}'.format(baseName))
        with open(fileName) as f:
            header = f.__next__()
            for (i, line) in enumerate(f):
                fields = line.rstrip().split(';')
                cn = int(fields[0])
                wn = int(fields[1])
                if F.otype.v(cn) != 'clause':
                    nodeNumberErrors.append([i, '{} is not a clause node'.format(cn)])
                if F.otype.v(wn) != 'word':
                    nodeNumberErrors.append([i, '{} is not a word node'.format(wn)])
                words = set(L.d(cn, 'word'))
                phrases = set(L.d(cn, 'phrase'))
                if wn not in words:
                    nodeNumberErrors.append([i, '{} is not a word of clause {}'.format(wn, cn)])
                for i in range(1, len(fields)//4):
                    (pn, pc) = (fields[2+4*i], fields[2+4*i+3])
                    if pn != '':
                        pn = int(pn)
                        if F.otype.v(pn) != 'phrase':
                            nodeNumberErrors.append([i, '{} is not a phrase node'.format(pn)])
                        if pn not in phrases:
                            nodeNumberErrors.append([i, '{} is not a phrase of clause {}'.format(pn, cn)])
                        pc = pc.strip()
                        phrases_seen[pn] += 1
                        if pc != '':
                            good = True
                            for i in [1]:
                                good = False
                                if pn in pf_corr:
                                    repeated[pn] += pc
                                    continue
                                if pc not in function_values:
                                    illegal_fvalue.add(pc)
                                    continue
                                good = True
                            if good:
                                pf_corr[pn] = pc

        utils.caption(0, '\t{}: Found {:>5} corrections in {}'.format(verb, len(pf_corr), fileName))
        if len(nodeNumberErrors):
            for (i, msg) in nodeNumberErrors:
                utils.caption(0, 'ERROR: Line {:>3}: {}'.format(i+1, msg))
        else:
            utils.caption(0, '\tOK: node numbers in sheet are consistent')
        if len(repeated):
            utils.caption(0, 'ERROR: Some phrases have been corrected multiple times!')
            for x in sorted(repeated):
                utils.caption(0, '\t{:>6}: {}'.format(x, ', '.join(repeated[x])))
        else:
            utils.caption(0, '\tOK: Corrected phrases did not receive multiple corrections')
        if len(non_phrase):
            utils.caption(0, 'ERROR: Corrections have been applied to non-phrase nodes: {}'.format(','.join(non_phrase)))
        else:
            utils.caption(0, '\tOK: all corrected nodes where phrase nodes')
        if len(illegal_fvalue):
            utils.caption(0, 'ERROR: Some corrections supply illegal values for phrase function!')
            utils.caption(0, '\t`{}`'.format('`, `'.join(illegal_fvalue)))
        else:
            utils.caption(0, '\tOK: all corrected values are legal')
    utils.caption(0, '\tFound {} corrections in the phrase function'.format(len(pf_corr)))
        
read_corr()

stats = collections.Counter()
for (p, times) in phrases_seen.items(): stats[times] += 1
for (times, n) in sorted(stats.items(), key=lambda y: (-y[1], y[0])):
    utils.caption(0, '\t{:<6} phrases seen {:<2} time(s)'.format(n, times))
utils.caption(0, '\tTotal phrases seen: {}'.format(len(phrases_seen)))


# # 2. Enrichment workflow
# 
# We create blank sheets for new feature assignments, based on the corrected data.

# In[16]:


enrich_field_spec = '''
valence
    adjunct
    complement
    core

predication
    NA
    regular
    copula

grammatical
    NA
    subject
    principal_direct_object
    direct_object
    NP_direct_object
    indirect_object
    L_object
    K_object
    infinitive_object
    *

original
    NA
    subject
    principal_direct_object
    direct_object
    NP_direct_object
    indirect_object
    L_object
    K_object
    infinitive_object
    *

lexical
    location
    time

semantic
    benefactive
    time
    location
    instrument
    manner
'''
enrich_fields = collections.OrderedDict()
cur_e = None
for line in enrich_field_spec.strip().split('\n'):
    if line.startswith(' '):
        enrich_fields.setdefault(cur_e, set()).add(line.strip())
    else:
        cur_e = line.strip()
nef = len(enrich_fields)
if None in enrich_fields:
    utils.caption(0, 'ERROR: Invalid enrich field specification')
else:
    utils.caption(4, '{} Enrich field specifications OK'.format(nef))
for (ef, fields) in sorted(enrich_fields.items()):
    utils.caption(0, '\t{} has possible values'.format(ef))
    for field in fields:
        utils.caption(0, '\t\t{}'.format(field))


# In[17]:


enrich_baseline_rules = dict(
    phrase='''Adju	Adjunct	adjunct	NA	NA			
Cmpl	Complement	complement	NA	*			
Conj	Conjunction	NA	NA	NA		NA	NA
EPPr	Enclitic personal pronoun	NA	copula	NA			
ExsS	Existence with subject suffix	core	copula	subject			
Exst	Existence	core	copula	NA			
Frnt	Fronted element	NA	NA	NA		NA	NA
Intj	Interjection	NA	NA	NA		NA	NA
IntS	Interjection with subject suffix	core	NA	subject			
Loca	Locative	adjunct	NA	NA		location	location
Modi	Modifier	NA	NA	NA		NA	NA
ModS	Modifier with subject suffix	core	NA	subject			
NCop	Negative copula	core	copula	NA			
NCoS	Negative copula with subject suffix	core	copula	subject			
Nega	Negation	NA	NA	NA		NA	NA
Objc	Object	complement	NA	direct_object			
PrAd	Predicative adjunct	adjunct	NA	NA			
PrcS	Predicate complement with subject suffix	core	regular	subject			
PreC	Predicate complement	core	regular	NA			
Pred	Predicate	core	regular	NA			
PreO	Predicate with object suffix	core	regular	direct_object			
PreS	Predicate with subject suffix	core	regular	subject			
PtcO	Participle with object suffix	core	regular	direct_object			
Ques	Question	NA	NA	NA		NA	NA
Rela	Relative	NA	NA	NA		NA	NA
Subj	Subject	core	NA	subject			
Supp	Supplementary constituent	adjunct	NA	NA			benefactive
Time	Time reference	adjunct	NA	NA		time	time
Unkn	Unknown	NA	NA	NA		NA	NA
Voct	Vocative	NA	NA	NA		NA	NA''',
    clause='''Objc	Object	complement	NA	direct_object			
InfC	Infinitive Construct clause	NA	NA				''',
)


# In[18]:


utils.caption(4, '\tChecking enrich baseline rules')
transform = collections.OrderedDict((('phrase', {}), ('clause', {})))
errors = 0
good = 0

for kind in ('phrase', 'clause'):
    for line in enrich_baseline_rules[kind].split('\n'):
        x = line.split('\t')
        nefields = len(x) - 2
        if len(x) - 2 != nef:
            utils.caption(0, 'ERROR: Wrong number of fields ({} must be {}) in {}:\n{}'.format(nefields, nef, kind, line))
            errors += 1
        transform[kind][x[0]] = dict(zip(enrich_fields, x[2:]))
    for e in error_values['function']:
        transform[kind][e] = dict(zip(enrich_fields, ['']*nef))

    for f in transform[kind]:
        for e in enrich_fields:
            val = transform[kind][f][e]
            if val != '' and val != 'NA' and val not in enrich_fields[e]:
                utils.caption(0, 'ERROR: Defaults for `{}` ({}): wrong `{}` value: "{}"'.format(f, kind, e, val))
                errors += 1
            else: good += 1
if errors:
    utils.caption(0, 'ERROR: There were {} errors ({} good)'.format(errors, good))
else:
    utils.caption(0, '\tEnrich baseline rules are OK ({} good)'.format(good))


# Let us prettyprint the baseline rules of enrichment for easier reference.

# In[19]:


if not SCRIPT:
    ltpl = '{:<8}: '+('{:<15}' * nef)
    utils.caption(0, ltpl.format('func', *enrich_fields), continuation=True)
    for kind in transform:
        utils.caption(0, '[{}]'.format(kind), continuation=True)
        for f in sorted(transform[kind]):
            sfs = transform[kind][f]
            utils.caption(0, ltpl.format(f, *[sfs[sf] for sf in enrich_fields]), continuation=True)


# ## 2.1 Enrichment logic
# 
# We apply enrichment logic to *all* verbs, not only to selected verbs.
# But only selected verbs can receive manual enrichment enhancements.
# 
# For some verbs, selected or not, additional logic specific to that verb can be specified.

# ## 2.2 Direct objects
# 
# We have to do some work to identify (multiple) direct objects and indirect objects.
# 
# [More on direct objects](https://github.com/ETCBC/valence/wiki/Discussion#direct-objects)

# In[20]:


objectfuncs = set('''
Objc PreO PtcO
'''.strip().split())

cmpl_as_obj_preps = set('''
K L
'''.strip().split())

no_prs = set('''
absent n/a
'''.strip().split())


# In[21]:


body_parts = set('''
>NP/ >P/ >PSJM/ >YB</ >ZN/
<JN/ <NQ/ <RP/ <YM/ <YM==/
BHN/ BHWN/ BVN/
CD=/ CD===/ CKM/ CN/
DD/
GRGRT/ GRM/ GRWN/ GW/ GW=/ GWJH/ GWPH/ GXWN/
FPH/
JD/ JRK/ JRKH/
KRF/ KSL=/ KTP/
L</ LCN/ LCWN/ LXJ/
M<H/ MPRQT/ MTL<WT/ MTNJM/ MYX/
NBLH=/
P<M/ PGR/ PH/ PM/ PNH/ PT=/
QRSL/
R>C/ RGL/
XDH/ XLY/ XMC=/ XRY/
YW>R/
ZRW</
'''.strip().split())


# In[22]:


utils.caption(4, 'Finding direct objects and determining the principal one')
clause_objects = collections.defaultdict(set)
objects = collections.defaultdict(set)
objects_count = collections.defaultdict(collections.Counter)
object_kinds = (
    'principal',
    'direct',
    'NP',
    'L',
    'K',
    'clause',
    'infinitive',
)

def is_marked(phr):
    # simple criterion for determining whether a direct object is marked:
    # has it the object marker somewhere?
    words = L.d(p, 'word')
    has_et = False
    for w in words:
        if F.lex.v(w) == '>T':
            has_et = True
            break
    return has_et

for c in clause_verb:
    these_objects = collections.defaultdict(set)
    direct_objects_cat = collections.defaultdict(set)

    for p in L.d(c, 'phrase'):
        pf = pf_corr.get(p, F.function.v(p))  # NB we take the corrected value for phrase function if there is one
        if pf in objectfuncs:
            direct_objects_cat['p_'+pf].add(p)
            these_objects['direct'].add(p)
        elif pf == 'Cmpl':
            pwords = L.d(p, 'word')
            w1 = pwords[0]
            w1l = F.lex.v(w1)
            w2l = F.lex.v(pwords[1]) if len(pwords) > 1 else None
            if w1l in cmpl_as_obj_preps and F.prs.v(w1) in no_prs and not (w1l == 'L' and w2l in body_parts):
                if w1l == 'K': these_objects['K'].add(p)
                elif w1l == 'L': these_objects['L'].add(p)
        
    # find clause objects
    for ac in L.d(L.u(c, 'sentence')[0], 'clause'):
        mothers = list(E.mother.f(ac))
        if not (mothers and mothers[0] == c): continue
        cr = F.rela.v(ac)
        ct = F.typ.v(ac)
        if cr in {'Objc'} or ct in {'InfC'}:
            clause_objects[c].add(ac)
            if cr in {'Objc'}:
                label = cr
                direct_objects_cat['c_'+label].add(ac)
                these_objects['direct'].add(ac)
                these_objects['clause'].add(ac)
            elif ct in {'InfC'}:
                if F.lex.v(L.d(ac, 'word')[0]) == 'L':
                    these_objects['infinitive'].add(ac)
        else:
            continue

    # order the objects in the natural ordering
    direct_objects_order = sortNodes(these_objects.get('direct', set()))
    nobjects = len(direct_objects_order)

    # compute the principal object
    principal_object = None

    for x in [1]:
        # just one object 
        if nobjects == 1:
            # we have chosen not to mark a principal object if there is only one object
            # the alternative is to mark it if it is a phrase. Uncomment the next 2 lines if you want this
            # theobject = list(dobjects_set)[0]
            # if F.otype.v(theobject) == 'phrase': principal_object = theobject
            break
        # rule 1: suffixes and promoted objects
        principal_candidates =            direct_objects_cat.get('p_PreO', set()) |            direct_objects_cat.get('p_PtcO', set())
        if len(principal_candidates) != 0:
            principal_object = sortNodes(principal_candidates)[0]
            break
        principal_candidates = direct_objects_cat.get('p_Objc', set())
        if len(principal_candidates) != 0:
            if len(principal_candidates) == 1:
                principal_object = list(principal_candidates)[0]
                break
            objects_marked = set()
            objects_unmarked = set()
            for p in principal_candidates:
                if is_marked(p):
                    objects_marked.add(p)
                else:
                    objects_unmarked.add(p)
            if len(objects_marked) != 0:
                principal_object = sortNodes(objects_marked)[0]
                break
            if len(objects_unmarked) != 0:
                principal_object = sortNodes(objects_unmarked)[0]
                break            
    if principal_object != None:
        these_objects['principal'].add(principal_object)
    if len(these_objects['infinitive']) and not len(these_objects['direct']):
        # we do not mark an infinitive object if there is no proper direct object around
        these_objects['infinitive'] = set()
    if len(these_objects['principal']):
        these_objects['direct'] -= these_objects['principal']
        for x in these_objects['direct'] - these_objects['clause']:
            # the NP objects are the non-principal phrase like direct objects
            these_objects['NP'].add(x)
        these_objects['direct'] -= these_objects['NP']
    if len(these_objects['principal']) == 0 and len(these_objects['direct']) and (
        len(these_objects['NP']) or\
        len(these_objects['L']) or\
        len(these_objects['K']) or\
        len(these_objects['infinitive'])
    ): # promote the direct objects to principal direct objects
        these_objects['principal'] = these_objects['direct']
        these_objects['direct'] = set()

    for kind in object_kinds:
        n = len(these_objects.get(kind, set()))
        objects_count[kind][n] += 1
        if n:
            objects[kind] |= these_objects[kind]

utils.caption(0, '\tDone')

for kind in object_kinds:
    total = 0
    for (count, n) in sorted(objects_count[kind].items(), key=lambda y: -y[0]):
        if count: total += n
        utils.caption(0, '\t{:>5} clauses with {:>2} {:>10} object{}'.format(n, count, kind, 's' if count != 1 else ''))
    utils.caption(0, '\t{:>5} clauses with {:>2} {:>10} object'.format(total, 'a', kind))


# ## 2.3 Indirect objects
# 
# The ETCBC database has not feature that marks indirect objects.
# We will use computation to determine whether a complement is an indirect object or a locative.
# This computation is just an approximation.
# 
# [More on indirect objects](https://github.com/ETCBC/valence/wiki/Discussion#indirect-objects)
# 
# ### The decision
# 
# We take a decision as follows.
# Based on indicators $ind$ and $loc$ that are proxies for the degree in which the complement is an indirect object or a locative, we arrive at a decision $L$ (complement is *locative*) or $I$ (complement is *indirect object*) or $C$ (complement is neither *locative* nor *indirect object*) as follows:
# 
# (1) $ loc > 0 \wedge ind = 0 \Rightarrow L $
# 
# (2) $ loc = 0 \wedge ind > 0 \Rightarrow I $
# 
# (3) $ loc > 0 \wedge ind > 0 \wedge\ loc - 1 > ind \Rightarrow L$
# 
# (4) $ loc > 0 \wedge ind > 0 \wedge\ loc + 1 < ind \Rightarrow I$
# 
# (5) $ loc > 0 \wedge ind > 0 \wedge |ind - loc| <= 1 \Rightarrow C$

# In[23]:


complfuncs = set('''
Cmpl PreC
'''.strip().split())

cmpl_as_iobj_preps = set('''
L >L
'''.strip().split())


# In[24]:


locative_lexemes = set('''
>RY/ >YL/ >XR/
<BR/ <BRH/ <BWR/ <C==/ <JR/ <L=/ <LJ=/ <LJH/ <LJL/ <MD=/ <MDH/ <MH/ <MQ/ <MQ===/ <QB/
BJN/ BJT/
CM CMJM/ CMC/ C<R/
DRK/
FDH/
HR/
JM/ JRDN/ JRWCLM/ JFR>L/
MDBR/ MW<D/ MWL/ MZBX/ MYRJM/ MQWM/ MR>CWT/ MSB/ MSBH/ MVH==/
QDM/
SBJB/
TJMN/ TXT/ TXWT/
YPWN/
'''.strip().split())

personal_lexemes = set('''
>B/ >CH/ >DM/ >DRGZR/ >DWN/ >JC/ >J=/ >KR/ >LJL/ >LMN=/ >LMNH/ >LMNJ/ >LWH/ >LWP/ >M/ 
>MH/ >MN==/ >MWN=/ >NC/ >NWC/ >PH/ >PRX/ >SJR/ >SJR=/ >SP/ >X/ >XCDRPN/
>XWH/ >XWT/
<BDH=/ <CWQ/ <D=/ <DH=/ <LMH/ <LWMJM/ <M/ <MD/ <MJT/ <QR=/ <R/ <WJL/ <WL/ <WL==/ <WLL/
<WLL=/ <YRH/
B<L/ B<LH/ BKJRH/ BKR/ BN/ BR/ BR===/ BT/ BTWLH/ BWQR/ BXRJM/ BXWN/ BXWR/
CD==/ CDH/ CGL/ CKN/ CLCJM/ CLJC=/ CMRH=/ CPXH/ CW<R/ CWRR/
DJG/ DWD/ DWDH/ DWG/ DWR/
F<JR=/ FB/ FHD/ FR/ FRH/ FRJD/ FVN/
GBJRH/ GBR/ GBR=/ GBRT/ GLB/ GNB/ GR/ GW==/ GWJ/ GZBR/
HDBR/ 
J<RH/ JBM/ JBMH/ JD<NJ/ JDDWT/ JLD/ JLDH/ JLJD/ JRJB/ JSWR/ JTWM/ JWYR/
JYRJM/ 
KCP=/ KHN/ KLH/ KMR/ KN<NJ=/ KNT/ KRM=/ KRWB/ KRWZ/
L>M/ LHQH/ LMD/ LXNH/
M<RMJM/ M>WRH/ MCBR/ MCJX/ MCM<T/ MCMR/ MCPXH/ MCQLT/ MD<=/ MD<T/ MG/
MJNQT/ MKR=/ ML>K/ MLK/ MLKH/ MLKT/ MLX=/ MLYR/ MMZR/ MNZRJM/ MPLYT/ MYRJ/
MPY=/ MQHL/ MQY<H/ MR</ MR>/ MSGR=/ MT/ MWRH/ MYBH=/
N<R/ N<R=/ N<RH/ N<RWT/ N<WRJM/ NBJ>/ NBJ>H/ NCJN/ NFJ>/ NGJD/ NJN/ NKD/ 
NKR/ NPC/ NPJLJM/ NQD/ NSJK/ NTJN/ 
PLGC/ PLJL/ PLJV/ PLJV=/ PQJD/ PR<H/ PRC/ PRJY/ PRJY=/ PRTMJM/ PRZWN/ 
PSJL/ PSL/ PVR/ PVRH/ PXH/ PXR/
QBYH/ QCRJM/ QCT=/ QHL/ QHLH/ QHLT/ QJM/ QYJN/
R<H=/ R<H==/ R<JH/ R<=/ R<WT/ R>H/ RB</ RB=/ RB==/ RBRBNJN/ RGMH/ RHB/ RKB=/
RKJL/ RMH/ RQX==/ 
SBL/ SPR=/ SRJS/ SRK/ SRNJM/ 
T<RWBWT/ TLMJD/ TLT=/ TPTJ/ TR<=/ TRCT>/ TRTN/ TWCB/ TWL<H/ TWLDWT/ TWTX/
VBX/ VBX=/ VBXH=/ VPSR/ VPXJM/
WLD/
XBL==/ XBL======/ XBR/ XBR=/ XBR==/ XBRH/ XBRT=/ XJ=/ XLC/ XM=/ XMWT/
XMWY=/ XNJK/ XR=/ XRC/ XRC====/ XRP=/ XRVM/ XTN/ XTP/ XZH=/
Y<JRH/ Y>Y>JM/ YJ/ YJD==/ YJR==/ YR=/ YRH=/ 
ZKWR/ ZMR=/ ZR</
'''.strip().split())


# In[25]:


utils.caption(4, 'Determinig kind of complements')

complements_c = collections.defaultdict(lambda: collections.defaultdict(lambda: []))
complements = {}
complementk = {}
kcomplements = collections.Counter()

nphrases = 0
ncomplements = 0

for c in clause_verb:
    for p in L.d(c, 'phrase'):
        nphrases += 1
        pf = pf_corr.get(p, F.function.v(p))
        if pf not in complfuncs: continue
        ncomplements += 1
        words = L.d(p, 'word')
        lexemes = [F.lex.v(w) for w in words]
        lexeme_set = set(lexemes)

        # measuring locativity
        lex_locativity = len(locative_lexemes & lexeme_set)
        prep_b = len([x for x in lexeme_set if x == 'B'])
        topo = len([x for x in words if F.nametype.v(x) == 'topo'])
        h_loc = len([x for x in words if F.uvf.v(x) == 'H'])
        body_part = 0
        if len(words) > 1 and F.lex.v(words[0]) == 'L' and F.lex.v(words[1]) in body_parts:
            body_part = 2
        loca = lex_locativity + topo + prep_b + h_loc + body_part

        # measuring indirect object
        prep_l = len([x for x in words if F.lex.v(x) in cmpl_as_iobj_preps and F.prs.v(x) not in no_prs])
        prep_lpr = 0
        lwn = len(words)
        for (n, wn) in enumerate(words):
            if F.lex.v(wn) in cmpl_as_iobj_preps:
                if n+1 < lwn:
                    nextw = words[n+1]
                    if F.lex.v(nextw) in personal_lexemes or F.ls.v(nextw) == 'gntl' or (
                        F.sp.v(nextw) == 'nmpr' and F.nametype.v(nextw) == 'pers'):
                        prep_lpr += 1                        
        indi = prep_l + prep_lpr

        # the verdict
        ckind = 'C'
        if loca == 0 and indi > 0: ckind = 'I'
        elif loca > 0 and indi == 0: ckind = 'L'
        elif loca > indi + 1: ckind = 'L'
        elif loca < indi - 1: ckind = 'I'
        complementk[p] = (loca, indi, ckind)
        kcomplements[ckind] += 1
        complements_c[c][ckind].append(p)
        complements[p] = (pf, ckind)

utils.caption(0, '\tDone')
for (label, n) in sorted(kcomplements.items(), key=lambda y: -y[1]):
    utils.caption(0, '\tPhrases of kind {:<2}: {:>6}'.format(label, n))
utils.caption(0, '\tTotal complements : {:>6}'.format(ncomplements))
utils.caption(0, '\tTotal phrases     : {:>6}'.format(nphrases))


# In[26]:


def has_L(vl, pn):
    words = L.d(pn, 'word')
    return len(words) > 0 and F.lex.v(words[0] == 'L')

def is_lex_personal(vl, pn):
    words = L.d(pn, 'word')
    return len(words) > 1 and (F.lex.v(words[1]) in personal_lexemes or F.nametype.v(words[1]) == 'pers')

def is_lex_local(vl, pn):
    words = L.d(pn, 'word')
    return len({F.lex.v(w) for w in words} & locative_lexemes) > 0

def has_H_locale(vl, pn):
    words = L.d(pn, 'word')
    return len({w for w in words if F.uvf.v(w) == 'H'}) > 0  


# ## 2.4 Generic logic
# 
# This is the function that applies the generic rules about (in)direct objects and locatives.
# It takes a phrase node and a set of new label values, and modifies those values.

# In[27]:


grule_as_str = {
    'pdos':   '''direct_object => principal_direct_object''',
    'pdos-x': '''non-object => principal_direct_object''',
    'ndos':   '''direct_object => NP_direct_object''',
    'ndos-x': '''non-object => NP_direct_object''',
    'dos':    '''non-object => direct_object''',
    'ldos':   '''non-object => L_object''',
    'kdos':   '''non-object => K_object''',
    'inds-c': '''complement => indirect_object''',
    'locs-c': '''complement => location''',
    'inds-p': '''predicate complement => indirect_object''',
    'locs-p': '''predicate complement => location''',
    'cdos':   '''direct-object =(superfluously)=> direct object (clause)''',
    'cdos-x': '''non-object => direct object (clause)''',
    'idos':   '''infinitive_object =(superfluously)=> infinitive_object (clause)''',
    'idos-x': '''infinitive clause => infinitive_object''',
}

def rule_as_str_g(x, i): return '{}-{}'.format(i, grule_as_str[i])

rule_as_str = dict(
    generic=rule_as_str_g,
)

def generic_logic_p(pn, values):
    gl = None
    if pn in objects['principal']:
        oldv = values['grammatical']
        if oldv == 'direct_object':
            gl = 'pdos'
        else:
            gl = 'pdos-x'
            values['original'] = oldv
        values['grammatical'] = 'principal_direct_object'
    elif pn in objects['NP']:
        oldv = values['grammatical']
        if oldv == 'direct_object':
            gl = 'ndos'
        else:
            gl = 'ndos-x'
            values['original'] = oldv
        values['grammatical'] = 'NP_direct_object'
    elif pn in objects['direct']:
        oldv = values['grammatical']
        if oldv != 'direct_object':
            gl = 'dos'
            values['original'] = oldv
            values['grammatical'] = 'direct_object'
    elif pn in objects['L']:
        oldv = values['grammatical']
        gl = 'ldos'
        values['original'] = oldv
        values['grammatical'] = 'L_object'
    elif pn in objects['K']:
        oldv = values['grammatical']
        gl = 'kdos'
        values['original'] = oldv
        values['grammatical'] = 'K_object'
    elif pn in complements:
        (pf, ck) = complements[pn]
        if ck in {'I', 'L'}:
            if pf == 'Cmpl':
                if ck == 'I':
                    values['grammatical'] = 'indirect_object'
                    gl = 'inds-c'
                else:
                    values['lexical'] = 'location'
                    values['semantic'] = 'location'
                    gl = 'locs-c'
            elif pf == 'PreC':
                if ck == 'I':
                    values['grammatical'] = 'indirect_object'
                    gl = 'inds-p'
                else:
                    values['lexical'] = 'location'
                    values['semantic'] = 'location'
                    gl = 'locs-p'
    return gl

def generic_logic_c(cn, values):
    gl = None
    if cn in objects['clause']:
        oldv = values['grammatical']
        if oldv == 'direct_object':
            gl = 'cdos'
        else:
            gl = 'cdos-x'
            values['original'] = oldv
        values['grammatical'] = 'direct_object'
    elif cn in objects['infinitive']:
        oldv = values['grammatical']
        if oldv == 'infinitive_object':
            gl = 'idos'
        else:
            gl = 'idos-x'
            values['original'] = oldv
        values['grammatical'] = 'infinitive_object'
    return gl

generic_logic = dict(
    phrase=generic_logic_p,
    clause=generic_logic_c,
)


# ## 2.5 Verb specific rules
# 
# The verb-specific enrichment rules are stored in a dictionary, keyed  by the verb lexeme.
# The rule itself is a list of items.
# 
# The last item is a tuple of conditions that need to be fulfilled to apply the rule.
# 
# A condition can take the shape of
# 
# * a function, taking a phrase or clause node as argument and returning a boolean value
# * an ETCBC feature for phrases or clauses : value, 
#   which is true iff that feature has that value for the phrase or clause in question

# In[28]:


dbl_obj_rules = (
    (
        ('semantic', 'benefactive'), 
        ('function:Adju', has_L, is_lex_personal),
    ),
    (
        ('lexical', 'location'),
        ('function:Cmpl', has_H_locale),
    ),
    (
        ('lexical', 'location'),
        ('semantic', 'location'),
        ('function:Cmpl', is_lex_local),
    ),
)
enrich_logic = dict(
    phrase={
        'CJT': dbl_obj_rules,
        'FJM': dbl_obj_rules,
    },
    clause={
    },
)


# In[29]:


rule_index = collections.defaultdict(lambda: [])

def rule_as_str_s(vl, i):
    (conditions, sfassignments) = rule_index[vl][i]
    label = '{}-{}\n'.format(vl, i+1)
    rule = '\tIF   {}'.format('\n\tAND  '.join(
        '{:<10} = {:<8}'.format(
                *c.split(':')
            ) if type(c) is str else '{:<15}'.format(
                c.__name__
            ) for c in conditions,
    ))
    ass = []
    for (i, sfa) in enumerate(sfassignments):
        ass.append('\t\t{:<10} => {:<15}\n'.format(*sfa))
    return '{}{}\n\tTHEN\n{}'.format(label, rule, ''.join(ass))

rule_as_str['specific'] = rule_as_str_s

def check_logic():
    utils.caption(4, 'Checking enrichment logic')
    errors = 0
    nrules = 0
    for kind in sorted(enrich_logic):
        for vl in sorted(enrich_logic[kind]):
            for items in enrich_logic[kind][vl]:
                rule_index[vl].append((items[-1], items[0:-1]))
            for (i, (conditions, sfassignments)) in enumerate(rule_index[vl]):
                if not SCRIPT: utils.caption(0, rule_as_str_s(vl, i), continuation=True)
                nrules += 1
                for (sf, sfval) in sfassignments:
                    if sf not in enrich_fields:
                        utils.caption(0, 'ERROR: {}: "{}" not a valid enrich field'.format(kind, sf), continuation=True)
                        errors += 1
                    elif sfval not in enrich_fields[sf]:
                        utils.caption(0, 'ERROR: {}: `{}`: "{}" not a valid enrich field value'.format(kind, sf, sfval), continuation=True)
                        errors += 1
                for c in conditions:
                    if type(c) == str:
                        x = c.split(':')
                        if len(x) != 2:
                            utils.caption(0, 'ERROR: {}: Wrong feature condition {}'.format(kind, c), continuation=True)
                            errors += 1
                        else:
                            (feat, val) = x
                            if feat not in legal_values:
                                utils.caption(0, 'ERROR: {}: Feature `{}` not in use'.format(kind, feat), continuation=True)
                                errors += 1
                            elif val not in legal_values[feat]:
                                utils.caption(0, 'ERROR: {}: Feature `{}`: not a valid value "{}"'.format(kind, feat, val), continuation=True)
                                errors += 1
    if errors:
        utils.caption(0, '\tERROR: There were {} errors in {} rules'.format(errors, nrules))
    else:
        utils.caption(0, '\tAll {} rules OK'.format(nrules))

check_logic()


# In[30]:


rule_cases = collections.defaultdict(lambda: collections.defaultdict(lambda: {}))

def apply_logic(kind, vl, n, init_values):
    values = deepcopy(init_values)
    gr = generic_logic[kind](n, values)
    if gr:
        rule_cases['generic'][kind].setdefault(('', gr), []).append(n)
    verb_rules = enrich_logic[kind].get(vl, [])
    for (i, items) in enumerate(verb_rules):
        conditions = items[-1]
        sfassignments = items[0:-1]

        ok = True
        for condition in conditions:
            if type(condition) is str:
                (feature, value) = condition.split(':')
                if feature == 'function' and kind == 'phrase':
                    fval = pf_corr.get(n, F.function.v(n))
                else:
                    fval = F.item[feature].v(n)
                this_ok =  fval == value
            else:
                this_ok = condition(vl, n)
            if not this_ok:
                ok = False
                break
        if ok:
            for (sf, sfval) in sfassignments:
                values[sf] = sfval
            rule_cases['specific'][kind].setdefault((vl, i), []).append(n)
    return tuple(values[sf] for sf in enrich_fields)


# # 2.6 Generate enrichments
# 
# First we generate enriched values for all relevant phrases.
# The generated enrichment values are computed on the basis of generic logic.
# Additionally, verb-bound logic is applied, if it has been specified.
# 
# We store the enriched features in a dictionary, first keyed by the type of constituent that
# receives the enrichments (`phrase` or `clause`), and then by the node number of the constituent.

# In[31]:


utils.caption(4, 'Generating enrichments')

seen = collections.defaultdict(collections.Counter)
enrichFields = dict()

def gen_enrich(verb):
    clauses_seen = set()

    for wn in occs[verb]:
        cn = L.u(wn, 'clause')[0]
        if cn in clauses_seen:
            continue
        clauses_seen.add(cn)
        vl = F.lex.v(wn).rstrip('[=')
        vstem = F.vs.v(wn)
        for pn in L.d(cn, 'phrase'):
            seen['phrase'][pn] += 1
            pf = pf_corr.get(pn, F.function.v(pn))
            enrichFields[pn] = apply_logic('phrase', vl, pn, transform['phrase'][pf])
        for scn in clause_objects[cn]:
            seen['clause'][scn] += 1
            scty = F.typ.v(scn)
            scr = F.rela.v(scn)
            enrichFields[scn] = apply_logic('clause', vl, scn, transform['clause'][scr if scr == 'Objc' else scty])       

for verb in verb_clause_index:
    gen_enrich(verb)
utils.caption(0, '\tGenerated enrichment values for {} verbs:'.format(len(verb_clause_index)))
utils.caption(0, '\tEnriched values for {:>5} nodes'.format(len(enrichFields)))


# In[32]:


utils.caption(0, '\tOverview of rule applications:')

for scope in rule_cases:
    totalscope = 0
    for kind in rule_cases[scope]:
        utils.caption(0, '{}-{} rules:'.format(scope, kind))
        totalkind = 0
        for rule_spec in rule_cases[scope][kind]:
            cases = rule_cases[scope][kind][rule_spec]
            n = len(cases)
            totalscope += n
            totalkind += n
            if not SCRIPT:
                if scope == 'generic':
                    utils.caption(0, '{:>4} x\n\t{}\n\t{}\n'.format(
                        n, rule_as_str[scope](*rule_spec), 
                        ', '.join(str(c) for c in cases[0:10]),
                    ))
                else:                
                    utils.caption(0, '{:>4} x\n\t{}\n\t{}\n'.format(
                        n, rule_as_str[scope](*rule_spec),
                        ', '.join(str(c) for c in cases[0:10]),
                    ))
        utils.caption(0, '{:>6} {}-{} rule applications'.format(totalkind, scope, kind))
    utils.caption(0, '{:>6} {} rule applications'.format(totalscope, scope))

for kind in seen:
    stats = collections.Counter()
    for (node, times) in seen[kind].items(): stats[times] += 1
    if not SCRIPT:
        for (times, n) in sorted(stats.items(), key=lambda y: (-y[1], y[0])):
            utils.caption(0, '\t{:>6} {} seen {:<2} time(s)'.format(n, kind, times))
    utils.caption(0, '\t{:>6} {} seen in total'.format(len(seen[kind]), kind))


# For selected verbs, we write the enrichments to spreadsheets.

# In[33]:


COMMON_FIELDS = '''
    cnode#
    vnode#
    onode#
    book
    chapter
    verse
    verb_lexeme
    verb_stem
    verb_occurrence
    text
    constituent
'''.strip().split()

PHRASE_FIELDS = '''
    type
    function
'''.strip().split()

CLAUSE_FIELDS = '''
    type
    rela
'''.strip().split()

field_names = COMMON_FIELDS + CLAUSE_FIELDS + PHRASE_FIELDS + list(enrich_fields) 
pfillrows = len(CLAUSE_FIELDS)
cfillrows = len(PHRASE_FIELDS)
fillrows =  pfillrows + cfillrows + len(enrich_fields)
if not SCRIPT: print('\n'.join(field_names))    


# In[34]:


utils.caption(4, 'Generate blank enrichment sheets')
sheetKind = 'enrich_blank'
utils.caption(0, '\tas {}'.format(vfile('{verb}', sheetKind)[1]))

def gen_sheet_enrich(verb):
    rows = []
    fieldsep = ';'
    clauses_seen = set()
    for wn in occs[verb]:
        cn = L.u(wn, 'clause')[0]
        if cn in clauses_seen: continue
        clauses_seen.add(cn)
        vn = L.u(wn, 'verse')[0]
        bn = L.u(wn, 'book')[0]
        (book_name, chapter, verse) = T.sectionFromNode(cn, lang='la')
        book = T.sectionFromNode(cn)[0]
        ln = linkShebanq+(linkPassage.format(book_name, chapter, verse))+linkAppearance
        vl = F.lex.v(wn).rstrip('[=')
        vstem = F.vs.v(wn)
        vt = T.text([wn], fmt='text-trans-plain')
        ct = T.text(L.d(cn, 'word'), fmt='text-trans-plain')
        
        common_fields = (cn, wn, -1, book, chapter, verse, vl, vstem, vt, ct, '')
        rows.append(common_fields + (('',)*fillrows))
        for pn in L.d(cn, 'phrase'):
            seen['phrase'][pn] += 1
            pt = T.text(L.d(pn, 'word'), fmt='text-trans-plain')
            common_fields = (cn, wn, pn, book, chapter, verse, vl, vstem, '', pt, 'phrase')
            pty = F.typ.v(pn)
            pf = pf_corr.get(pn, F.function.v(pn))
            phrase_fields =                ('',)*pfillrows +                (pty, pf) +                enrichFields[pn]
            rows.append(common_fields + phrase_fields)
        for scn in clause_objects[cn]:
            seen['clause'][scn] += 1
            sct = T.text(L.d(scn, 'word'), fmt='text-trans-plain')
            common_fields = (cn, wn, scn, book, chapter, verse, vl, vstem, '', sct, 'clause')
            scty = F.typ.v(scn)
            scr = F.rela.v(scn)
            clause_fields =                (scty, scr) +                ('',)*cfillrows +                enrichFields[scn]
            rows.append(common_fields + clause_fields)

    location = vfile(verb, sheetKind)
    if location == None: return
    (baseName, fileName) = location

    row_file = open(fileName, 'w')
    row_file.write('{}\n'.format(fieldsep.join(field_names)))
    for row in rows:
        row_file.write('{}\n'.format(fieldsep.join(str(x) for x in row)))
    row_file.close()
    utils.caption(0, '\t\tfor verb {} ({:>5} rows)'.format(verb, len(rows)))
    
for verb in verbs: gen_sheet_enrich(verb)

utils.caption(0, '\tDone')


# In[35]:


def showcase(n):
    otype = F.otype.v(n)
    att1 = pf_corr.get(n, F.function.v(n)) if otype == 'phrase' else F.rela.v(n)
    att2 = F.typ.v(n)
    utils.caption(0, '''{} ({}-{}) {}\n{} {}:{}    {}\n'''.format(
        otype, att1, att2,
        T.text(L.d(n, 'word'), fmt='text-trans-plain'),
        *T.sectionFromNode(n),
        T.text(L.d(L.u(n, 'verse')[0], 'word'), fmt='text-trans-plain'),
    ), continuation=True)


# In[36]:


if not SCRIPT:
    showcase(654844)
    showcase(445014)
    #showcase(426954)


# In[37]:


def check_h(vl, show_results=False):
    hl = {}
    total = 0
    for w in F.otype.s('word'):
        if F.sp.v(w) != 'verb' or F.lex.v(w).rstrip('[=/') != vl: continue
        total += 1
        c = L.u(w, 'clause')[0]
        ps = L.d(c, 'phrase')
        phs = {p for p in ps if len({w for w in L.d(p, 'word') if F.uvf.v(w) == 'H'}) > 0}
        for f in ('Cmpl', 'Adju', 'Loca'):
            phc = {p for p in ps if pf_corr.get(p, None) or (pf_corr.get(p, F.function.v(p))) == f}
            if len(phc & phs): hl.setdefault(f, set()).add(w)
    for f in hl:
        utils.caption(0, 'Verb {}: {} occurrences. He locales in {} phrases: {}'.format(vl, total, f, len(hl[f])), continuation=True)
        if show_results: utils.caption(0, '\t{}'.format(', '.join(str(x) for x in hl[f])), continuation=True)

if not SCRIPT:
    check_h('BW>', show_results=True)        


# It would be handy to generate an informational spreadsheet that shows all these cases.

# ## 2.6 Process the filled in enrichments
# 
# We read the enrichments, and perform some consistency checks.
# If the filled-in sheet does not exist, we take the blank sheet, with the default assignment of the new features.
# If a phrase got conflicting features, because it occurs in sheets for multiple verbs, the values in the filled-in sheet take precedence over the values in the blank sheet. If both occur in a filled in sheet, a warning will be issued.

# In[38]:


def read_enrich():
    of_enriched = {
        False: {}, # for enrichments found in blank sheets
        True: {}, # for enrichments found in filled sheets
    }
    repeated = {
        False: collections.defaultdict(list), # for blank sheets
        True: collections.defaultdict(list), # for filled sheets
    }
    wrong_value = {
        False: collections.defaultdict(list),
        True: collections.defaultdict(list),
    }

    non_match = collections.defaultdict(list)
    wrong_node = collections.defaultdict(list)

    results = []
    dev_results = [] # results that deviate from the filled sheet
    
    ERR_LIMIT = 10

    for verb in sorted(verbs):
        vresults = {
            False: {}, # for blank sheets
            True: {}, # for filled sheets
        }
        for check in (
            (False, 'blank'), 
            (True, 'filled'),
        ):
            is_filled = check[0]
            
            location = vfile(verb, 'enrich_{}'.format(check[1]))
            if location == None: continue
            (baseName, fileName) = location

            if not os.path.exists(fileName):
                if not is_filled:
                    utils.caption(0, '\tNO {} enrichment sheet for {}'.format(check[1], baseName))
                continue
            utils.caption(0, '\t{} enrichment sheet for {}'.format(check[1], baseName))

            with open(fileName) as fh:
                header = fh.__next__()
                for line in fh:
                    fields = line.rstrip().split(';')
                    on = int(fields[2])
                    if on < 0: continue
                    kind = fields[10]
                    objects_seen[kind][on] += 1
                    vvals = tuple(fields[-nef:])
                    for (f, v) in zip(enrich_fields, vvals):
                        if v != '' and v != 'X' and v != 'NA' and v not in enrich_fields[f]:
                            wrong_value[is_filled][on].append((verb, f, v))
                    vresults[is_filled][on] = vvals
                    if on in of_enriched[is_filled]:
                        if on not in repeated[is_filled]:
                            repeated[is_filled][on] = [of_enriched[is_filled][on]]
                        repeated[is_filled][on].append((verb, vvals))
                    else:
                        of_enriched[is_filled][on] = (verb, vvals)
                    if F.otype.v(on) != kind: 
                        non_match[on].append((verb, kind))
            for on in sorted(vresults[True]):          # check whether the phrase ids are not mangled
                if on not in vresults[False]:
                    wrong_node[on].append(verb)
            for on in sorted(vresults[False]):      # now collect all results, give precedence to filled values
                if F.otype.v(on) == 'phrase':
                    f_corr = on in pf_corr  # manual correction in phrase function
                    f_good = pf_corr.get(on, F.function.v(on)) 
                else:
                    f_corr = ''
                    f_good = ''
                s_manual = on in vresults[True] and vresults[False][on] != vresults[True][on] # real change

                # here we determine which value is going to be put in a feature
                # basic rule: if there is an filled-in sheet, take the value from there, else from the blank one
                # exception: 
                # if a value is empty in the filled-in sheet, but not in the blank one, take the non-empty one
                #
                # Why? Well, sometimes we improve the enrich logic. There may be filled-in sheets based on older
                # blank sheets. 
                # We want to push new values in blank sheets through unfilled in values in the filled sheets.
                # If it is intentional to remove a value from the blank sheet, 
                # you can put an X in the corresponding filled field.
                blank_results = vresults[False][on]
                these_results = []

                for (i, br) in enumerate(blank_results):
                    the_value = br
                    if s_manual and vresults[True][on][i] != '':
                        the_value = vresults[True][on][i]
                        if the_value == 'X':
                            the_value = ''
                    these_results.append(the_value)
                these_results = tuple(these_results)
                            
                # these_results = vresults[True][on] if s_manual else vresults[False][on]
                
                if f_corr or s_manual:
                    dev_results.append((on,)+these_results+(f_good, f_corr, s_manual))
                results.append((on,)+these_results+(f_good, f_corr, s_manual))

    for check in (
        (False, 'blank'), 
        (True, 'filled'),
    ):
        if len(wrong_value[check[0]]): #illegal values in sheets
            wrongs = wrong_value[check[0]]
            for x in sorted(wrongs)[0:ERR_LIMIT]:
                px = T.text(L.d(x, 'word'), fmt='ev')
                ref_node = L.u(x, 'clause')[0] if F.otype.v(x) != 'clause' else x
                cx = T.text(L.d(ref_node, 'word'), fmt='ev')
                passage = T.sectionFromNode(x)
                utils.caption(0, 'ERROR: {} Illegal value(s) in {}: {} = {} in {}:'.format(
                    passage, check[1], x, px, cx
                ), continuation=True)
                for (verb, f, v) in wrongs[x]:
                    utils.caption(0, 'ERROR: \t"{}" is an illegal value for "{}" in verb {}'.format(
                        v, f, verb,
                    ), continuation=True)
            ne = len(wrongs)
            if ne > ERR_LIMIT: utils.caption(0, ' ... AND {} CASES MORE'.format(ne - ERR_LIMIT), continuation=True)
        else:
            utils.caption(0, '\tOK: The used {} enrichment sheets have legal values'.format(check[1]))

        nerrors = 0
        if len(repeated[check[0]]): # duplicates in sheets, check consistency
            repeats = repeated[check[0]]
            for x in sorted(repeats):
                overview = collections.defaultdict(list)
                for y in repeats[x]: overview[y[1]].append(y[0])
                px = T.text(L.d(x, 'word'), fmt='ev')
                ref_node = L.u(x, 'clause')[0] if F.otype.v(x) != 'clause' else x
                cx = T.text(L.d(ref_node, 'word'), fmt='ev')
                passage = T.sectionFromNode(x)
                if len(overview) > 1:
                    nerrors += 1
                    if nerrors < ERR_LIMIT:
                        utils.caption(0, 'ERROR: {} Conflict in {}: {} = {} in {}:'.format(
                            passage, check[1], x, px, cx
                        ), continuation=True)
                        for vals in overview:
                            utils.caption(0, '\t{:<40} in verb(s) {}'.format(
                                ', '.join(vals),
                                ', '.join(overview[vals]),
                        ), continuation=True)
                elif False: # for debugging purposes
                #else:
                    nerrors += 1
                    if nerrors < ERR_LIMIT:
                        utils.caption(0, '\t{} Agreement in {} {} = {} in {}: {}'.format(
                            passage, check[1], x, px, cx, ','.join(list(overview.values())[0]),
                        ), continuation=True)
            ne = nerrors
            if ne > ERR_LIMIT: utils.caption(0, ' ... AND {} CASES MORE'.format(ne - ERR_LIMIT), continuation=True)
        if nerrors == 0:
            utils.caption(0, '\tOK: The used {} enrichment sheets are consistent'.format(check[1]))

    if len(non_match):
        utils.caption(0, 'ERROR: Enrichments have been applied to nodes with non-matching types:')
        for x in sorted(non_match)[0:ERR_LIMIT]:
            (verb, shouldbe) = non_match[x]
            px = T.text(L.d(x, 'word'), fmt='ev')
            utils.caption(0, 'ERROR: {}: {} Node {} is not a {} but a {}'.format(
                verb, T.sectionFromNode(x), x, shouldbe, F.otype.v(x),
            ), continuation=True)
        ne = len(non_phrase)
        if ne > ERR_LIMIT: utils.caption(0, ' ... AND {} CASES MORE'.format(ne - ERR_LIMIT), continuation=True)
    else:
        utils.caption(0, '\tOK: all enriched nodes where phrase nodes')

    if len(wrong_node):
        utils.caption(0, 'ERROR: Node in filled sheet did not occur in blank sheet:')
        for x in sorted(wrong_node)[0:ERR_LIMIT]:
            px = T.text(L.d(x, 'word'), fmt='ev')
            utils.caption(0, '{}: {} node {}'.format(
                wrong_node[x], T.sectionFromNode(x), x,
            ), continuation=True)
        ne = len(wrong_node)
        if ne > ERR_LIMIT: utils.caption(0, ' ... AND {} CASES MORE'.format(ne - ERR_LIMIT), continuation=True)
    else:
        utils.caption(0, '\tOK: all enriched nodes occurred in the blank sheet')

    if len(dev_results):
        utils.caption(0, '\tOK: there are {} manual correction/enrichment annotations'.format(len(dev_results)))
        for r in dev_results[0:ERR_LIMIT]:
            (x, *vals, f_good, f_corr, s_manual) = r
            px = T.text(L.d(x, 'word'), fmt='ev')
            cx = T.text(L.d(L.u(x, 'clause')[0], 'word'), fmt='ev')
            utils.caption(0, '{:<30} {:>7} => {:<3} {:<3} {}\n\t{}\n\t\t{}'.format(
                'COR' if f_corr else '',
                'MAN' if s_manual else'',
                '{} {}:{}'.format(*T.sectionFromNode(x)), x, ','.join(vals), px, cx
            ), continuation=True)
        ne = len(dev_results)
        if ne > ERR_LIMIT: utils.caption(0, '... AND {} ANNOTATIONS MORE'.format(ne - ERR_LIMIT), continuation=True)
    else:
        utils.caption(0, '\tthere are no manual correction/enrichment annotations')
    return results


# In[39]:


utils.caption(4, 'Processing enrichment sheets ...')
sheetKind = 'enrich_filled'

utils.caption(0, '\tas {}'.format(vfile('{verb}', sheetKind)[1]))
objects_seen = collections.defaultdict(collections.Counter)
sheetResults = read_enrich()


# In[40]:


if not SCRIPT:
    list(enrichFields.items())[0:10]


# In[41]:


if not SCRIPT:
    sheetResults[0:10]


# Combine the sheet results with the generic results in one single dictionary, keyed by node number.

# In[42]:


utils.caption(4, 'Combine the manual results with the generic results')
allResults = dict()
for (n, *features) in sheetResults:
    allResults[n] = features
utils.caption(0, '\tAnnotations from sheets for {} nodes'.format(len(allResults)))
utils.caption(0, '\tMerging {} annotations from generic enrichment'.format(len(enrichFields)))
for (n, features) in enrichFields.items():
    if n in allResults: continue
    allResults[n] = features + ('', '', False)
utils.caption(0, '\tResulting in annotations for {} nodes'.format(len(allResults)))


# # 3 Generate data
# 
# We write the correction and enrichment data as a data module in text-fabric format.

# In[43]:


newFeatures = list(enrich_fields.keys())+['function', 'f_correction', 's_manual']

description = dict(
    title='Correction and enrichment features',
    description='Corrections, alternatives and additions to the ETCBC4b encoding of the Hebrew Bible',
    purpose='Support the decision process of assigning valence to verbs',
    method='Generated blank correction and enrichment spreadsheets with selected clauses',
    steps='sheets filled out by researcher; read back in by program; generated new features based on contents',
    author='The content and nature of the features are by Janet Dyk, the workflow is by Dirk Roorda',
)

metaData = {
    '': description,
    'valence': {
        'description': 'verbal valence main classification',
    },
    'predication': {
        'description': 'verbal function main classification',
    },
    'grammatical': {
        'description': 'constituent role main classification',
    },
    'original': {
        'description': 'default value before enrichment logic has been applied',
    },
    'lexical': {
        'description': 'additional lexical characteristics',
    },
    'semantic': {
        'description': 'additional semantic characteristics',
    },
    'f_correction': {
        'description': 'whether the phrase function has been manually corrected',
    },
    's_manual': {
        'description': 'whether the generated enrichment features have been manually changed',
    },
    'function': {
        'description': 'corrected phrase function, only present for phrases that were in a correction sheet',
    },
}

for f in newFeatures: metaData[f]['valueType'] = 'str'


# In[44]:


nodeFeatures = dict()

for (node, featureVals) in allResults.items():
    for (fName, fVal) in zip(newFeatures, featureVals):
        fValRep = fVal
        if type(fVal) is bool:
            fValRep = 'y' if fVal else ''
        nodeFeatures.setdefault(fName, {})[node] = fValRep

RENAMES = [('function', 'cfunction')]
for (oldF, newF) in RENAMES:
    for data in (nodeFeatures, metaData):
        data[newF] = data[oldF]
        del data[oldF]


# In[45]:


utils.caption(4, 'Writing TF enrichment features')
TF = Fabric(locations=thisTempTf, silent=True)
TF.save(nodeFeatures=nodeFeatures, edgeFeatures={}, metaData=metaData)


# # Diffs
# 
# Check differences with previous versions.

# In[46]:


utils.checkDiffs(thisTempTf, thisTf, only=set(nodeFeatures))


# # Deliver 
# 
# Copy the new TF features from the temporary location where they have been created to their final destination.

# In[47]:


utils.deliverFeatures(thisTempTf, thisTf, nodeFeatures)


# # Compile TF

# In[48]:


utils.caption(4, 'Load and compile the new TF features')

TF = Fabric(locations=[coreTf, thisTf], modules=[''])
api = TF.load('''
    lex gloss lex_utf8
    sp vs lex rela typ
    function
''' + ' '.join(nodeFeatures))
api.makeAvailableIn(globals())


# # Examples
# Take the first 10 phrases and retrieve the corrected and uncorrected function feature.
# Note that the corrected function feature is only filled in, if it occurs in a clause in which a selected verb occurs.

# In[49]:


for i in list(F.otype.s('phrase'))[0:10]: 
    print('{} - {} - {}'.format(
        F.function.v(i), 
        F.cfunction.v(i),
        L.u(i, 'clause')[0] in clause_verb,
    ))


# In[ ]:


if SCRIPT:
    stop(good=True)


# ## Results
# 
# We put all corrections and enrichments in a single csv file for checking.
# 
# We also generate a smaller csv, with only the data for selected verbs in it.

# In[80]:


f = open(allResults, 'w')
g = open(selectedResults, 'w')

NALLFIELDS = 17
tpl = ('{};' * (NALLFIELDS - 1))+'{}\n'

utils.caption(0, 'collecting constituents ...')
f.write(tpl.format(
    '-',
    '-',
    'passage',
    'verb(s) text',
    '-',
    '-',
    '-',
    '-',
    '-',
    '-',
    '-',
    '-',
    '-',
    '-',
    '-',
    '-',
    'clause text',
    'clause node',
))
f.write(tpl.format(
    'corrected',
    'enriched',
    'passage',
    '-',
    'object type',
    'clause rela',
    'clause type',
    'phrase function (old)',
    'phrase function (new)',
    'phrase type',
    'valence',
    'predication',
    'grammatical',
    'original',
    'lexical',
    'semantic',
    'object text',
    'object node',
))
i = 0
h = 0
j = 0
c = 0
d = 0
CHUNK_SIZE = 10000
sel_verbs = set(verbs)
for cn in sorted(clause_verb):
    c += 1
    vrbs = sorted(clause_verb[cn])
    lex_vrbs = {F.lex.v(verb).rstrip('[=') for verb in vrbs}
    selected = len(lex_vrbs & sel_verbs) != 0
    if selected:
        d += 1
        sel_vrbs = [v for v in vrbs if F.lex.v(v).rstrip('[=') in verbs]
        
        g.write(tpl.format(
            '',
            '',
            '{} {}:{}'.format(*T.sectionFromNode(cn)),
            ' '.join(F.lex.v(verb) for verb in sel_vrbs),
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            T.text(L.d(cn, 'word'), fmt='text-trans-plain'),
            cn,
        ))

    f.write(tpl.format(
        '',
        '',
        '{} {}:{}'.format(*T.sectionFromNode(cn)),
        ' '.join(F.lex.v(verb) for verb in vrbs),
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        T.text(L.d(cn, 'word'), fmt='text-trans-plain'),
        cn,
    ))
    for pn in L.d(cn, 'phrase'):
        i += 1
        if selected: h += 1
        j += 1
        if j == CHUNK_SIZE:
            j = 0
            utils.caption(0, '{:>6} selected of {:>6} constituents in {:>5} selected of {:>5} clauses ...'.format(h, i, d, c))
            
        material = tpl.format(
            'COR' if F.f_correction.v(pn) == 'y' else '',
            'MAN' if F.s_manual.v(pn) == 'y' else '',
            '{} {}:{}'.format(*T.sectionFromNode(pn)),
            '',
            'phrase',
            '',
            '',
            F.function.v(pn),
            F.cfunction.v(pn),
            F.typ.v(pn),
            F.valence.v(pn),
            F.predication.v(pn),
            F.grammatical.v(pn),
            F.original.v(pn),
            F.lexical.v(pn),
            F.semantic.v(pn),
            T.text(L.d(pn, 'word'), fmt='text-trans-plain'),
            pn,
        )
        f.write(material)
        if selected:
            g.write(material)
    for scn in clause_objects[cn]:
        i += 1
        if selected: h += 1
        j += 1
        if j == CHUNK_SIZE:
            j = 0
            utils.caption(0, '{:>6} constituents in {:>5} clauses ...'.format(i, c))
        material = tpl.format(
            '',
            '',
            '{} {}:{}'.format(*T.sectionFromNode(scn)),
            '',
            'clause',
            F.rela.v(scn),
            F.typ.v(scn),
            '',
            '',
            '',
            F.valence.v(scn),
            F.predication.v(scn),
            F.grammatical.v(scn),
            F.original.v(scn),
            F.lexical.v(scn),
            F.semantic.v(scn),
            T.text(L.d(scn, 'word'), fmt='text-trans-plain'),
            scn,
        )
        f.write(material)
        if selected:
            g.write(material)

f.close()
g.close()
utils.caption(0, '{:>6} selected of {:>6} constituents in {:>5} selected of {:>5} clauses done'.format(h, i, d, c))


# In[81]:


x  = 671522
print(pf_corr.get(x, F.function.v(x)))
print(is_lex_local('FJM',x))
print(x in rule_cases['specific']['phrase'][('FJM', 2)])
print(F.lexical.v(x))

