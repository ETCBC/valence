
# coding: utf-8

# <img align="right" src="images/etcbc.png"/>
# 
# # Verbal valence
# 
# *Verbal valence* is a kind of signature of a verb, not unlike overloading in programming languages.
# The meaning of a verb depends on the number and kind of its complements, i.e. the linguistic entities that act as arguments for the semantic function of the verb.
# 
# We will use a set of flowcharts to specify and compute the sense of a verb in specific contexts depending on the verbal valence. The flowcharts have been composed by Janet Dyk. Although they are not difficult to understand, it takes a good deal of ingenuity to apply them in all the real world situations that we encounter in our corpus.
# 
# Read more in the [wiki](https://github.com/ETCBC/valence/wiki).

# # Pipeline
# See [operation](https://github.com/ETCBC/pipeline/blob/master/README.md#operation) 
# for how to run this script in the pipeline.

# In[1]:


if 'SCRIPT' not in locals():
    SCRIPT = False
    FORCE = True
    CORE_NAME = 'bhsa'
    NAME = 'valence'
    VERSION= 'c'
    CORE_MODULE = 'core'

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
# 
# We also make use of corrected and enriched data delivered by the
# [corrEnrich notebook](https://github.com/ETCBC/valence/blob/master/notebooks/corrEnrich.ipynb).
# The features of that data module are specified
# [here](https://github.com/ETCBC/valence/wiki/Data).

# ## Results
# 
# We produce a text-fabric feature `sense` with the sense labels per verb occurrence, and add
# this to the *valence* data module created in the
# [corrEnrich](https://github.com/ETCBC/valence/blob/master/notebooks/corrEnrich.ipynb) notebook.
# 
# We also show the results in 
# [SHEBANQ](https://shebanq.ancient-data.org), the website of the ETCBC that exposes its Hebrew Text Database in such a way
# that users can query it, save their queries, add manual annotations and even upload bulks of generated annotations.
# That is exactly what we do: the valency results are visible in SHEBANQ in notes view, so that every outcome can be viewed in context.

# # Flowchart logic
# 
# Valence flowchart logic translates the verb context into a label that is characteristic for the context.
# You could say, it is a fingerprint of the context.
# Verb meanings are complex, depending on context. It turns out that we can organize
# the meaning selection of verbs around these finger prints.
# 
# For each verb, the we can specify a *flowchart* as a mapping of fingerprints to concrete meanings.
# We have flowcharts for a limited, but open set of verbs.
# They are listed in the
# [wiki](https://github.com/ETCBC/valence/wiki),
# and will be referred to from the resulting valence annotations in SHEBANQ.
# 
# For each verb, the flowchart is represented as a mapping of *sense labels* to meaning templates.
# A sense label is a code for the presence and nature of direct objects and  complements that are present in the context.
# See the [legend](https://github.com/ETCBC/valence/wiki/Legend) of sense labels.
# 
# The interesting part is the *sense template*, 
# which consist of a translation text augmented with placeholders for the direct objecs and complements.
# 
# See for example the flowchart of [NTN](https://github.com/ETCBC/valence/wiki/FC_NTN).
# 
# * `{verb}` the verb occurrence in question
# * `{pdos}` principal direct objects (phrase)
# * `{kdos}` K-objects (phrase)
# * `{ldos}` L-objects (phrase)
# * `{ndos}` direct objects (phrase) (none of the above)
# * `{idos}` infinitive construct (clause) objects
# * `{cdos}` direct objects (clause) (none of the above)
# * `{inds}` indirect objects
# * `{bens}` benefactive adjuncts
# * `{locs}` locatives
# * `{cpls}` complements, not marked as either indirect object or locative
# 
# In case there are multiple entities, the algorithm returns them chunked as phrases/clauses.
# 
# Apart from the template, there is also a *status* and an optional *account*. 
# 
# The status is ``!`` in normal cases, ``?`` in dubious cases, and ``-`` in erroneous cases.
# In SHEBANQ these statuses are translated into colors of the notes (blue/orange/red).
# 
# The account contains information about the grounds of which the algorithm has arrived at its conclusions.

# In[2]:


senses = set('''
<FH
BR>
CJT
DBQ
FJM
NTN
QR>
ZQN
'''.strip().split())

senseLabels = '''
--
-i
-b
-p
-c
d-
di
db
dp
dc
n.
l.
k.
i.
c.
'''.strip().split()

constKindSpecs = '''
verb:verb
dos:direct object
pdos:principal direct object
kdos:K-object
ldos:L-object
ndos:NP-object
idos:infinitive object clause
cdos:direct object clause
inds:indirect object
bens:benefactive
locs:locative
cpls:complement
'''.strip().split('\n')


# # Results
# 
# See the results as annotations on [SHEBANQ](https://shebanq.ancient-data.org/hebrew/note?version=4b&id=Mnx2YWxlbmNl&tp=txt_tb1&nget=v).
# 
# The complete set of results is in the note set 
# [valence](https://shebanq.ancient-data.org/hebrew/note?version=4b&id=Mnx2YWxlbmNl&tp=txt_tb1).

# # Firing up the engines

# In[3]:


import sys, os
import collections
from copy import deepcopy
import utils
from tf.fabric import Fabric


# # Setting up the context: source file and target directories
# 
# The conversion is executed in an environment of directories, so that sources, temp files and
# results are in convenient places and do not have to be shifted around.

# In[4]:


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

# In[5]:


if SCRIPT:
    (good, work) = utils.mustRun(None, '{}/.tf/{}.tfx'.format(thisTf, 'sense'), force=FORCE)
    if not good: stop(good=False)
    if not work: stop(good=True)


# # Loading the feature data
# 
# We load the features we need from the BHSA core database and from the valence module,
# as far as generated by the 
# [enrich](https://github.com/ETCBC/valence/blob/master/programs/enrich.ipynb) notebook.

# In[6]:


utils.caption(4, 'Load the existing TF dataset')
TF = Fabric(locations=[coreTf, thisTf], modules=[''])


# We instruct the API to load data.

# In[7]:


api = TF.load('''
    function rela typ
    g_word_utf8 trailer_utf8
    lex prs uvf sp pdp ls vs vt nametype gloss
    book chapter verse label number
    s_manual f_correction
    valence predication grammatical original lexical semantic
    mother
''')
api.makeAvailableIn(globals())


# # Locations

# In[8]:


resultDir = '{}/annotations'.format(thisTemp)
flowchartBase = 'https://github.com/ETCBC/valence/wiki'

if not os.path.exists(resultDir):
    os.makedirs(resultDir)


# # Indicators
# 
# Here we specify by what features we recognize key constituents.
# We use predominantly features that come from the correction/enrichment workflow.

# In[9]:


# pf ... : predication feature
# gf_... : grammatical feature
# vf_... : valence feature
# sf_... : lexical feature
# of_... : original feature

pf_predicate = {
    'regular',
}
gf_direct_object = {
    'principal_direct_object',
    'NP_direct_object',
    'direct_object',
    'L_object',
    'K_object',
    'infinitive_object',
}
gf_indirect_object = {
    'indirect_object',
}
gf_complement = {
    '*',
}
sf_locative = {
    'location',
}
sf_benefactive ={
    'benefactive',
}
vf_locative = {
    'complement',
    'adjunct',
}

verbal_stems = set('''
    qal
'''.strip().split())


# # Pronominal suffixes
# We collect the information to determine how to render pronominal suffixes on words. 
# On verbs, they must be rendered *accusatively*, like `see him`.
# But on nouns, they must be rendered *genitively*, like `hand my`.
# So we make an inventory of part of speech types and the pronominal suffixes that occur on them.
# On that basis we make the translation dictionaries `pronominal suffix` and `switch_prs`.
# 
# Finally, we define a function `get_prs_info` that for each word delivers the pronominal suffix info and gloss,
# if there is any, and else `(None, None)`.

# In[10]:


prss = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
for w in F.otype.s('word'):
    prss[F.sp.v(w)][F.prs.v(w)] += 1
if not SCRIPT:
    for sp in sorted(prss):
        for prs in sorted(prss[sp]):
            print('{:<5} {:<3} : {:>5}'.format(sp, prs, prss[sp][prs]))


# In[11]:


pronominal_suffix = {
    'accusative': {
        'W': ('p3-sg-m', 'him'),
        'K': ('p2-sg-m', 'you:m'),
        'J': ('p1-sg-', 'me'),
        'M': ('p3-pl-m', 'them:mm'),
        'H': ('p3-sg-f', 'her'),
        'HM': ('p3-pl-m', 'them:mm'),
        'KM': ('p2-pl-m', 'you:mm'),
        'NW': ('p1-pl-', 'us'),
        'HW': ('p3-sg-m', 'him'),
        'NJ': ('p1-sg-', 'me'),
        'K=': ('p2-sg-f', 'you:f'),
        'HN': ('p3-pl-f', 'them:ff'),
        'MW': ('p3-pl-m', 'them:mm'),
        'N': ('p3-pl-f', 'them:ff'),
        'KN': ('p2-pl-f', 'you:ff'),
    },
    'genitive' : {
        'W': ('p3-sg-m', 'his'),
        'K': ('p2-sg-m', 'your:m'),
        'J': ('p1-sg-', 'my'),
        'M': ('p3-pl-m', 'their:mm'),
        'H': ('p3-sg-f', 'her'),
        'HM': ('p3-pl-m', 'their:mm'),
        'KM': ('p2-pl-m', 'your:mm'),
        'NW': ('p1-pl-', 'our'),
        'HW': ('p3-sg-m', 'his'),
        'NJ': ('p1-sg-', 'my'),
        'K=': ('p2-sg-f', 'your:f'),
        'HN': ('p3-pl-f', 'their:ff'),
        'MW': ('p3-pl-m', 'their:mm'),
        'N': ('p3-pl-f', 'their:ff'),
        'KN': ('p2-pl-f', 'your:ff'),        
    }
}
switch_prs = dict(
    subs = 'genitive',
    verb = 'accusative',
    prep = 'accusative',
    conj = None,
    nmpr = None,
    art = None,
    adjv = 'genitive',
    nega = None,
    prps = None,
    advb = None,
    prde = None,
    intj = 'accusative',
    inrg = 'genitive',
    prin = None,
)

def get_prs_info(w):
    sp = F.sp.v(w)
    prs = F.prs.v(w)
    switch = switch_prs[sp]
    return pronominal_suffix.get(switch, {}).get(prs, (None, None))


# # Making a verb-clause index
# 
# We generate an index which gives for each verb lexeme a list of clauses that have that lexeme as the main verb.
# In the index we store the clause node together with the word node(s) that carries the main verb(s).
# 
# Clauses may have multiple verbs. In many cases it is a copula plus an other verb.
# In those cases, we are interested in the other verb, so we exclude copulas.
# 
# Yet, there are also sentences with more than one main verb.
# In those cases, we treat both verbs separately as main verb of one and the same clause.

# In[12]:


utils.caption(4, 'Making the verb-clause index')
occs = collections.defaultdict(list)   # dictionary of all verb occurrence nodes per verb lexeme
verb_clause = collections.defaultdict(list)    # dictionary of all verb occurrence nodes per clause node
clause_verb = collections.OrderedDict() # idem but for the occurrences of selected verbs

for w in F.otype.s('word'):
    if F.sp.v(w) != 'verb': continue
    lex = F.lex.v(w).rstrip('[')
    pf = F.predication.v(L.u(w, 'phrase')[0])
    if pf in pf_predicate:
        cn = L.u(w, 'clause')[0]
        clause_verb.setdefault(cn, []).append(w)
        verb_clause[lex].append((cn, w))
utils.caption(0, '\tDone ({} clauses)'.format(len(clause_verb)))


# # (Indirect) Objects, Locatives, Benefactives

# In[13]:


utils.caption(4, 'Finding key constituents')
constituents = collections.defaultdict(lambda: collections.defaultdict(set))
ckinds = '''
    dos pdos ndos kdos ldos idos cdos inds locs cpls bens
'''.strip().split()

# go through all relevant clauses and collect all types of direct objects
for c in clause_verb:
    these_constituents = collections.defaultdict(set)
    # phrase like constituents
    for p in L.d(c, 'phrase'):
        gf = F.grammatical.v(p)
        of = F.original.v(p)
        sf = F.semantic.v(p)
        vf = F.valence.v(p)
        ckind = None
        if gf in gf_direct_object:
            if gf =='principal_direct_object':
                ckind = 'pdos'
            elif gf == 'NP_direct_object':
                ckind = 'ndos'
            elif gf == 'L_object':
                ckind = 'ldos'
            elif gf == 'K_object':
                ckind = 'kdos'
            else:
                ckind = 'dos'
        elif gf in gf_indirect_object:
            ckind = 'inds'
        elif  sf and sf in sf_benefactive:
            ckind = 'bens'
        elif sf in sf_locative and vf in vf_locative:
            ckind = 'locs'
        elif gf in gf_complement:
            ckind = 'cpls'
        if ckind: these_constituents[ckind].add(p)

    # clause like constituents: only look for object clauses dependent on this clause
    for ac in L.d(L.u(c, 'sentence')[0], 'clause'):
        dep = list(E.mother.f(ac))
        if len(dep) and dep[0] == c:
            gf = F.grammatical.v(ac)
            ckind = None
            if gf in gf_direct_object:
                if gf == 'direct_object':
                    ckind = 'cdos'
                elif gf == 'infinitive_object':
                    ckind = 'idos'
            if ckind: these_constituents[ckind].add(ac)
    
    for ckind in these_constituents:
        constituents[c][ckind] |= these_constituents[ckind]

utils.caption(0, '\tDone, {} clauses with relevant constituents'.format(len(constituents))) 


# In[14]:


def makegetGloss():
    if 'lex' in F.otype.all:
        def _getGloss(w): 
            gloss = F.gloss.v(L.u(w, 'lex')[0])
            return '?' if gloss == None else gloss
    else:
        def _getGloss(w): 
            gloss = F.gloss.v(w)
            return '?' if gloss == None else gloss

    return _getGloss

getGloss = makegetGloss()


# In[15]:


testcases = (
#    426955,
#    427654,
#    428420,
#    429412,
#    429501,
#    429862,
#    431695,
#    431893,
    430372,
)
    
def showcase(n):
    otype = F.otype.v(n)
    verseNode = L.u(n, 'verse')[0]
    place = T.sectionFromNode(verseNode)
    print('''CASE {}={} ({}-{})\nCLAUSE: {}\nVERSE\n{} {}\nGLOSS {}\n'''.format(
        n, otype, F.rela.v(n), F.typ.v(n),
        T.text(L.d(n, 'word'), fmt='text-trans-plain'),
        '{} {}:{}'.format(*place),
        T.text(L.d(verseNode, 'word'), fmt='text-trans-plain'),
        ' '.join(getGloss(w) for w in L.d(verseNode, 'word'))
    ))
    print('PHRASES\n')
    for p in L.d(n, 'phrase'):
        print('''{} ({}-{}) {} "{}"'''.format(
            p, F.function.v(p), F.typ.v(n),
            T.text(L.d(p, 'word'), fmt='text-trans-plain'),
            ' '.join(getGloss(w) for w in L.d(p, 'word')),
        ))
        print('valence = {}; grammatical = {}; lexical = {}; semantic = {}\n'.format(
            F.valence.v(p),
            F.grammatical.v(p),
            F.lexical.v(p),
            F.semantic.v(p),
        ))
    print('SUBCLAUSES\n')
    for ac in L.d(L.u(n, 'sentence')[0], 'clause'):
        dep = list(E.mother.f(ac))
        if not(len(dep) and dep[0] == n): continue
        print('''{} ({}-{}) {} "{}"'''.format(
            ac, F.rela.v(ac), F.typ.v(ac),
            T.text(L.d(ac, 'word'), fmt='text-trans-plain'),
            ' '.join(getGloss(w) for w in L.d(ac, 'word')),
        ))
        print('valence = {}; grammatical = {}; lexical = {}; semantic = {}\n'.format(
            F.valence.v(ac),
            F.grammatical.v(ac),
            F.lexical.v(ac),
            F.semantic.v(ac),
        ))

    print('CONSTITUENTS')
    for ckind in ckinds:
        print('{:<4}: {}'.format(ckind, ','.join(str(x) for x in sorted(constituents[n][ckind]))))
    print('================\n')

if not SCRIPT:
    for n in (testcases): showcase(n)


# # Overview of quantities

# In[16]:


utils.caption(4, 'Counting constituents')

constituents_count = collections.defaultdict(collections.Counter)

for c in constituents:
    for ckind in ckinds:
        n = len(constituents[c][ckind])
        constituents_count[ckind][n] += 1

for ckind in ckinds:
    total = 0
    for (count, n) in sorted(constituents_count[ckind].items(), key=lambda y: -y[0]):
        if count: total += n
        utils.caption(0, '\t{:>5} clauses with {:>2} {:<10} constituents'.format(n, count, ckind))
    utils.caption(0, '\t{:>5} clauses with {:>2} {:<10} constituent'.format(total, 'a', ckind))
utils.caption(0, '\t{:>5} clauses'.format(len(clause_verb)))


# # Applying the flowchart
# 
# We can now apply the flowchart in a straightforward manner.
# 
# We output the results as a comma separated file that can be imported directly into SHEBANQ as a set of notes, so that the reader can check results within SHEBANQ. This has the benefit that the full context is available, and also data view can be called up easily to inspect the coding situation for each particular instance.

# In[17]:


glossHacks = {
    'XQ/': 'law/precept',
}


# In[18]:


def reptext(label, ckind, v, phrases, num=False, txt=False, gloss=False, textformat='text-trans-plain'): 
    if phrases == None: return ''
    label_rep = '{}='.format(label) if label else ''
    phrases_rep = []
    for p in sorted(phrases, key=sortKey):
        ptext = '[{}|'.format(F.number.v(p) if num else '[')
        if txt:
            ptext += T.text(L.d(p, 'word'), fmt=textformat)
        if gloss:
            words = L.d(p, 'word')
            if ckind == 'ldos' and F.lex.v(words[0]) == 'L': words = words[1:]

            wtexts = []
            for w in words:
                g = glossHacks.get(F.lex.v(w), getGloss(w)).replace('<object marker>','&')
                if F.lex.v(w) == 'BJN/' and F.pdp.v(w) == 'prep': g = 'between'
                prs_g = get_prs_info(w)[1]
                uvf = F.uvf.v(w)
                wtext = ''
                if uvf == 'H': ptext += 'toward '
                wtext += g if w != v else '' # we do not have to put in the gloss of the verb in question
                wtext += ('~'+prs_g) if prs_g != None else ''
                wtexts.append(wtext)
            ptext += ' '.join(wtexts)
        ptext += ']'
        phrases_rep.append(ptext)
    return ' '.join(phrases_rep)


# In[19]:


debug_messages = collections.defaultdict(lambda: collections.defaultdict(list))

constKinds = collections.OrderedDict()

for constKindSpec in constKindSpecs:
    (constKind, constKindName) = constKindSpec.strip().split(':', 1)
    constKinds[constKind] = constKindName

def flowchart(v, lex, verb, consts):
    consts = deepcopy(consts)
    n_ = collections.defaultdict(lambda: 0)
    for ckind in ckinds: n_[ckind] = len(consts[ckind])
    char1 = None
    char2 = None
    # determine char 1 of the sense label
    if n_['pdos'] > 0:
        if n_['ndos'] > 0: char1 = 'n'
        elif n_['cdos'] > 0: char1 = 'c'
        elif n_['ldos'] > 0: char1 = 'l'
        elif n_['kdos'] > 0: char1 = 'k'
        elif n_['idos'] > 0: char1 = 'i'
        else:
        # in trouble: if there is a principal direct object, there should be an other object as well
        # and the other one should be an NP, object clause, L_object, K_object, or I_object
        # If this happens, it is probably the result of manual correction
        # We warn, and remedy
            msg_rep = '; '.join('{} {}'.format(n_[ckind], ckind) for ckind in ckinds)
            if n_['dos'] > 0:
                # there is an other object (dos should only be used if there is a single object)
                # we'll put the dos in the ndos (which was empty)
                # This could be caused by a manual enrichment sheet that has been generated 
                # before the concept of NP_direct_object had been introduced
                char1 = 'n'
                consts['ndos'] = consts['dos']
                del consts['dos']
                debug_messages[lex]['pdos with dos'].append('{}: {}'.format(T.sectionFromNode(v), msg_rep))
            else:
                # there is not another object, we treat this as a single object, so as a dos
                char1 = 'd'
                consts['dos'] = consts['pdos']
                del consts['pdos']
                debug_messages[lex]['lonely pdos'].append('{}: {}'.format(T.sectionFromNode(v), msg_rep))
    else:
        if n_['cdos'] > 0:
        # in the case of a single object, the clause objects act as ordinary objects
            char1 = 'd'
            consts['dos'] |= consts['cdos']
            del consts['cdos']
        if n_['ndos'] > 0:
        # in the case of a single object, the np_objects act as ordinary objects
            char1 = 'd'
            consts['dos'] |= consts['ndos']
            del consts['ndos']

    n_ = collections.defaultdict(lambda: 0)
    for ckind in ckinds: n_[ckind] = len(consts[ckind])

    if n_['pdos'] == 0 and n_['dos'] > 0:
        char1 = 'd'
    if n_['pdos'] == 0 and n_['dos'] == 0:
        char1 = '-'

    # determine char 2 of the sense label
    if char1 in 'nclki':
        char2 = '.'
    else:
        if n_['inds'] > 0:
            char2 = 'i'
        elif n_['bens'] > 0:
            char2 = 'b'
        elif n_['locs'] > 0:
            char2 = 'p'
        elif n_['cpls'] > 0:
            char2 = 'c'
        else:
            char2 = '-'

    sense_label = char1+char2
    sense = lex if lex in senses else None
    status = '*' if lex in senses else '?'
    
    verb_rep = reptext('', '', v, verb, num=True, gloss=True)
    consts_rep = dict((ckind, reptext('', ckind, v, consts[ckind], num=True, gloss=True)) for ckind in consts)
        
    return (sense_label, sense, status, consts_rep)


# In[20]:


sfields = '''
    version
    book
    chapter
    verse
    clause_atom
    is_shared
    is_published
    status
    keywords
    ntext
'''.strip().split()

sfields_fmt = ('{}\t' * (len(sfields) - 1)) + '{}\n' 


# # Running the flowchart
# 
# The next cell finally performs all the flowchart computations for all verbs in all contexts.

# In[21]:


utils.caption(4, 'Checking the flowcharts')
missingFlowcharts = set()

for lex in verb_clause:
    if lex not in senses:
        missingFlowcharts.add(lex)
utils.caption(0, '\tNo flowchart for {} verbs, e.g. {}'.format(len(missingFlowcharts), ', '.join(sorted(missingFlowcharts)[0:10])))

good = True
for lex in senses:
    if lex not in verb_clause:
        error('No verb {} in enriched corpus'.format(lex))
        good = False
if good:
    utils.caption(0, '\tAll flowcharts belong to a verb in the corpus')


# In[22]:


utils.caption(4, 'Applying the flowcharts')

outcome_lab = collections.Counter()
outcome_lab_l = collections.defaultdict(lambda: collections.Counter())

# we want an overview of the flowchart decisions per lexeme
# Per lexeme, per sense_label we store the clauses

decisions = collections.defaultdict(lambda: collections.defaultdict(dict))

note_keyword_base = 'valence'

nnotes = collections.Counter()

senseFeature = dict()

ofs = open('{}/{}'.format(resultDir, 'valenceNotes.csv'), 'w')
ofs.write('{}\n'.format('\t'.join(sfields)))

i = 0
j = 0
chunkSize = 10000

for lex in verb_clause:
    hasFlowchart = lex in senses
    for (c,v) in verb_clause[lex]:
        if F.vs.v(v) not in verbal_stems: continue
        
        i += 1
        j += 1
        if j == chunkSize:
            j = 0
            utils.caption(0, '\t{:>5} clauses'.format(i))
        book = F.book.v(L.u(v, 'book')[0])
        chapter = F.chapter.v(L.u(v, 'chapter')[0])
        verse = F.verse.v(L.u(v, 'verse')[0])
        sentence_n = F.number.v(L.u(v, 'sentence')[0])
        clause_n = F.number.v(c)
        clause_atom_n = F.number.v(L.u(v, 'clause_atom')[0])
        
        verb = [L.u(v, 'phrase')[0]]
        consts = constituents[c]
        n_ = collections.defaultdict(lambda: 0)
        for ckind in ckinds: n_[ckind] = len(consts[ckind])
        
        (sense_label, sense, status, constsRep) = flowchart(v, lex, verb, consts)
        senseRep = 'legend' if sense == None else sense
        senseDoc = 'Legend' if sense == None else 'FC_{}'.format(sense.replace('>', 'A').replace('<', 'O'))
        senseLink = '{}/{}'.format(flowchartBase, senseDoc)
        
        senseFeature[v] = sense_label
        
        constElems = []
        for (constKind, constKindName) in constKinds.items():
            if constKind not in constsRep: continue
            material = constsRep[constKind]
            if not material: continue
            constElems.append('*{}*={}'.format(constKindName, material))

        outcome_lab[sense_label] += 1
        outcome_lab_l[lex][sense_label] += 1
        decisions[lex][sense_label][c] = sense_label

        ofs.write(sfields_fmt.format(
            VERSION,
            book,
            chapter,
            verse,
            clause_atom_n,
            'T',
            '',
            status,
            note_keyword_base,
            'verb [{nm}|{vb}] has sense `{sl}` [{sn}]({slink}) {cs}'.format(
                nm=F.number.v(L.u(v, 'phrase')[0]),
                vb=F.g_word_utf8.v(v),
                sn=senseRep,
                slink=senseLink,
                sl=sense_label,
                cs='; '.join(constElems)
            ),
        ))
        nnotes[note_keyword_base] += 1
utils.caption(0, '\t{:>5} clauses'.format(i))            
ofs.close()

show_limit = 20
for lex in debug_messages:
    error(lex, continuation=True)
    for kind in debug_messages[lex]:
        utils.caption(0, '\tERROR: {}'.format(kind), continuation=True)
        messages = debug_messages[lex][kind]
        lm = len(messages)
        utils.caption(0, '\tERROR: \t{}{}'.format(
            '\n\t\t'.join(messages[0:show_limit]),
            '' if lm <= show_limit else '\n\t\tAND {} more'.format(lm-show_limit),
        ), continuation=True)


# # Add sense feature to valence module
# 
# We create a new TF feature `sense`, being a mapping from verb word nodes to sense labels, as computed by the flowchart algorithm above.
# 
# We add this feature to the valence module, which has been constructed by the corrEnrich notebook.

# In[23]:


nodeFeatures = dict(sense=senseFeature)
metaData = dict(
    sense=dict(
        valueType='str',
        description='sense label verb occurrences, computed by the flowchart algorithm, see https://github.com/ETCBC/valence/wiki/Legend',
    )
)


# In[24]:


utils.caption(4, 'Writing sense feature to TF')
TF = Fabric(locations=thisTempTf, silent=True)
TF.save(nodeFeatures=nodeFeatures, edgeFeatures={}, metaData=metaData)


# # Diffs
# 
# Check differences with previous versions.

# In[25]:


utils.checkDiffs(thisTempTf, thisTf, only=set(nodeFeatures))


# # Deliver 
# 
# Copy the new TF feature from the temporary location where it has been created to its final destination.

# In[26]:


utils.deliverFeatures(thisTempTf, thisTf, nodeFeatures)


# # Compile TF

# In[27]:


utils.caption(4, 'Load and compile the new TF features')

TF = Fabric(locations=[coreTf, thisTf], modules=[''])
api = TF.load('''
    lex sp vs
    predication gloss
''' + ' '.join(nodeFeatures))
api.makeAvailableIn(globals())


# # Examples

# In[28]:


utils.caption(4, 'Show sense counts')
senseLabels = sorted({F.sense.v(v) for v in F.otype.s('word')} - {None})
utils.caption(0, '\tSense labels = {}'.format(' '.join(senseLabels)))

senseCount = collections.Counter()
noSense = []
isPredicate = {'regular', 'copula'}

for v in F.sp.s('verb'):
    sense = F.sense.v(v)
    if sense == None:
        # skip words that are not verbs in the qal
        if F.vs.v(v) != 'qal': continue
        # skip verbs in a phrase that is not a verb phrase, e.g. some participles
        # the criterion here is whether the value of feature `predication` is non trivial
        p = L.u(v, 'phrase')
        if F.predication.v(p) not in isPredicate: continue 
        noSense.append(v)
        continue
    senseCount[sense] +=1
utils.caption(0, '\tCounted {} senses'.format(sum(senseCount.values())))
if noSense:
    utils.caption(0, '\tWARNING: {} verb occurrences do not have a sense'.format(len(noSense)))
    for v in noSense[0:10]:
        utils.caption(0, '\t\t{:<20} word {:>6} phrase {:>6} = {:<5}'.format(
            '{} {}:{}'.format(*T.sectionFromNode(v)),
            v,
            L.u(v, 'phrase')[0],
            F.lex.v(v)
        ))
else:
    utils.caption(0, '\tAll relevant verbs have been assigned a sense')

for x in sorted(senseCount.items(), key=lambda x: (-x[1], x[0])):
    utils.caption(0, '\t\t{:<2} occurs {:>6}x'.format(*x))


# For more fine grained overview with graphics, see the
# [senses](https://github.com/ETCBC/valence/blob/master/programs/senses.ipynb)
# notebook.

# In[ ]:


if SCRIPT:
    stop(good=True)


# In[65]:


if not SCRIPT:
    utils.caption(0, '\tReporting flowchart application')
    ntot = 0
    for (lab, n) in sorted(nnotes.items(), key=lambda x: x[0]):
        ntot += n
        print('{:<10} notes: {}'.format(lab, n))
    print('{:<10} notes: {}'.format('Total', ntot))

    for lex in [''] + sorted(senses):
        print('All lexemes' if lex == '' else lex)
        src_lab = outcome_lab if lex == '' else outcome_lab_l.get(lex, collections.defaultdict(lambda: 0))
        tot = 0
        for x in senseLabels:
            n = src_lab[x]
            tot += n
            print('     Sense    {:<7}: {:>5} clauses'.format(x, n))
        print('     All senses      : {:>5} clauses'.format(tot))
        print(' ')


# In[49]:


def show_decision(verbs=None, labels=None, books=None): # show all clauses that have a verb in verbs and a sense label in labels
    results = []
    for verb in decisions:
        if verbs != None and verb not in verbs: continue
        for label in decisions[verb]:
            if labels != None and label not in labels: continue
            for (c, stxt) in sorted(decisions[verb][label].items()):
                book = T.sectionFromNode(L.u(c, 'book')[0])[0]
                if books != None and book not in books: continue
                sentence_words = L.d(L.u(c, 'sentence')[0], 'word')
                results.append('{:<7} {:<12} {:<5} {:<2} {}\n\t{}\n\t{}\n'.format(
                    c,
                    '{} {}: {}'.format(*T.sectionFromNode(c)),
                    verb,
                    label,
                    stxt,
                    T.text(sentence_words, fmt='text-trans-plain'),
                    ' '.join(getGloss(w) for w in sentence_words),
                ).replace('<', '&lt;'))
    print('\n'.join(sorted(results)))


# In[50]:


show_decision(verbs={'FJM'}, books={'Isaiah'})


# In[ ]:




