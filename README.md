# Verbal Valence
Study of verbal valence patterns in Biblical Hebrew. Part of the SYNVAR project.

# Main idea

The meaning of verbs depends critically on the number and nature of the complements found in their neighbourhood.
It is possible to write a flowchart, that lists those meanings as a function of the patterns of complements.

Having a linguistic text database of the Hebrew Bible at our disposal, it is possible to implement those flowcharts 
into an algorithm and apply that algorithm to all verb occurrences in the Hebrew Bible.

# Results

The results of executing this idea are being delivered in several forms, summarized here.
The terms in the result descriptions are explained in the section **Dependencies**.

* **Annotations** A set of annotations in **SHEBANQ** where the verbal valency analysis has been made explicit and viewable in context;
* **New data** A module of higher level features for **text-fabric** on which flowchart decisions have been based;
* **Corrections and enrichments** CSV files used in the workflows for correction and enrichments, contain the manual edits;
* **Documents** describing concrete flowcharts for individual verbs, to be found in the **wiki** of this repository;
* **Jupyter notebooks** that describe and execute:
  1. the data correction workflow, followed by enrichment;
  2. the application of the flowcharts to the whole corpus.

# Dependencies

## Data
This work rests on the Hebrew Text Database, compiled by the 
[ETCBC](https://www.godgeleerdheid.vu.nl/en/research/institutes-and-centres/eep-talstra-centre-for-bible-and-computer/index.aspx)
over the years.
We use version
[4b](https://doi.org/10.17026/dans-z6y-skyh) of 2015, which has been archived in
[DANS](https://dans.knaw.nl/en/front-page?set_language=en)-[EASY](https://easy.dans.knaw.nl/ui/deposit).
In order to work with it in text-fabric, we have stored a tf-version of this dataset in the github repo
[text-fabric-data-legacy](https://github.com/ETCBC/text-fabric-data-legacy).
There is a newer version available, 4c, from [text-fabric-data](https://github.com/ETCBC/text-fabric-data),
but 4b is the newest version that shows in SHEBANQ, hence we use 4c. 

## Tools
This works makes use of the tool
[text-fabric](https://github.com/ETCBC/text-fabric), which is a data model, file format and software package to deal with 
ancient texts and linguistic annotations.
This tool is employed in two *Jupyter notebooks*, residing in this repository.
These notebooks are executable, self documenting Python programs.

## Wiki
This github repository has an associated **wiki** which contains the flowcharts of individual verbs.

## Showcasing
We show the results in 
[SHEBANQ](https://shebanq.ancient-data.org), the website of the ETCBC that exposes its Hebrew Text Database in such a way
that users can query it, save their queries, add manual annotations and even upload bulks of generated annotations.
That is exactly what we do: the valency results are visible in SHEBANQ in notes view, so that every outcome can be viewed in context.

SHEBANQ is hosted by
[DANS](https://dans.knaw.nl/en/front-page?set_language=en).

# Complications

While the idea expressed above is simple, the execution of it meets a number of challenges.

## Data correction

The ETCBC data has been encoded by an ongoing effort of decades, during which principles of encoding and linguistic theories
were subject to change. This has led to several inconsistencies in the details of the encoding, in particular the assignment of
functions to phrases.

We have met this challenge by implementing a data correction workflow, where faulty phrase function assignments have been replaced
by better ones.

## Data enrichment
 
Moreover, the stance of the ETCBC encoders is objectivistic: a piece of markup is applied only when there
is objective, measured evidence to do so.
On the other hand, the traits needed by the flow charts to base decisions on, are often at a higher level of interpretation.
For example, the notion of *indirect object* is not present in the ETCBC encoding.

We have met this challenge by enriching the encoding with a bunch of higher level features, that we compute from the ensemble
of lower level features. We also have a workflow in place to manually adjust the outcome of this process.

However, the present results do not rely on manual enrichment, because the enrichment algorithm is still work in progress.
For now, we bet on improving the algorithm, rather than supplying manual enhancements.
In this stage, manual enhancements are counter productive, because checking manual enhancements after updates to the algorithm, is an
extra layer of complexity, especially when the vocabulary of the enrichments is in development as well.

## Target language dependency

When talking about the meanings of Hebrew verbs in English, the verbal valency patterns in the Enlish language interfere with those
in the Hebrew language. 
This hampers a clear organization of meanings of Hebrew verbs in their own terms, and may camouflage complexities in Hebrew meanings,
or complicate simple meaning structures.

We deal with this challenge by decoupling the valency patterns from the meanings. 
So, for each verb context, we annotate its valency pattern, categorize the relevant bits of context, and leave it there.
For selected verbs, we have an explicit map from valency patterns to meanings in English, a.k.a. the *flowchart*. 
For those verbs, we will insert a link to the flowchart of that verb.

# Authors

* Janet Dyk - linguistic researcher in Biblical Hebrew - author of the *flow charts*
* Dirk Roorda - wrote the Jupyter notebooks to identify the valency patterns. Wrote Text-Fabric as well.

# Date

Updated: 2017-09-12

Created: 2017-09-12
