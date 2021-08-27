# Verbal Valence

[![SWH](https://archive.softwareheritage.org/badge/origin/https://github.com/ETCBC/valence/)](https://archive.softwareheritage.org/browse/origin/https://github.com/ETCBC/valence/)
[![DOI](https://zenodo.org/badge/103229517.svg)](https://zenodo.org/badge/latestdoi/103229517)
[![etcbc](programs/images/etcbc.png)](http://www.etcbc.nl)
[![dans](programs/images/dans.png)](https://dans.knaw.nl/en)

### BHSA Family

* [bhsa](https://github.com/etcbc/bhsa) Core data and feature documentation
* [phono](https://github.com/etcbc/phono) Phonological representation of Hebrew words
* [parallels](https://github.com/etcbc/parallels) Links between similar verses
* [valence](https://github.com/etcbc/valence) Verbal valence for all occurrences
  of some verbs
* [trees](https://github.com/etcbc/trees) Tree structures for all sentences
* [bridging](https://github.com/etcbc/bridging) Open Scriptures morphology
  ported to the BHSA
* [pipeline](https://github.com/etcbc/pipeline) Generate the BHSA and SHEBANQ
  from internal ETCBC data files
* [shebanq](https://github.com/etcbc/shebanq) Engine of the
  [shebanq](https://shebanq.ancient-data.org) website

## About

Study of verbal valence patterns in Biblical Hebrew,

Part of the
[SYNVAR](https://www.nwo.nl/en/research-and-results/research-projects/i/30/9930.html)
project carried out at the 
[ETCBC](http://etcbc.nl)

## Results

The results of this study are being delivered in several forms, summarized here.
It might be helpful to consult a description of the
[sources](https://github.com/ETCBC/valence/wiki/Sources)
first.

* **Visualization**: a 
  [tutorial notebook](https://github.com/ETCBC/valence/blob/master/programs/senses.ipynb)
  showing the sense distribution of the 10 most frequent verbs;
* **Annotations**: a set of
  [annotations](https://shebanq.ancient-data.org/hebrew/note?version=4b&id=Mnx2YWxlbmNl&tp=txt_tb1&nget=v)
  in **SHEBANQ** showing the verbal valence analysis in context;
* **Data module**: a set of 
  [higher level features](https://github.com/ETCBC/valence/tree/master/tf)
  in **text-fabric** format, storing the flowchart input data and outcomes;
* **Spreadsheets**: a set of
  [CSV files](https://github.com/ETCBC/valence/tree/master/source/4b)
  used in the workflows for data entry;
* **Documents**: a number flowcharts for individual verbs, to be found in the
  [wiki](https://github.com/ETCBC/valence/wiki)
  of this repository;
* **Program code**: a
  [bunch of Jupyter notebooks](https://github.com/ETCBC/valence/tree/master/programs)
  that describe and execute the following tasks:
  1. The data correction workflow, followed by enrichment;
  2. The application of the flowcharts to the whole corpus.

![tf](programs/images/tf-small.png)
[![dans](programs/images/dans.png)](https://www.dans.knaw.nl)

For more information, go to the [wiki of this repo](https://github.com/ETCBC/valence/wiki).

## Authors
* [Janet Dyk](mailto:j.w.dyk@vu.nl) -
  [VU ETCBC](http://etcbc.nl) -
  linguistic researcher in Biblical Hebrew -
  author of the *flowcharts*;
* [Dirk Roorda](mailto:dirk.roorda@dans.knaw.nl) -
  [DANS](https://dans.knaw.nl/en/front-page?set_language=en) -
  author of the notebooks and documentation and the supporting library
  [Text-Fabric](https://github.com/annotation/text-fabric).

## Date

Updated: 2017-09-14

Created: 2017-09-14
