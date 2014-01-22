# DDD-Downloader

The DDD downloader is a set of scripts to download the 
[Databank Digitale Dagbladen](http://kranten.kb.nl/about/Ontsluiting) 
from the [National library of the Netherlands](http://kb.nl/).
It was created in 2011 by [Daan Odijk](http://daan.odijk.me/)
at [ILPS](http://ilps.science.uva.nl/) (University of Amsterdam).

The scripts and resulting dataset have been used in these publications:

- Odijk D, de Rooij O, Peetz M-H, Pieters T, de Rijke M, Snelders S.  2012.  
  [Semantic Document Selection](http://ilps.science.uva.nl/biblio/semantic-document-selection). 
  TPDL 2012: Theory and Practice of Digital Libraries.
- Odijk D, Santucci G, de Rijke M, Angelini M, Granato G.  2012.  
  [Time-Aware Exploratory Search: Exploring Word Meaning through Time](http://ilps.science.uva.nl/biblio/time-aware-exploratory-search-exploring-word-meaning-through-time).
  SIGIR 2012 Workshop on Time-aware Information Access.

If you use these script or the retrieved dataset for your own research, 
please include a reference to one of these articles.

The documentation of the scripts is currently very limited as the 
code has been developed for internal use.

The [code](https://github.com/dodijk/ddd-downloader) is released
under LGPL license (see below). If you have any questions, contact
[Daan](http://daan.odijk.me).

## Usage

- Update the collection via [OAI/PMH](http://www.openarchives.org/pmh/) using `update.sh`.
  This will generate a set of XML files from the OAI/PMH server.
- Use `multi_store_kb.py` to download all OCR'ed text from the KB servers.
- `count_kb.py` will give an overview of the number of files downloaded.
- This also is a nice starting point if you want to process the files yourself.

## License

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this program.  If not, see
<http://www.gnu.org/licenses/>.
