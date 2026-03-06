# Jlpigraf

The package is an API adapter for Epigraf. [Epigraf](https://github.com/digicademy/epigraf) is a proven tool for capturing, annotating, and publishing humanities corpora, which can be conceptualised as collections of articles. Epigraf provides a powerful API that can be used to query and modify project data. The [Rpigraf](https://github.com/datavana/rpigraf) package is available for conveniently querying project data via the API. Rpigraf's functions make it easier to filter and structure data. Jlpigraf is a clone of Rpigraf with the same objective. It provides data as DataFrames that can be used directly in other Julia packages for visualization or analysis.

## Installation
Get the package from GitHub:
``` julia
using Pkg
Pkg.add(url="https://github.com/zweiglimmergneis/Jlpigraf.jl.git")
using Jlpigraf

```
## Documentation
See Epigraf's [documentation](https://epigraf.inschriften.net/help/coreconcepts/api) for a detailed description of the API. 

## Usage

Follow the instructions in [Rpigraf](https://github.com/datavana/rpigraf#access-the-epigraf-api)'s README, to get access to an Epigraf API endpoint. 

### First steps

``` julia
# Get an article list
articles = fetch_table("articles"; db = "epi_movies", maxpages = 2)
```
`articles` holds a DataFrame with article data, corresponding to the top level of the Relational Article Model.

The package is still in the early stages. More examples will be added as the functionality expands.


