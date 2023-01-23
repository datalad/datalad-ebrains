# DataLad EBRAINS extension

[![Build status](https://ci.appveyor.com/api/projects/status/vld82efr44i6b6s3/branch/main?svg=true)](https://ci.appveyor.com/project/mih/datalad-ebrains/branch/main) [![codecov.io](https://codecov.io/github/datalad/datalad-ebrains/coverage.svg?branch=main)](https://codecov.io/github/datalad/datalad-ebrains?branch=main) [![crippled-filesystems](https://github.com/datalad/datalad-ebrains/workflows/crippled-filesystems/badge.svg)](https://github.com/datalad/datalad-ebrains/actions?query=workflow%3Acrippled-filesystems) [![docs](https://github.com/datalad/datalad-ebrains/workflows/docs/badge.svg)](https://github.com/datalad/datalad-ebrains/actions?query=workflow%3Adocs) [![Documentation Status](https://readthedocs.org/projects/datalad-ebrains/badge/?version=latest)](http://docs.datalad.org/projects/ebrains/en/latest/?badge=latest)

EBRAINS is a digital research infrastructure, created by the EU-funded Human
Brain Project, that gathers an extensive range of data and tools for brain
related research. EBRAINS capitalizes on the work performed by the Human
Brain Project teams in digital neuroscience, brain medicine and brain-inspired
technology.

The purpose of this DataLad extension package is to represent and retrieve
EBRAINS datasets as DataLad datasets (without requiring downloading or
duplication of datasets hosted on EBRAINS) to make them compatible with
the wider DataLad ecosystem.

Commands provided by this extension

- `ebrains-authenticate` -- Obtain an EBRAINS authentication token
- `ebrains-clone` -- Export a dataset from the [EBRAINS Knowledge
  Graph](https://kg.ebrains.eu) as a DataLad dataset

See the documentation for details:
 http://docs.datalad.org/projects/ebrains/en/latest/

## Installation

```
# create and enter a new virtual environment (optional)
$ virtualenv --python=python3 ~/env/dl-ebrains
$ . ~/env/dl-ebrains/bin/activate
# install from GitHub
$ python -m pip install git+https://github.com/datalad/datalad-ebrains.git
```

## Support

For general information on how to use or contribute to DataLad (and this
extension), please see the [DataLad website](http://datalad.org) or the
[main GitHub project page](http://datalad.org).

All bugs, concerns and enhancement requests for this software can be submitted here:
https://github.com/datalad/datalad-ebrains/issues

If you have a problem or would like to ask a question about how to use DataLad,
please [submit a question to
NeuroStars.org](https://neurostars.org/tags/datalad) with a ``datalad`` tag.
NeuroStars.org is a platform similar to StackOverflow but dedicated to
neuroinformatics.

All previous DataLad questions are available here:
http://neurostars.org/tags/datalad/


## Acknowledgements

This development was supported by European Union’s Horizon 2020 research and
innovation programme under grant agreement [Human Brain Project SGA3
(H2020-EU.3.1.5.3, grant no.
945539)](https://cordis.europa.eu/project/id/945539).
