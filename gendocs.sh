#!/bin/bash

# More info on autodoc customizations:
#
# http://tomfaulhaber.github.com/autodoc/

lein deps
lein autodoc
cd autodoc
git add -A
git commit -m "Documentation update."
git push origin gh-pages
cd ..
