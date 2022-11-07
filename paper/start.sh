#!/usr/bin/env bash
atril main.pdf &
code .
when-changed -r sections/*.tex main.tex -c ./pdflatex.sh +b main.tex
