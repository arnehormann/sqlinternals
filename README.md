sqlinternals
============
[![Build Status](https://travis-ci.org/arnehormann/sqlinternals.png?branch=master)](https://travis-ci.org/arnehormann/sqlinternals)

Provide a way to get driver.Rows from sql.Row and sql.Rows (Go [issue 5606](https://code.google.com/p/go/issues/detail?id=5606)). This has to use **unsafe**, so it can't be used in some contexts.

Documentation lives at [godoc.org](http://godoc.org/github.com/arnehormann/sqlinternals).
