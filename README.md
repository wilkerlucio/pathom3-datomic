# Pathom 3 Datomic integration

Pathom 3 Datomic provides a dynamic resolver to integrate with Datomic.

## State of the project

This project is experimental. 

After some iterations of playing with it, we found a fundamental problem regarding access control. Applications usually require some sort of access control that is business-dependent. While it is relatively easy to expose the whole Datomic database, the access control is not something we can solve generically.

So if you are writing some sort of admin UI that allows full access, this might be useful for you, otherwise you are probably better writing specific resolvers that reach for Datomic, that way you have full control over all the access.

## Usage

TODO
