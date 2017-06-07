# twitterStats
Real-time statistics calculated from Twitter's streaming API

# Building & Running

To build and run using Cabal's nix-style local builds, just:

    bash$ cabal new-build
    bash$ ./dist-newstyle/build/twitterStats-0.1.0.0/build/twitterStats/twitterStats
    
It will print the statistics to the command line every 5 seconds.
